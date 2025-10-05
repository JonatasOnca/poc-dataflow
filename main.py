import logging
import argparse
from datetime import datetime, timezone
import uuid

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.jdbc import ReadFromJdbc
from google.cloud import bigquery
import apache_beam.pvalue as pvalue

from beam_core._helpers.file_handler import load_yaml, load_schema, load_query
from beam_core._helpers.secret_manager import get_secret
from beam_core._helpers.transform_functions import TRANSFORM_MAPPING, generic_transform


def get_high_water_mark(project_id, dataset_id, table_id, column_name, column_type):
    try:
        client = bigquery.Client(project=project_id)
        query = f"SELECT MAX({column_name}) as hwm FROM `{project_id}.{dataset_id}.{table_id}`"
        logging.info(f"Executando query para obter high-water mark: {query}")
        query_job = client.query(query)
        results = query_job.result()
        row = next(iter(results))
        hwm = row.hwm

        if hwm is None:
            logging.warning(f"A tabela '{table_id}' está vazia ou o HWM é nulo. Iniciando carga completa.")
            if column_type.upper() in ['TIMESTAMP', 'DATETIME']:
                return datetime(1970, 1, 1, tzinfo=timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
            else:
                return 0

        if column_type.upper() in ['TIMESTAMP', 'DATETIME'] and isinstance(hwm, datetime):
            hwm = hwm.strftime('%Y-%m-%d %H:%M:%S')

        logging.info(f"High-water mark encontrado: {hwm}")
        return hwm

    except Exception as e:
        logging.warning(f"Não foi possível obter o high-water mark para '{table_id}'. Assumindo carga inicial. Erro: {e}")
        if column_type.upper() in ['TIMESTAMP', 'DATETIME']:
            return datetime(1970, 1, 1, tzinfo=timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
        else:
            return 0


class TransformWithSideInputDoFn(beam.DoFn):
    def __init__(self, transform_fn):
        self._transform_fn = transform_fn

    def process(self, element):
        transformed_element = self._transform_fn(element)
        yield transformed_element


class ExecuteBqMergeDoFn(beam.DoFn):
    def __init__(self, project_id, gcp_region, merge_query, staging_table_id):
        self._project_id = project_id
        self._gcp_region = gcp_region
        self._merge_query = merge_query
        self._staging_table_id = staging_table_id

    def process(self, element_count, _wait_for_write):
        if element_count == 0:
            logging.warning("No new elements found to process. Skipping MERGE step.")
            self._cleanup_staging_table() 
            return

        logging.info(f"Carga na tabela de staging concluída. Total de elementos processados: {element_count}. Acionando MERGE.")
        client = bigquery.Client(project=self._project_id, location=self._gcp_region)
        try:
            logging.info(f"Executando a query MERGE: \n{self._merge_query}")
            merge_job = client.query(self._merge_query)
            merge_job.result()
            logging.info("Query MERGE concluída com sucesso.")
        except Exception as e:
            logging.error(f"Falha ao executar a query MERGE: {e}")
            raise e
        finally:
            self._cleanup_staging_table()

    def _cleanup_staging_table(self):
        client = bigquery.Client(project=self._project_id, location=self._gcp_region)
        try:
            logging.info(f"Removendo a tabela de staging: {self._staging_table_id}")
            client.delete_table(self._staging_table_id, not_found_ok=True)
            logging.info(f"Tabela de staging {self._staging_table_id} removida.")
        except Exception as e:
            logging.error(f"Falha ao remover a tabela de staging {self._staging_table_id}: {e}")


def run():
    parser = argparse.ArgumentParser()
    parser.add_argument('--config_file', required=True, help='Caminho GCS para o arquivo config.yaml')
    parser.add_argument('--chunk_name', required=False, default="ALL", type=str, help='Chunk a ser executado')
    parser.add_argument('--table_name', required=False, default=None, type=str, help='Nome da tabela no banco de dados')
    parser.add_argument(
        '--load_type',
        choices=['backfill', 'delta', 'merge'],
        default='backfill',
        help='Tipo de carga: "backfill", "delta" (append-only), ou "merge" (upsert).'
    )
    known_args, pipeline_args = parser.parse_known_args()

    app_config = load_yaml(known_args.config_file)
    logging.info("Iniciando o pipeline com a seguinte configuração: %s", app_config)

    chunk_name = known_args.chunk_name
    table_name = known_args.table_name
    load_type = known_args.load_type

    logging.info("Buscando dados de acesso ao Banco de Dados no Secret Manager")
    db_creds = get_secret(
        project_id=app_config['gcp']['project_id'], 
        secret_id=app_config['source_db']['secret_id'], 
        version_id=app_config['source_db']['secret_version']
    )

    DB_HOST = db_creds.get('host')
    DB_NAME = db_creds.get('database')
    DB_USER = db_creds.get('user')
    DB_PASSWORD = db_creds.get('password')
    DB_PORT = db_creds.get('port')
    JDBC_URL = f"jdbc:mysql://{DB_HOST}:{DB_PORT}/{DB_NAME}"

    logging.info("Configurando as opções da pipeline do Dataflow")
    pipeline_options = PipelineOptions(
        pipeline_args,
        runner=app_config['dataflow']['parameters']['runner'],
        project=app_config['gcp']['project_id'],
        region=app_config['gcp']['region'],
        staging_location=app_config['dataflow']['parameters']['staging_location'],
        temp_location=app_config['dataflow']['parameters']['temp_location'],
        job_name=app_config['dataflow']['job_name'],
        setup_file='./setup.py'
    )

    project_id = app_config['gcp']['project_id']
    bq_dataset = app_config['bronze_dataset']
    queries_location = app_config['dataflow']['parameters']['queries_location']
    schemas_location = app_config['dataflow']['parameters']['schemas_location']

    if table_name:
        TABLE_LIST = [table_name]
    else:
        TABLE_LIST = []
        for chunk in app_config['chunks']:
            if chunk.get('name', 'ALL') == chunk_name:
                TABLE_LIST = chunk['lista']
                break

    with beam.Pipeline(options=pipeline_options) as pipeline:
        for table_name in TABLE_LIST:
            table_config = next((t for t in app_config['tables'] if t.get('name') == table_name), None)
            if not table_config:
                logging.error(f"Configuração para a tabela '{table_name}' não encontrada no arquivo YAML. Pulando.")
                continue

            _query_file = table_config['query_file']
            _schema_file = table_config['schema_file']
            base_query = load_query(f'{queries_location}/{_query_file}')
            _schema = load_schema(f'{schemas_location}/{_schema_file}')

            final_query = base_query
            target_table_id = f'{project_id}.{bq_dataset}.{table_name}'
            write_disposition = beam.io.BigQueryDisposition.WRITE_TRUNCATE
            destination_table_for_write = target_table_id
            destination_table_for_query = None
            current_load_type = load_type

            if load_type in ['delta', 'merge']:
                delta_config = table_config.get('delta_config')
                if not delta_config or 'column' not in delta_config:
                    logging.warning(f"Configuração 'delta_config' não encontrada para '{table_name}'. Executando como backfill.")
                    current_load_type = 'backfill'
                else:
                    hwm_column = delta_config['column']
                    hwm_type = delta_config.get('type', 'TIMESTAMP').upper()
                    high_water_mark = get_high_water_mark(project_id, bq_dataset, table_name, hwm_column, hwm_type)
                    condition_value = f"'{high_water_mark}'" if hwm_type not in ['INTEGER', 'BIGINT', 'INT', 'NUMERIC'] else str(high_water_mark)
                    where_clause = f"WHERE {hwm_column} > {condition_value}" if 'WHERE' not in base_query.upper() else f"AND {hwm_column} > {condition_value}"
                    final_query = f"{base_query.strip()} {where_clause}"

                    if current_load_type == 'delta':
                        write_disposition = beam.io.BigQueryDisposition.WRITE_APPEND
                    elif current_load_type == 'merge':
                        write_disposition = beam.io.BigQueryDisposition.WRITE_TRUNCATE
                        staging_table_name = f"{table_name}_staging_{uuid.uuid4().hex}"
                        destination_table_for_write = f"{project_id}.{bq_dataset}.{staging_table_name}"  # ponto, não dois-pontos
                        destination_table_for_query = destination_table_for_write  # mesma string
                        logging.info(f"Dados serão carregados na tabela de staging: {destination_table_for_write}")

            additional_bq_params = {}
            partitioning_info = table_config.get('partitioning_config')
            if partitioning_info and partitioning_info.get('column'):
                additional_bq_params['timePartitioning'] = {'type': partitioning_info.get('type', 'DAY').upper(),
                                                           'field': partitioning_info['column']}
            clustering_info = table_config.get('clustering_config')
            if clustering_info and clustering_info.get('columns'):
                additional_bq_params['clustering'] = {'fields': clustering_info['columns']}

            transform_function = TRANSFORM_MAPPING.get(table_name, generic_transform)

            rows = (
                pipeline
                | f'Read {table_name} from MySQL' >> ReadFromJdbc(
                    driver_class_name=app_config['database']['driver_class_name'],
                    table_name=table_name,
                    jdbc_url=JDBC_URL,
                    username=DB_USER,
                    password=DB_PASSWORD,
                    query=final_query,
                    driver_jars=app_config['database']['driver_jars'],
                )
                | f'Convert {table_name} to Dict' >> beam.Map(lambda row: row._asdict())
            )

            transformed_data = (
                rows
                | f'Transform {table_name}' >> beam.ParDo(TransformWithSideInputDoFn(transform_function))
            )

            # Contagem global usada como gatilho para MERGE
            element_count_signal = transformed_data | f'Count Elements for {table_name}' >> beam.combiners.Count.Globally()

            # Escrita no BigQuery
            _ = transformed_data | f'Write {table_name} to BigQuery' >> beam.io.WriteToBigQuery(
                table=destination_table_for_write,
                schema=_schema,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition=write_disposition,
                additional_bq_parameters=additional_bq_params,
                insert_retry_strategy='RETRY_NEVER',
            )

            # MERGE
            if current_load_type == 'merge':
                merge_config = table_config.get('merge_config')
                if not merge_config or 'keys' not in merge_config:
                    raise ValueError(f"A configuração 'merge_config' com 'keys' é obrigatória para '{table_name}' com load_type='merge'.")

                merge_keys = merge_config['keys']
                schema_fields = [field['name'] for field in _schema['fields']]
                on_clause = " AND ".join([f"T.{key} = S.{key}" for key in merge_keys])
                update_cols = [col for col in schema_fields if col not in merge_keys]
                update_set_clause = ", ".join([f"T.{col} = S.{col}" for col in update_cols])
                columns_list = ", ".join(schema_fields)
                values_list = ", ".join([f"S.{col}" for col in schema_fields])

                merge_query = f"""
                    MERGE `{target_table_id}` AS T
                    USING `{destination_table_for_query}` AS S
                    ON {on_clause}
                    WHEN MATCHED THEN
                        UPDATE SET {update_set_clause}
                    WHEN NOT MATCHED THEN
                        INSERT ({columns_list})
                        VALUES ({values_list})
                """

                # Usa element_count_signal como entrada do ParDo
                _ = element_count_signal | f'Execute MERGE for {table_name}' >> beam.ParDo(
                    ExecuteBqMergeDoFn(
                        project_id=project_id,
                        gcp_region=app_config['gcp']['region'],
                        merge_query=merge_query,
                        staging_table_id=destination_table_for_query
                    ),
                    _wait_for_write=pvalue.AsIter(element_count_signal)  # PCollection como side input
                )


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
