import logging
import argparse
from datetime import datetime, timezone
import uuid

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.jdbc import ReadFromJdbc
from google.cloud import bigquery

from beam_core._helpers.file_handler import load_yaml, load_schema, load_query
from beam_core._helpers.secret_manager import get_secret
from beam_core._helpers.transform_functions import TRANSFORM_MAPPING, generic_transform


def get_high_water_mark(project_id, dataset_id, table_id, column_name, column_type):
    try:
        client = bigquery.Client(project=project_id)
        query = f"SELECT MAX({column_name}) as hwm FROM `{project_id}.{dataset_id}.{table_id}`"
        logging.info(f"Executando query para HWM: {query}")
        query_job = client.query(query)
        results = query_job.result()
        row = next(iter(results))
        hwm = row.hwm

        if hwm is None:
            logging.warning(f"Tabela '{table_id}' vazia ou HWM nulo. Iniciando carga completa.")
            if column_type.upper() in ['TIMESTAMP', 'DATETIME']:
                return datetime(1970, 1, 1, tzinfo=timezone.utc).strftime('%Y-%m-%d %H:%M:%S.%f')
            else:
                return 0

        if column_type.upper() in ['TIMESTAMP', 'DATETIME'] and isinstance(hwm, datetime):
            hwm = hwm.strftime('%Y-%m-%d %H:%M:%S.%f')

        logging.info(f"High-water mark encontrado: {hwm}")
        return hwm

    except Exception as e:
        logging.warning(f"Falha ao obter HWM para '{table_id}', assumindo carga inicial: {e}")
        if column_type.upper() in ['TIMESTAMP', 'DATETIME']:
            return datetime(1970, 1, 1, tzinfo=timezone.utc).strftime('%Y-%m-%d %H:%M:%S.%f')
        else:
            return 0


class TransformWithSideInputDoFn(beam.DoFn):
    def __init__(self, transform_fn):
        self._transform_fn = transform_fn

    def process(self, element):
        yield self._transform_fn(element)


def execute_merge(project_id, gcp_region, target_table_id, staging_table_id, merge_keys, schema_fields):
    """Função separada para MERGE pós-pipeline"""
    if not staging_table_id:
        logging.info(f"Nenhuma tabela de staging para {target_table_id}. Pulando MERGE.")
        return

    on_clause = " AND ".join([f"T.{key} = S.{key}" for key in merge_keys])
    update_cols = [c for c in schema_fields if c not in merge_keys]
    update_set_clause = ", ".join([f"T.{c} = S.{c}" for c in update_cols])
    columns_list = ", ".join(schema_fields)
    values_list = ", ".join([f"S.{c}" for c in schema_fields])

    merge_query = f"""
        MERGE `{target_table_id}` AS T
        USING `{staging_table_id}` AS S
        ON {on_clause}
        WHEN MATCHED THEN
            UPDATE SET {update_set_clause}
        WHEN NOT MATCHED THEN
            INSERT ({columns_list})
            VALUES ({values_list})
    """

    client = bigquery.Client(project=project_id, location=gcp_region)
    logging.info(f"Executando MERGE: {merge_query}")
    client.query(merge_query).result()
    logging.info("MERGE concluído com sucesso.")

    # Remove tabela de staging
    logging.info(f"Removendo tabela de staging: {staging_table_id}")
    client.delete_table(staging_table_id, not_found_ok=True)


def run():
    parser = argparse.ArgumentParser()
    parser.add_argument('--config_file', required=True, help='Caminho GCS para config.yaml')
    parser.add_argument('--chunk_name', default="ALL", type=str)
    parser.add_argument('--table_name', default=None, type=str)
    parser.add_argument('--load_type', choices=['backfill', 'delta', 'merge'], default='backfill')
    known_args, pipeline_args = parser.parse_known_args()

    app_config = load_yaml(known_args.config_file)
    logging.info(f"Iniciando pipeline com config: {app_config}")

    chunk_name = known_args.chunk_name
    table_name = known_args.table_name
    load_type = known_args.load_type

    db_creds = get_secret(
        project_id=app_config['gcp']['project_id'], 
        secret_id=app_config['source_db']['secret_id'], 
        version_id=app_config['source_db']['secret_version']
    )
    JDBC_URL = f"jdbc:mysql://{db_creds['host']}:{db_creds['port']}/{db_creds['database']}"

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
        TABLE_LIST = next((c['lista'] for c in app_config['chunks'] if c.get('name','ALL')==chunk_name), [])

    merge_tasks = []

    with beam.Pipeline(options=pipeline_options) as pipeline:
        for table_name in TABLE_LIST:
            table_config = next((t for t in app_config['tables'] if t.get('name')==table_name), None)
            if not table_config:
                logging.error(f"Configuração para '{table_name}' não encontrada. Pulando.")
                continue

            base_query = load_query(f"{queries_location}/{table_config['query_file']}")
            _schema = load_schema(f"{schemas_location}/{table_config['schema_file']}")
            final_query = base_query
            target_table_id = f"{project_id}.{bq_dataset}.{table_name}"
            write_disposition = beam.io.BigQueryDisposition.WRITE_TRUNCATE
            destination_table_for_write = target_table_id
            destination_table_for_query = None
            current_load_type = load_type

            if load_type in ['delta', 'merge']:
                delta_config = table_config.get('delta_config')
                if delta_config and 'column' in delta_config:
                    hwm_column = delta_config['column']
                    hwm_type = delta_config.get('type', 'TIMESTAMP').upper()
                    high_water_mark = get_high_water_mark(project_id, bq_dataset, table_name, hwm_column, hwm_type)
                    condition_value = f"'{high_water_mark}'" if hwm_type not in ['INTEGER','BIGINT','INT','NUMERIC'] else str(high_water_mark)
                    where_clause = f"WHERE {hwm_column} > {condition_value}" if 'WHERE' not in base_query.upper() else f"AND {hwm_column} > {condition_value}"
                    final_query = f"{base_query.strip()} {where_clause}"

                    if current_load_type == 'delta':
                        write_disposition = beam.io.BigQueryDisposition.WRITE_APPEND
                    elif current_load_type == 'merge':
                        staging_table_name = f"{table_name}_staging_{uuid.uuid4().hex}"
                        write_disposition = beam.io.BigQueryDisposition.WRITE_TRUNCATE
                        destination_table_for_write = f"{project_id}.{bq_dataset}.{staging_table_name}"
                        destination_table_for_query = destination_table_for_write
                        logging.info(f"Dados serão carregados na staging: {destination_table_for_write}")
                        merge_tasks.append({
                            'target_table_id': target_table_id,
                            'staging_table_id': destination_table_for_query,
                            'merge_keys': table_config['merge_config']['keys'],
                            'schema_fields': [f['name'] for f in _schema['fields']]
                        })

            additional_bq_params = {}
            if table_config.get('partitioning_config') and table_config['partitioning_config'].get('column'):
                additional_bq_params['timePartitioning'] = {
                    'type': table_config['partitioning_config'].get('type','DAY').upper(),
                    'field': table_config['partitioning_config']['column']
                }
            if table_config.get('clustering_config') and table_config['clustering_config'].get('columns'):
                additional_bq_params['clustering'] = {'fields': table_config['clustering_config']['columns']}

            transform_function = TRANSFORM_MAPPING.get(table_name, generic_transform)

            # Read MySQL
            rows = (pipeline
                    | f"Read {table_name} from MySQL" >> ReadFromJdbc(
                        driver_class_name=app_config['database']['driver_class_name'],
                        table_name=table_name,
                        jdbc_url=JDBC_URL,
                        username=db_creds['user'],
                        password=db_creds['password'],
                        query=final_query,
                        driver_jars=app_config['database']['driver_jars'],
                    )
                    | f"Convert {table_name} to Dict" >> beam.Map(lambda row: row._asdict())
            )

            transformed_data = rows | f"Transform {table_name}" >> beam.ParDo(TransformWithSideInputDoFn(transform_function))

            # WriteToBigQuery
            _ = transformed_data | f"Write {table_name} to BigQuery" >> beam.io.WriteToBigQuery(
                table=destination_table_for_write,
                schema=_schema,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition=write_disposition,
                additional_bq_parameters=additional_bq_params,
                insert_retry_strategy='RETRY_NEVER',
                custom_gcs_temp_location=app_config['dataflow']['parameters']['temp_location']
            )

    # Após pipeline terminar, executar MERGE para cada staging
    for task in merge_tasks:
        execute_merge(
            project_id=project_id,
            gcp_region=app_config['gcp']['region'],
            target_table_id=task['target_table_id'],
            staging_table_id=task['staging_table_id'],
            merge_keys=task['merge_keys'],
            schema_fields=task['schema_fields']
        )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
