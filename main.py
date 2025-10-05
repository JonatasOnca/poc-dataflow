import logging
import argparse
from datetime import datetime, timezone

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.jdbc import ReadFromJdbc
from google.cloud.bigquery import TimePartitioning # Importa TimePartitioning
from google.cloud import bigquery

# Supondo que estes helpers existem na estrutura de pastas do seu projeto
from beam_core._helpers.file_handler import load_yaml, load_schema, load_query
from beam_core._helpers.secret_manager import get_secret
from beam_core._helpers.transform_functions import TRANSFORM_MAPPING, generic_transform

def get_high_water_mark(project_id, dataset_id, table_id, column_name, column_type):
    """
    Busca o valor máximo da coluna de controle na tabela de destino do BigQuery.
    """
    try:
        client = bigquery.Client(project=project_id)
        query = f"SELECT MAX({column_name}) as hwm FROM `{project_id}.{dataset_id}.{table_id}`"
        logging.info(f"Executando query para obter high-water mark: {query}")
        
        query_job = client.query(query)
        results = query_job.result()
        
        row = next(iter(results))
        hwm = row.hwm

        if hwm is None:
            # Se a tabela estiver vazia, retorna um valor inicial padrão
            logging.warning(f"A tabela '{table_id}' está vazia ou o HWM é nulo. Iniciando carga completa.")
            if column_type.upper() == 'TIMESTAMP':
                # Retorna uma data muito antiga para buscar todos os registros
                return datetime(1970, 1, 1, tzinfo=timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
            else: # INTEGER, etc.
                return 0
        
        logging.info(f"High-water mark encontrado: {hwm}")
        return hwm

    except Exception as e:
        # Se a tabela não existir, ou outro erro ocorrer, assume uma carga inicial
        logging.warning(f"Não foi possível obter o high-water mark para '{table_id}'. Assumindo carga inicial. Erro: {e}")
        if column_type.upper() == 'TIMESTAMP':
            return datetime(1970, 1, 1, tzinfo=timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
        else:
            return 0


class TransformWithSideInputDoFn(beam.DoFn):
    def __init__(self, transform_fn):
        self._transform_fn = transform_fn

    def process(self, element):
        transformed_element = self._transform_fn(element)
        yield transformed_element

def run():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--config_file',
        required=True,
        help='Caminho GCS para o arquivo config.yaml'
    )
    parser.add_argument(
        '--chunk_name',
        required=False,
        default="ALL",
        type=str,
        help='Chunk a ser executado'
    )
    parser.add_argument(
        '--table_name', 
        required=False,
        default=None,
        type=str, 
        help='Nome da tabela no banco de dados',
    )
    parser.add_argument(
        '--load_type',
        choices=['backfill', 'delta'],
        default='backfill',
        help='Tipo de carga: "backfill" para carga completa ou "delta" para incremental.'
    )

    known_args, pipeline_args = parser.parse_known_args()

    app_config =  load_yaml(known_args.config_file)
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

    DB_HOST = db_creds['host']
    DB_NAME = db_creds['database']
    DB_USER = db_creds['user']
    DB_PASSWORD = db_creds['password']
    DB_PORT = db_creds['port']
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
            table_config = None
            for t in app_config['tables']:
                if t.get('name') == table_name:
                    table_config = t
                    break
            
            if not table_config:
                logging.error(f"Configuração para a tabela '{table_name}' não encontrada no arquivo YAML.")
                continue

            _query_file = table_config['query_file']
            _schema_file = table_config['schema_file']
            base_query = load_query(f'{queries_location}/{_query_file}')
            _schema = load_schema(f'{schemas_location}/{_schema_file}')
            
            final_query = base_query
            write_disposition = beam.io.BigQueryDisposition.WRITE_TRUNCATE

            if load_type == 'delta':
                delta_config = table_config.get('delta_config')
                
                if not delta_config or 'column' not in delta_config:
                    logging.warning(
                        f"Configuração 'delta_config' não encontrada ou inválida para a tabela '{table_name}'. "
                        f"Executando como backfill (WRITE_TRUNCATE)."
                    )
                else:
                    hwm_column = delta_config['column']
                    hwm_type = delta_config.get('type', 'TIMESTAMP').upper()
                    
                    logging.info(f"Iniciando carga delta para '{table_name}' usando a coluna '{hwm_column}'.")
                    
                    high_water_mark = get_high_water_mark(
                        project_id=project_id,
                        dataset_id=bq_dataset,
                        table_id=table_name,
                        column_name=hwm_column,
                        column_type=hwm_type
                    )
                    
                    if hwm_type in ['INTEGER', 'BIGINT', 'INT', 'NUMERIC']:
                        condition_value = high_water_mark
                    else:
                        condition_value = f"'{high_water_mark}'"

                    if 'WHERE' in base_query.upper():
                        where_clause = f"AND {hwm_column} > {condition_value}"
                    else:
                        where_clause = f"WHERE {hwm_column} > {condition_value}"
                    
                    final_query = f"{base_query.strip()} {where_clause}"
                    
                    logging.info(f"Query delta final para a tabela '{table_name}': {final_query}")
                    write_disposition = beam.io.BigQueryDisposition.WRITE_APPEND
            
            time_partitioning_config = None
            partitioning_info = table_config.get('partitioning_config')
            if partitioning_info:
                part_type = partitioning_info.get('type', 'DAY').upper()
                part_column = partitioning_info.get('column')
                
                if part_column:
                    logging.info(f"Configurando particionamento do tipo '{part_type}' na coluna '{part_column}' para a tabela '{table_name}'.")
                    time_partitioning_config = {
                            'timePartitioning': {
                                'type': part_type,
                                'field': part_column
                            }
                        }
                else:
                    logging.warning(f"Configuração de particionamento para '{table_name}' está incompleta (falta 'column'). A tabela não será particionada.")

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
                | f'Transform {table_name}' >> beam.ParDo(
                    TransformWithSideInputDoFn(transform_function),
                )
            )

            (transformed_data
                | f'Write {table_name} to BigQuery' >> beam.io.WriteToBigQuery(
                    table=f'{project_id}:{bq_dataset}.{table_name}',
                    schema=_schema,
                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                    write_disposition=write_disposition,
                    additional_bq_parameters=time_partitioning_config or {}
                )
            )

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()