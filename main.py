import logging
import argparse
# import time

import apache_beam as beam
# from apache_beam.pvalue import AsSingleton
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.jdbc import ReadFromJdbc
from utils.secret_manager import get_secret
from utils.file_handler import load_yaml, load_schema, load_query

def generic_transform(row_dict):
    return row_dict

def transform_genero_table(row_dict):
    """Mapeia uma linha da tabela 'genero' para um dicionário."""
    return generic_transform(row_dict)

def transform_raca_table(row_dict):
    """Mapeia uma linha da tabela 'raca' para um dicionário."""
    return generic_transform(row_dict)


def transform_pedidos_table(row_dict):
    return generic_transform(row_dict)

TRANSFORM_MAPPING = {
    'genero': transform_genero_table,
    'raca': transform_raca_table,
}

class TransformWithSideInputDoFn(beam.DoFn):
    def __init__(self, transform_fn):
        self._transform_fn = transform_fn

    def process(self, element):
        # 'element' é o registro principal (a linha da tabela)
        # 'start_signal_info' é o dado vindo do passo "Start"
        
        # Você pode usar a informação do side input se quiser
        # Por exemplo, logar ou adicionar ao registro
        # logging.info(f"Start signal received: {start_signal_info}")
        
        # Aplica a função de transformação original
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

    known_args, pipeline_args = parser.parse_known_args()

    app_config =  load_yaml(known_args.config_file)
    logging.info("Iniciando o pipeline com a seguinte configuração: %s", app_config)

    chunk_name = known_args.chunk_name
    table_name = known_args.table_name

    
    logging.info("Busca os dados de acesso ao Banco na Secrets")
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
    
    logging.info("Configura as opções da pipeline")
    pipeline_options = PipelineOptions(
        pipeline_args,
        runner=app_config['dataflow']['parameters']['runner'],
        project=app_config['gcp']['project_id'],
        region=app_config['gcp']['region'],

        staging_location=app_config['dataflow']['parameters']['staging_location'],
        temp_location=app_config['dataflow']['parameters']['temp_location'],
        queries_location=app_config['dataflow']['parameters']['queries_location'],
        schemas_location=app_config['dataflow']['parameters']['schemas_location'],

        job_name=app_config['dataflow']['job_name'],
        setup_file='./setup.py'
    )
    
    project_id = app_config['gcp']['project_id']
    bq_dataset = app_config['bronze_dataset']

    queries_location = app_config['dataflow']['parameters']['queries_location']
    schemas_location = app_config['dataflow']['parameters']['schemas_location']
    
    chunk_name = known_args.chunk_name
    table_name = known_args.table_name
    if table_name:
        TABLE_LIST = [table_name]
    else:
        for chunk in app_config['chunks']:
            if chunk.get('name', 'ALL') == chunk_name:
                TABLE_LIST = chunk['lista']
                break
    

    with beam.Pipeline(options=pipeline_options) as pipeline:

        for table_name in TABLE_LIST:
            for table in app_config['tables']:
                try:
                    if table_name == table.get('name', 'N/A'):
                        break
                except Exception as e:
                    logging.error(f"Falha ao construir o pipeline para a tabela '{table_name}': {e}")

            _query_file = table['query_file']
            _schema_file = table['schema_file']
            _query = load_query(f'{queries_location}/{_query_file}')
            _schema = load_schema(f'{schemas_location}/{_schema_file}')

            transform_function = TRANSFORM_MAPPING.get(table_name, generic_transform)

            rows = (
                pipeline
                | f'Read {table_name} from MySQL' >> ReadFromJdbc(
                    driver_class_name=app_config['database']['driver_class_name'],
                    table_name=table_name,
                    jdbc_url=JDBC_URL,
                    username=DB_USER,
                    password=DB_PASSWORD,
                    query=_query,
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
                    write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE
                )
            )
            
if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()