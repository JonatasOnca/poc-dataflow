import json
import argparse
import yaml

import logging
import apache_beam as beam
from apache_beam.io.filesystems import FileSystems
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.jdbc import ReadFromJdbc
from google.cloud import secretmanager
from utils.secret_manager import get_secret
from utils.file_handler import load_schema, load_query


def run():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--config_file',
        required=True,
        help='Caminho para o arquivo de configuração YAML.'
    )
    known_args, pipeline_args = parser.parse_known_args()

    logging.info("Le o YAML de com as configuraçoes")
    with FileSystems.open(known_args.config_file) as f:
        app_config = yaml.safe_load(f)

    logging.info("Iniciando o pipeline com a seguinte configuração: %s", app_config)

    
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
        job_name=app_config['dataflow']['job_name'],
        setup_file='./setup.py'
    )
    project_id = app_config['gcp']['project_id']
    bq_dataset = app_config['destination_dataset']

    for table in app_config['tables']:
        _query = load_query(table['query_file'])
        _schema = load_schema(table['schema_file'])
        logging.info("Executa a pipeline de ingestão.")
        with beam.Pipeline(options=pipeline_options) as pipeline:
            logging.info("1. Leitura do MySQL usando ReadFromJdbc")
            dados_mysql = pipeline | 'Ler do MySQL' >> ReadFromJdbc(
                driver_class_name=app_config['database']['driver_class_name'],
                table_name=table['name'],
                jdbc_url=JDBC_URL,
                username=DB_USER,
                password=DB_PASSWORD,
                query=_query,
                driver_jars=app_config['database']['driver_jars'],
            )

            logging.info("2. Transformação para dicionários")
            dados_formatados = dados_mysql | 'Converter para Dicionário' >> beam.Map(lambda row: dict(row._asdict()))
            logging.info("3. Escrita no BigQuery")
            dados_formatados | 'Escrever no BigQuery' >> beam.io.WriteToBigQuery(
                table=f'{project_id}:{bq_dataset}.{table_name}',
                schema=_schema,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE
            )

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()