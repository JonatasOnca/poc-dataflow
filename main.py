import json
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.jdbc import ReadFromJdbc
from google.cloud import secretmanager


# Configurações do Google Cloud
PROJECT_ID = 'abemcomum-saev-prod'
REGION = 'us-central1'
SECRET_ID = 'dataflow-mysql-credentials'
# SECRET_ID = 'dataflow-mysql-credentials-temp'
TEMP_LOCATION = 'gs://abemcomum-saev-prod/mysql-to-bq-dataflow-multiple-tables/temp'

# Configurações do MySQL
JDBC_DRIVER_JAR = '_drivers/mysql-connector-j-8.0.33.jar'
JDBC_DRIVER_CLASS = 'com.mysql.cj.jdbc.Driver'


# Configurações do BigQuery
BQ_DATASET = 'bronze'
BQ_TABLE = 'raca'
BQ_QUERY = '''
    SELECT 
        CAST(PEL_ATIVO AS UNSIGNED) AS PEL_ATIVO, 
        # CAST(PEL_DT_ATUALIZACAO AS DATETIME) AS PEL_DT_ATUALIZACAO, 
        # CAST(PEL_DT_CRIACAO AS DATETIME) AS PEL_DT_CRIACAO, 
        CAST(PEL_ID AS SIGNED) AS PEL_ID, 
        CAST(PEL_NOME AS CHAR) AS PEL_NOME, 
        CAST(PEL_OLD_ID AS SIGNED) AS PEL_OLD_ID
    FROM raca
'''
BQ_SCHEMA = {
    'fields': 
        [
            {
                "name": "PEL_ATIVO",
                "mode": "",
                "type": "INTEGER",
                "description": "",
                "fields": []
            },
            # {
            #     "name": "PEL_DT_ATUALIZACAO",
            #     "mode": "",
            #     "type": "TIMESTAMP",
            #     "description": "",
            #     "fields": []
            # },
            # {
            #     "name": "PEL_DT_CRIACAO",
            #     "mode": "",
            #     "type": "TIMESTAMP",
            #     "description": "",
            #     "fields": []
            # },
            {
                "name": "PEL_ID",
                "mode": "",
                "type": "INTEGER",
                "description": "",
                "fields": []
            },
            {
                "name": "PEL_NOME",
                "mode": "",
                "type": "STRING",
                "description": "",
                "fields": []
            },
            {
                "name": "PEL_OLD_ID",
                "mode": "",
                "type": "INTEGER",
                "description": "",
                "fields": []
            }
        ]
    }
def map_raca_to_dict(row):
    """Mapeia uma linha da tabela 'raca' para um dicionário."""
    return {
        "PEL_ATIVO": row.PEL_ATIVO,
        "PEL_DT_ATUALIZACAO": row.PEL_DT_ATUALIZACAO,
        "PEL_DT_CRIACAO": row.PEL_DT_CRIACAO,
        "PEL_ID": row.PEL_ID,
        "PEL_NOME": row.PEL_NOME,
        "PEL_OLD_ID": row.PEL_OLD_ID,
        }

def get_secret(project_id: str, secret_id: str, version_id: str = "latest") -> dict:
    """
    Acessa um segredo no Google Cloud Secret Manager e o retorna como um dicionário.
    """
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
    response = client.access_secret_version(request={"name": name})
    payload = response.payload.data.decode("UTF-8")
    return json.loads(payload)

def run():
    """Executa a pipeline de ingestão."""

    db_creds = get_secret(PROJECT_ID, SECRET_ID, version_id=2)
    DB_HOST = db_creds['host']
    DB_NAME = db_creds['database']
    DB_USER = db_creds['user']
    DB_PASSWORD = db_creds['password']
    DB_PORT = db_creds['port']
    # JDBC_URL = f"jdbc:mysql://{DB_HOST}:{DB_PORT}/{DB_NAME}?serverTimezone=UTC"
    JDBC_URL = f"jdbc:mysql://{DB_HOST}:{DB_PORT}/{DB_NAME}"
    
    # Opções da pipeline
    pipeline_options = PipelineOptions(
        runner='DirectRunner',  # Use 'DataflowRunner' para executar no Google Cloud
        project=PROJECT_ID,
        region=REGION,
        temp_location=TEMP_LOCATION,
        job_name='mysql-para-bq-ingestao'
    )

    with beam.Pipeline(options=pipeline_options) as pipeline:
        # 1. Leitura do MySQL usando ReadFromJdbc
        dados_mysql = pipeline | 'Ler do MySQL' >> ReadFromJdbc(
            driver_class_name=JDBC_DRIVER_CLASS,
            table_name=BQ_TABLE,
            jdbc_url=JDBC_URL,
            username=DB_USER,
            password=DB_PASSWORD,
            query=BQ_QUERY,
            driver_jars=JDBC_DRIVER_JAR,
        )

        # 2. Transformação para dicionários
        dados_formatados = dados_mysql | 'Converter para Dicionário' >> beam.Map(lambda row: dict(row._asdict()))


        # 3. Escrita no BigQuery
        dados_formatados | 'Escrever no BigQuery' >> beam.io.WriteToBigQuery(
            table=f'{PROJECT_ID}:{BQ_DATASET}.{BQ_TABLE}',
            schema=BQ_SCHEMA,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE
        )

if __name__ == '__main__':
    run()