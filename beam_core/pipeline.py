import apache_beam as beam
import logging

from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.bigquery import WriteToBigQuery
from apache_beam.io.jdbc import ReadFromJdbc

from transforms.data_validation import MapAndValidate, OutputTags
from connectors.secret_manager import get_secret
from utils.file_handler import load_schema, load_query

# 1. Importe o dicionário do novo arquivo
from beam_core.mapping_functions import MAP_FUNCTIONS

# 2. As definições das funções e do dicionário foram removidas daqui


def build_table_pipeline(p, table_config, common_configs, app_config):
    """
    Constrói e anexa um ramo do pipeline para processar uma única tabela.
    Isso inclui leitura, validação (DLQ) e escrita no BigQuery.
    """
    table_name = table_config['name']
    driver_class_name = app_config['database']['driver_class_name']
    driver_jars = app_config['database']['driver_jars']

    # O código aqui continua funcionando perfeitamente, pois common_configs['map_functions']
    # receberá o dicionário MAP_FUNCTIONS importado.
    map_fn = common_configs['map_functions'][table_config['map_function']]
    _query = load_query(table_config['query_file'])
    _schema = load_schema(table_config['schema_file'])
    
    table_spec = f"{common_configs['gcp_project']}:{common_configs['dataset']}.{table_name}"
    error_table_spec = f"{common_configs['gcp_project']}:{common_configs['dataset']}.{table_name}_errors"
    write_disposition = table_config.get('write_disposition', 'WRITE_TRUNCATE')

    logging.info(f"Construindo ramo do pipeline para a tabela: {table_name}")

    logging.info(f"Passo 1: Ler do MySQL. A PCollection de entrada é a própria pipeline 'p': {table_name}")

    source_data = (
        p
        | f'ReadFromMySQL_{table_name}' >> ReadFromJdbc(
            driver_class_name=driver_class_name,
            table_name=table_name,
            jdbc_url=common_configs['jdbc_url'],
            username=common_configs['db_creds']['user'],
            password=common_configs['db_creds']['password'],
            # query=_query,
            driver_jars=driver_jars
        )
    )
    
    logging.info(f"Passo 2: Mapear e validar cada linha, separando sucesso de falha (DLQ): {table_name}")

    processed_results = (
        source_data
        | f'MapAndValidate_{table_name}' >> beam.ParDo(MapAndValidate(map_fn)).with_outputs(
            OutputTags.FAILURE, main=OutputTags.SUCCESS
        )
    )

    successful_records = processed_results[OutputTags.SUCCESS]
    failed_records = processed_results[OutputTags.FAILURE]

    logging.info(f"Passo 3: Escrever registros bem-sucedidos na tabela principal do BigQuery: {table_name}")

    (
        successful_records
        | f'WriteToBigQuery_{table_name}' >> WriteToBigQuery(
            table=table_spec,
            schema={'fields': _schema},
            create_disposition='CREATE_IF_NEEDED',
            write_disposition=write_disposition
        )
    )

    logging.info(f"Passo 4: Escrever registros com falha na tabela de erros do BigQuery: {table_name}")

    (
        failed_records
        | f'WriteErrorsToBigQuery_{table_name}' >> WriteToBigQuery(
            table=error_table_spec,
            schema={
                'fields': [
                    {'name': 'original_data', 'type': 'STRING'},
                    {'name': 'error_message', 'type': 'STRING'},
                    {'name': 'traceback', 'type': 'STRING'},
                ]
            },
            create_disposition='CREATE_IF_NEEDED',
            write_disposition='WRITE_APPEND'
        )
    )

def run(app_config: dict, pipeline_options: PipelineOptions):
    """
    Executa o pipeline de ingestão para múltiplas tabelas do MySQL para o BigQuery.
    """
    gcp_config = app_config['gcp']
    db_config = app_config['source_db']
    
    logging.info(f"1. Configurações e credenciais (feito uma vez)")
    db_creds = get_secret(gcp_config['project_id'], db_config['secret_id'])
    jdbc_url = f"jdbc:mysql://{db_creds['host']}:{db_creds['port']}/{db_creds['database']}?serverTimezone=UTC"
    
    common_configs = {
        "gcp_project": gcp_config['project_id'],
        "dataset": app_config['destination_dataset'],
        "db_creds": db_creds,
        "jdbc_url": jdbc_url,
        "map_functions": MAP_FUNCTIONS
    }
    
    with beam.Pipeline(options=pipeline_options) as p:
        logging.info(f"2. Iterar sobre cada tabela e construir seu ramo no pipeline")
        
        for table_config in app_config['tables']:
            try:
                map_function_name = table_config.get('map_function')
                if not map_function_name or map_function_name not in MAP_FUNCTIONS:
                    raise ValueError(f"Função de mapeamento '{map_function_name}' não encontrada.")
                
                build_table_pipeline(p, table_config, common_configs, app_config)

            except Exception as e:
                table_name = table_config.get('name', 'N/A')
                logging.error(f"Falha ao construir o pipeline para a tabela '{table_name}': {e}")