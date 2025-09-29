import apache_beam as beam
import logging
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.bigquery import WriteToBigQuery
from apache_beam.io.jdbc import ReadFromJdbc

from transforms.data_validation import MapAndValidate, OutputTags 
from connectors.secret_manager import get_secret
from utils.file_handler import load_query, load_schema

# --- Funções de Mapeamento ---
# Ter funções de mapeamento separadas torna o código mais limpo e extensível.
# Cada função lida com a estrutura específica de uma tabela.

def map_ccdsGene_to_dict(row):
    """Mapeia uma linha da tabela 'ccdsGene' para um dicionário."""
    return {
        "bin": row.bin,
        "cdsEnd": row.cdsEnd,
        "cdsEndStat": row.cdsEndStat,
        "cdsStart": row.cdsStart,
        "cdsStartStat": row.cdsStartStat,
        "chrom": row.chrom,
        "exonCount": row.exonCount,
        "exonEnds": row.exonEnds,
        "exonFrames": row.exonFrames,
        "exonStarts": row.exonStarts,
        "name": row.name,
        "name2": row.name2,
        "score": row.score,
        "strand": row.strand,
        "txEnd": row.txEnd,
        "txStart": row.txStart,
        
    }

def map_ccdsInfo_to_dict(row):
    """Mapeia uma linha da tabela 'ccdsInfo' para um dicionário."""
    return {
        "ccds": row.ccds,
        "mrnaAcc": row.mrnaAcc,
        "protAcc": row.protAcc,
        "srcDb": row.srcDb,
    }

# Um dicionário para registrar as funções de mapeamento disponíveis.
# Isso permite que o config.yaml especifique qual função usar.
MAP_FUNCTIONS = {
    "map_ccdsGene_to_dict": map_ccdsGene_to_dict,
    "map_ccdsInfo_to_dict": map_ccdsInfo_to_dict,
}

def run(app_config: dict, pipeline_options: PipelineOptions):
    """
    Executa o pipeline de ingestão com tratamento de erros (Dead-Letter Queue).
    """
    gcp_config = app_config['gcp']
    db_config = app_config['source_db']
    destination_dataset = app_config['destination_dataset']
    tables_to_ingest = app_config['tables']

    # 1. Obter credenciais (sem alteração)
    db_creds = get_secret(gcp_config['project_id'], db_config['secret_id'])
    jdbc_url = f"jdbc:mysql://{db_creds['host']}:{db_creds['port']}/{db_creds['database']}?serverTimezone=UTC"

    with beam.Pipeline(options=pipeline_options) as p:
        for table_config in tables_to_ingest:
            try: # 💡 Adicionado para erros de configuração
                table_name = table_config['name']
                _query = load_query(table_config['query_file'])
                _schema = load_schema(table_config['schema_file'])
                map_function_name = table_config.get('map_function')
                
                if not map_function_name or map_function_name not in MAP_FUNCTIONS:
                    raise ValueError(f"Função de mapeamento '{map_function_name}' não encontrada para a tabela '{table_name}'.")
                map_fn = MAP_FUNCTIONS[map_function_name]

                table_spec = f"{gcp_config['project_id']}:{destination_dataset}.{table_name}"
                error_table_spec = f"{gcp_config['project_id']}:{destination_dataset}.{table_name}_errors"
                write_disposition = table_config.get('write_disposition', 'WRITE_TRUNCATE')

                logging.info(f"Criando ramo do pipeline para a tabela: {table_name}")

                # 2. Ler os dados do MySQL (sem alteração)
                source_data = (
                    p
                    | f'ReadFromMySQL_{table_name}' >> ReadFromJdbc(
                        table_name=table_name,
                        driver_class_name='com.mysql.cj.jdbc.Driver',
                        jdbc_url=jdbc_url,
                        username=db_creds['user'],
                        password=db_creds['password'],
                    )
                )

                # 3. Aplicar o mapeamento e validar usando a nova DoFn com saídas múltiplas
                # A DoFn retorna um objeto especial com as saídas que definimos
                processed_results = (
                    source_data
                    | f'MapAndValidate_{table_name}' >> beam.ParDo(MapAndValidate(map_fn)).with_outputs(
                        OutputTags.FAILURE, main=OutputTags.SUCCESS
                      )
                )

                # 4. Separar os resultados de sucesso e de falha
                successful_records = processed_results[OutputTags.SUCCESS]
                failed_records = processed_results[OutputTags.FAILURE]

                # 5. Escrever os registros bem-sucedidos no BigQuery
                (
                    successful_records
                    | f'WriteToBigQuery_{table_name}' >> WriteToBigQuery(
                        table=table_spec,
                        schema={'fields': _schema},
                        create_disposition='CREATE_IF_NEEDED',
                        write_disposition=write_disposition
                    )
                )

                # 6. Escrever os registros com erro em uma tabela de erros separada no BigQuery
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

            except Exception as e:
                # Este 'except' captura erros na CONFIGURAÇÃO do ramo do pipeline
                # (ex: arquivo de query não encontrado, função de mapeamento inválida)
                logging.error(f"Falha ao construir o pipeline para a tabela '{table_config.get('name', 'N/A')}': {e}")
