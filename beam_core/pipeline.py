import apache_beam as beam
import logging
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.bigquery import WriteToBigQuery
from apache_beam.io.jdbc import ReadFromJdbc

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
    Executa o pipeline de ingestão para múltiplas tabelas do MySQL para o BigQuery.
    """
    gcp_config = app_config['gcp']
    db_config = app_config['source_db']
    destination_dataset = app_config['destination_dataset']
    tables_to_ingest = app_config['tables']

    # 1. Obter credenciais do banco de dados do Secret Manager (feito uma vez)
    db_creds = get_secret(gcp_config['project_id'], db_config['secret_id'])
    # logging.info(f"Secret: {db_config}")
    jdbc_url = f"jdbc:mysql://{db_creds['host']}:{db_creds['port']}/{db_creds['database']}?serverTimezone=UTC"
    # logging.info(f"jdbc_url: {jdbc_url}")

    with beam.Pipeline(options=pipeline_options) as p:
        # 2. Iterar sobre cada tabela na configuração para criar um ramo no pipeline
        for table_config in tables_to_ingest:
            table_name = table_config['name']
            _query = load_query(table_config['query_file'])
            _schema = load_schema(table_config['schema_file'])
            table_spec = f"{gcp_config['project_id']}:{destination_dataset}.{table_name}"
            write_disposition = table_config.get('write_disposition', 'WRITE_TRUNCATE')

            # Obter a função de mapeamento correta do nosso registro
            map_function_name = table_config.get('map_function')
            if not map_function_name or map_function_name not in MAP_FUNCTIONS:
                raise ValueError(f"Função de mapeamento '{map_function_name}' não encontrada para a tabela '{table_name}'.")
            map_fn = MAP_FUNCTIONS[map_function_name]

            logging.info(f"Criando ramo do pipeline para a tabela: {table_name}")

            # 3. Definir o ramo do pipeline para esta tabela
            (
                p
                # Usar um label único para cada etapa de leitura
                | f'ReadFromMySQL_{table_name}' >> ReadFromJdbc(
                    table_name=table_name,
                    driver_class_name='com.mysql.cj.jdbc.Driver',
                    driver_jars=['/app/drivers/mysql-connector-j-9.4.0.jar'],
                    jdbc_url=jdbc_url,
                    username=db_creds['user'],
                    password=db_creds['password'],
                    # query=_query
                )
                # Usar um label único para cada etapa de mapeamento
                | f'MapToDict_{table_name}' >> beam.Map(map_fn)
                # Usar um label único para cada etapa de escrita
                | f'WriteToBigQuery_{table_name}' >> WriteToBigQuery(
                    table=table_spec,
                    schema={'fields': _schema},
                    create_disposition='CREATE_IF_NEEDED',
                    write_disposition=write_disposition
                )
            )