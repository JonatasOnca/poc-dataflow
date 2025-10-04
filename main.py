import logging
import argparse
from datetime import datetime, timezone
from typing import Dict, List, Any

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.jdbc import ReadFromJdbc
from apache_beam.io.fileio import FileNaming
import pyarrow as pa

# Presume-se que a estrutura de pastas e os helpers existem
from beam_core._helpers.secret_manager import get_secret
from beam_core._helpers.file_handler import load_yaml, load_schema, load_query
from beam_core._helpers.bq_handler import get_last_watermark

# Definição de tipo para clareza
Row = Dict[str, Any]

# -------------------------------------------------------------------
# Funções de Transformação
# -------------------------------------------------------------------
def generic_transform(element: Row) -> Row:
    """
    Transformação genérica que, por padrão, retorna o elemento original.
    Pode ser estendida com lógica de negócio específica.

    Args:
        element: Um dicionário representando uma linha da tabela de origem.

    Returns:
        O dicionário transformado.
    """
    return element

TRANSFORM_MAPPING: Dict[str, callable] = {
    'genero': generic_transform,
    'raca': generic_transform,
}

# -------------------------------------------------------------------
# Helpers Específicos da Pipeline
# -------------------------------------------------------------------
def convert_bq_schema_to_pyarrow(bq_schema: Dict[str, List[Dict]]) -> pa.Schema:
    """
    Converte um schema do BigQuery (carregado de um JSON) para um schema PyArrow.

    Args:
        bq_schema: Dicionário contendo a chave 'fields' com a lista de campos do BQ.

    Returns:
        Um objeto de schema do PyArrow.
    """
    type_mapping = {
        'STRING': pa.string(), 'INTEGER': pa.int64(), 'INT64': pa.int64(),
        'FLOAT': pa.float64(), 'FLOAT64': pa.float64(), 'BOOLEAN': pa.bool_(),
        'BOOL': pa.bool_(), 'TIMESTAMP': pa.timestamp('us'),
        'DATETIME': pa.timestamp('us'), 'DATE': pa.date32(),
    }
    fields = [
        pa.field(field['name'], type_mapping.get(field['type'].upper(), pa.string()))
        for field in bq_schema.get('fields', [])
    ]
    return pa.schema(fields)


class PartitionedFileNaming(FileNaming):
    """
    Define a lógica para criar nomes de arquivos e diretórios particionados
    no estilo Hive (ex: .../ano=2025/mes=10/dia=04/...).
    """
    def __init__(self, base_path: str):
        self._base_path = base_path

    def get_windowed_filename(self, window, pane, shard_index, total_shards, compression, destination: Row) -> str:
        """
        Gera o caminho final do arquivo com base nos dados do elemento.
        """
        year = destination.get('partition_year', 1970)
        month = destination.get('partition_month', '01')
        day = destination.get('partition_day', '01')
        
        partition_path = f"ano={year}/mes={month}/dia={day}"
        filename_prefix = f"{self._base_path}/{partition_path}/data"
        
        return super().get_windowed_filename(
            window, pane, shard_index, total_shards, compression, filename_prefix
        )

# -------------------------------------------------------------------
# Função Principal da Pipeline
# -------------------------------------------------------------------
def run(argv: List[str] = None):
    """Função principal que constrói e executa a pipeline do Apache Beam."""
    parser = argparse.ArgumentParser(description="Pipeline Beam para ingestão incremental do MySQL para GCS.")
    parser.add_argument('--config_file', required=True, help='Caminho (local ou GCS) para o arquivo de configuração YAML.')
    parser.add_argument('--chunk_name', default="ALL", type=str, help='Nome do "chunk" de tabelas a ser executado.')
    parser.add_argument('--table_name', default=None, type=str, help='Nome de uma tabela específica para executar.')

    known_args, pipeline_args = parser.parse_known_args(argv)
    app_config = load_yaml(known_args.config_file)
    logging.info("Configuração da pipeline carregada: %s", app_config)

    # --- Configuração de Banco de Dados ---
    db_creds = get_secret(
        project_id=app_config['gcp']['project_id'], 
        secret_id=app_config['source_db']['secret_id'], 
        version_id=app_config['source_db']['secret_version']
    )
    JDBC_URL = f"jdbc:mysql://{db_creds['host']}:{db_creds['port']}/{db_creds['database']}"
    
    # --- Lógica de Seleção de Tabelas ---
    if known_args.table_name:
        TABLE_LIST = [known_args.table_name]
    else:
        TABLE_LIST = next((chunk['lista'] for chunk in app_config['chunks'] if chunk.get('name', 'ALL') == known_args.chunk_name), [])

    # --- Construção da Pipeline Dinâmica ---
    with beam.Pipeline(options=PipelineOptions(pipeline_args, **app_config['dataflow']['parameters'])) as pipeline:
        for table_name in TABLE_LIST:
            try:
                table_config = next((t for t in app_config['tables'] if t.get('name') == table_name), None)
                if not table_config:
                    logging.warning(f"Configuração para a tabela '{table_name}' não encontrada. Pulando.")
                    continue
                
                # ATUALIZAÇÃO: Nome do job dinâmico para evitar conflitos
                job_name = f"mysql-{table_name}-incremental-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}"
                pipeline.options.view_as(PipelineOptions).view_as(beam.options.pipeline_options.GoogleCloudOptions).job_name = job_name

                # 1. Busca da Watermark e Construção da Query Incremental
                pipeline_id = f"mysql_{table_name}_to_gcs"
                last_watermark = get_last_watermark(
                    project_id=app_config['gcp']['project_id'],
                    **app_config['watermark_table'],
                    pipeline_name=pipeline_id
                )
                logging.info(f"[{table_name}] Watermark atual: {last_watermark}")

                queries_location = app_config['dataflow']['parameters']['queries_location']
                schemas_location = app_config['dataflow']['parameters']['schemas_location']
                gcs_query_file = table_config['query_file']
                gcs_schema_file = table_config['schema_file']
                base_query = load_query(f'{queries_location}/{gcs_query_file}')
                incremental_query = f"{base_query} WHERE {table_config['watermark_column']} > '{last_watermark}'"
                
                # 2. Leitura e Transformação
                rows = (
                    pipeline
                    | f'Read Incremental {table_name}' >> ReadFromJdbc(
                        driver_class_name=app_config['database']['driver_class_name'], table_name=table_name,
                        jdbc_url=JDBC_URL, username=db_creds['user'], password=db_creds['password'],
                        query=incremental_query, driver_jars=app_config['database']['driver_jars']
                    )
                    | f'Convert {table_name} to Dict' >> beam.Map(lambda row: row._asdict())
                )
                
                transformed_data = rows | f'Transform {table_name}' >> beam.Map(TRANSFORM_MAPPING.get(table_name, generic_transform))

                # 3. Enriquecer Dados com Chaves de Partição
                watermark_col = table_config['watermark_column']
                def add_partition_fields(element: Row) -> Row:
                    enriched_element = element.copy()
                    watermark_value = element.get(watermark_col)
                    
                    if isinstance(watermark_value, datetime):
                        enriched_element['partition_year'] = watermark_value.year
                        enriched_element['partition_month'] = f"{watermark_value.month:02d}"
                        enriched_element['partition_day'] = f"{watermark_value.day:02d}"
                    else:
                        enriched_element.update({'partition_year': 1970, 'partition_month': '01', 'partition_day': '01'})
                        logging.warning(f"Não foi possível gerar partição para o valor '{watermark_value}'. Usando default.")
                    return enriched_element

                partitioned_data = transformed_data | f'Add Partition Fields for {table_name}' >> beam.Map(add_partition_fields)

                # 4. Escrita Particionada para o GCS
                pyarrow_schema = convert_bq_schema_to_pyarrow(load_schema(f'{schemas_location}/{gcs_schema_file}'))
                base_gcs_path = table_config['landing_zone_gcs_path_table']

                (partitioned_data
                    | f'Write {table_name} Partitioned' >> beam.io.WriteToParquet(
                        file_path_prefix=base_gcs_path,
                        schema=pyarrow_schema,
                        file_naming=PartitionedFileNaming(base_gcs_path),
                        # Otimização para tipos de timestamp do BQ
                        use_fast_timestamp_micros=True
                    )
                )
                
                # 5. Calcular e Salvar a Nova Watermark para o Orquestrador
                (transformed_data
                    | f'Extract {table_name} Watermark' >> beam.Map(lambda row: row[watermark_col])
                    | f'Get {table_name} Max Watermark' >> beam.CombineGlobally(max).without_defaults()
                    | f'Write {table_name} New Watermark' >> beam.io.WriteToText(
                        f"{base_gcs_path}/_temp_exec/{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}/_NEW_WATERMARK",
                        shard_name_template='',
                        append_trailing_newlines=False
                    )
                )

            except Exception as e:
                logging.error(f"Falha ao construir a pipeline para a tabela '{table_name}': {e}", exc_info=True)

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()