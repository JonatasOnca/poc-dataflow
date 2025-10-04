import logging
import argparse
from datetime import datetime, timezone
from typing import Dict, List, Any
import os

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions
from apache_beam.io.jdbc import ReadFromJdbc
from apache_beam.io.parquetio import WriteToParquet
import pyarrow as pa

# -------------------------------------------------------------------
# Helpers personalizados
# -------------------------------------------------------------------
from beam_core._helpers.secret_manager import get_secret
from beam_core._helpers.file_handler import load_yaml, load_schema, load_query
from beam_core._helpers.bq_handler import get_last_watermark

Row = Dict[str, Any]

# -------------------------------------------------------------------
# Funções de Transformação
# -------------------------------------------------------------------
TRANSFORM_MAPPING: Dict[str, callable] = {
    'genero': lambda el: el,
    'raca': lambda el: el,
}

class ProcessAndEnrichData(beam.DoFn):
    """Aplica transformações e adiciona campos de particionamento."""
    def __init__(self, table_name: str, watermark_col: str):
        self.table_name = table_name
        self.watermark_col = watermark_col
        self.transform_fn = TRANSFORM_MAPPING.get(self.table_name, lambda el: el)

    def process(self, element: Row):
        transformed = self.transform_fn(element)
        enriched = transformed.copy()
        watermark_value = enriched.get(self.watermark_col)

        if isinstance(watermark_value, datetime):
            enriched['partition_year'] = watermark_value.year
            enriched['partition_month'] = f"{watermark_value.month:02d}"
            enriched['partition_day'] = f"{watermark_value.day:02d}"
        else:
            enriched.update({'partition_year': 1970, 'partition_month': '01', 'partition_day': '01'})

        yield enriched

# -------------------------------------------------------------------
# Schema converter
# -------------------------------------------------------------------
def convert_bq_schema_to_pyarrow(bq_schema: Dict[str, List[Dict]]) -> pa.Schema:
    type_mapping = {
        'STRING': pa.string(), 'INTEGER': pa.int64(), 'INT64': pa.int64(),
        'FLOAT': pa.float64(), 'FLOAT64': pa.float64(), 'BOOLEAN': pa.bool_(),
        'BOOL': pa.bool_(), 'TIMESTAMP': pa.timestamp('us'),
        'DATETIME': pa.timestamp('us'), 'DATE': pa.date32(),
    }
    fields = []
    for field in bq_schema.get('fields', []):
        field_type_upper = field.get('type', '').upper()
        pa_type = type_mapping.get(field_type_upper, pa.string())
        fields.append(pa.field(field.get('name', 'unknown'), pa_type))
    return pa.schema(fields)

# -------------------------------------------------------------------
# Função principal
# -------------------------------------------------------------------
def run(argv: List[str] = None) -> None:
    parser = argparse.ArgumentParser(description="Pipeline Beam - MySQL para GCS incremental.")
    parser.add_argument('--config_file', required=True, help='Caminho do arquivo YAML de configuração.')
    parser.add_argument('--chunk_name', default="ALL", help='Nome do chunk de tabelas a ser executado.')
    parser.add_argument('--table_name', default=None, help='Nome de uma tabela específica.')
    known_args, pipeline_args = parser.parse_known_args(argv)

    app_config = load_yaml(known_args.config_file)
    logging.info("Configuração carregada com sucesso.")

    # Credenciais do banco
    db_creds = get_secret(
        project_id=app_config['gcp']['project_id'],
        secret_id=app_config['source_db']['secret_id'],
        version_id=app_config['source_db']['secret_version']
    )
    JDBC_URL = f"jdbc:mysql://{db_creds['host']}:{db_creds['port']}/{db_creds['database']}"

    # Definição das tabelas que serão processadas
    if known_args.table_name:
        TABLE_LIST = [known_args.table_name]
        job_identifier = known_args.table_name
    else:
        TABLE_LIST = next(
            (chunk['lista'] for chunk in app_config['chunks']
             if chunk.get('name', 'ALL') == known_args.chunk_name),
            []
        )
        job_identifier = known_args.chunk_name.lower().replace('_', '-')

    # 🔁 Um job independente por tabela
    for table_name in TABLE_LIST:
        try:
            logging.info(f"🔹 Iniciando pipeline para tabela: {table_name}")

            table_config = next((t for t in app_config['tables'] if t.get('name') == table_name), None)
            if not table_config:
                logging.warning(f"Tabela '{table_name}' não encontrada na configuração. Pulando.")
                continue

            pipeline_id = f"mysql_{table_name}_to_gcs"
            last_watermark = get_last_watermark(
                project_id=app_config['gcp']['project_id'],
                **app_config['watermark_table'],
                pipeline_name=pipeline_id
            )
            logging.info(f"[{table_name}] Última watermark: {last_watermark}")

            # Parâmetros de job únicos por tabela
            run_timestamp = datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')
            job_name = f"mysql-{table_name}-incremental-{run_timestamp}"

            pipeline_options = PipelineOptions(pipeline_args, **app_config['dataflow']['parameters'])
            pipeline_options.view_as(GoogleCloudOptions).job_name = job_name

            queries_location = app_config['dataflow']['parameters']['queries_location']
            schemas_location = app_config['dataflow']['parameters']['schemas_location']

            base_query = load_query(f"{queries_location}/{table_config['query_file']}")
            incremental_query = f"{base_query} WHERE {table_config['watermark_column']} > '{last_watermark}'"

            # 🚀 Cada tabela roda em seu próprio pipeline
            with beam.Pipeline(options=pipeline_options) as pipeline:
                # 1️⃣ Leitura do MySQL
                rows = (
                    pipeline
                    | f"Read_{table_name}" >> ReadFromJdbc(
                        driver_class_name=app_config['database']['driver_class_name'],
                        table_name=table_name,
                        jdbc_url=JDBC_URL,
                        username=db_creds['user'],
                        password=db_creds['password'],
                        query=incremental_query,
                        driver_jars=app_config['database']['driver_jars']
                    )
                    | f"ToDict_{table_name}" >> beam.Map(lambda row: row._asdict())
                )

                # 2️⃣ Enriquecimento
                enriched = (
                    rows
                    | f"Enrich_{table_name}" >> beam.ParDo(
                        ProcessAndEnrichData(table_name, table_config['watermark_column'])
                    )
                )

                # 3️⃣ Escrita particionada em Parquet
                pyarrow_schema = convert_bq_schema_to_pyarrow(
                    load_schema(f"{schemas_location}/{table_config['schema_file']}")
                )
                base_gcs_path = table_config['landing_zone_gcs_path_table']

                def add_partition_path(element):
                    year = element.get('partition_year', 1970)
                    month = element.get('partition_month', '01')
                    day = element.get('partition_day', '01')
                    path = os.path.join(base_gcs_path, f"ano={year}", f"mes={month}", f"dia={day}")
                    return (path, element)

                partitioned = (
                    enriched
                    | f"AddPartitionPath_{table_name}" >> beam.Map(add_partition_path)
                )

                grouped = (
                    partitioned
                    | f"GroupByPath_{table_name}" >> beam.GroupByKey()
                )

                def write_partition(kv):
                    path, rows_iter = kv
                    rows_list = list(rows_iter)
                    if not path:
                        logging.warning(f"[{table_name}] Caminho nulo encontrado, pulando partição.")
                        return
                    yield from (
                        rows_list
                        | f"WriteParquet_{table_name}" >> WriteToParquet(
                            file_path_prefix=path,
                            schema=pyarrow_schema,
                            file_name_suffix=".parquet",
                            codec="snappy"
                        )
                    )

                _ = (
                    grouped
                    | f"WritePartitions_{table_name}" >> beam.FlatMap(write_partition)
                )

                # 4️⃣ Atualiza watermark
                watermark_col = table_config['watermark_column']
                (
                    rows
                    | f"ExtractWatermark_{table_name}" >> beam.Map(lambda row: row.get(watermark_col))
                    | f"FilterValidWatermark_{table_name}" >> beam.Filter(lambda v: v is not None)
                    | f"MaxWatermark_{table_name}" >> beam.CombineGlobally(max).without_defaults()
                    | f"WriteWatermark_{table_name}" >> beam.io.WriteToText(
                        file_path_prefix=os.path.join(
                            base_gcs_path, "_temp_exec", run_timestamp, "_NEW_WATERMARK"
                        ),
                        shard_name_template="",
                        append_trailing_newlines=False
                    )
                )

                logging.info(f"✅ Pipeline '{table_name}' concluído com sucesso.")

        except Exception as e:
            logging.error(f"❌ Erro ao construir pipeline para '{table_name}': {e}", exc_info=True)

# -------------------------------------------------------------------
if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
