import logging
import argparse
import uuid
from datetime import datetime

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.jdbc import ReadFromJdbc
from google.cloud import bigquery
from google.api_core.exceptions import NotFound

from beam_core._helpers.add_metadata import AddMetadataDoFn
from beam_core._helpers.file_handler import load_yaml, load_schema, load_query
from beam_core.connectors.mysql_connector import execute_merge, get_high_water_mark, read_from_jdbc_partitioned
from beam_core.connectors.secret_manager import get_secret
from beam_core._helpers.transform_functions import TRANSFORM_MAPPING, generic_transform

def run():
    parser = argparse.ArgumentParser()
    parser.add_argument('--config_file', required=True, help='Caminho GCS para config.yaml')
    parser.add_argument('--chunk_name', default="ALL", type=str)
    parser.add_argument('--table_name', default=None, type=str)
    parser.add_argument('--load_type', choices=['backfill', 'delta', 'merge'], default='delta')
    known_args, pipeline_args = parser.parse_known_args()

    app_config = load_yaml(known_args.config_file)
    logging.info(f"Iniciando pipeline com config: {app_config}")
    
    chunk_name = known_args.chunk_name
    table_name = known_args.table_name
    load_type = known_args.load_type
    job_name = app_config['dataflow']['job_name']

    chunk_name_hyphen_lower = chunk_name.replace("_", "-").lower()
    timestamp_formatado = datetime.now().strftime("%Y%m%d-%H%M%S")
    job_execution_id = f"{job_name}-{chunk_name_hyphen_lower}-{load_type}-date-{timestamp_formatado}-uuid-{uuid.uuid4()}"

    db_creds = get_secret(
        project_id=app_config['gcp']['project_id'],
        secret_id=app_config['source_db']['secret_id'],
        version_id=app_config['source_db']['secret_version']
    )

    FETCH_SIZE = app_config.get('dataflow', {}).get('parameters', {}).get('fetch_size')

    base_jdbc_url = f"jdbc:mysql://{db_creds['host']}:{db_creds['port']}/{db_creds['database']}"
    fetch_params = f"?useCursorFetch=true&defaultFetchSize={FETCH_SIZE}" if FETCH_SIZE else ""
    JDBC_URL = f"{base_jdbc_url}{fetch_params}"

    pipeline_options = PipelineOptions(
        pipeline_args,
        runner=app_config['dataflow']['parameters']['runner'],
        project=app_config['gcp']['project_id'],
        region=app_config['gcp']['region'],
        staging_location=app_config['dataflow']['parameters']['staging_location'],
        temp_location=app_config['dataflow']['parameters']['temp_location'],
        job_name=job_execution_id,
        setup_file='./setup.py'
    )

    project_id = app_config['gcp']['project_id']
    bq_dataset = app_config['bronze_dataset']
    queries_location = app_config['dataflow']['parameters']['queries_location']
    schemas_location = app_config['dataflow']['parameters']['schemas_location']

    if table_name:
        TABLE_LIST = [table_name]
    else:
        TABLE_LIST = next((c['lista'] for c in app_config['chunks'] if c.get('name', 'ALL') == chunk_name), [])

    merge_tasks = []

    with beam.Pipeline(options=pipeline_options) as pipeline:
        for table_name in TABLE_LIST:
            table_config = next((t for t in app_config['tables'] if t.get('name') == table_name), None)
            if not table_config:
                logging.error(f"Configuração para '{table_name}' não encontrada. Pulando a ingestão desta tabela.")
                continue

            schema = load_schema(f"{schemas_location}/{table_config['schema_file']}")
            base_query = load_query(f"{queries_location}/{table_config['query_file']}")

            if not schema or not base_query:
                logging.error(f"Schema ou query não encontrados para {table_name}. Pulando.")
                continue

            final_query = base_query
            target_table_id = f"{project_id}.{bq_dataset}.{table_name}"
            write_disposition = beam.io.BigQueryDisposition.WRITE_TRUNCATE
            destination_table_for_write = target_table_id
            destination_table_for_query = None
            current_load_type = load_type

            if load_type in ['delta', 'merge']:
                delta_config = table_config.get('delta_config')
                if delta_config and 'column' in delta_config:
                    if delta_config['column']:
                        hwm_column = delta_config['column']
                        hwm_type = delta_config.get('type', 'TIMESTAMP').upper()
                        high_water_mark = get_high_water_mark(project_id, bq_dataset, table_name, hwm_column, hwm_type)
                        condition_value = f"'{high_water_mark}'" if hwm_type not in ['INTEGER', 'BIGINT', 'INT', 'NUMERIC'] else str(high_water_mark)
                        base_query_upper = base_query.strip().upper()
                        where_clause = f"WHERE {hwm_column} > {condition_value}"
                        if 'WHERE' in base_query_upper:
                            where_clause = f"AND {hwm_column} > {condition_value}"

                        final_query = f"{base_query.strip()} {where_clause}"
                    else: 
                        final_query = f"{base_query.strip()}"

                    if current_load_type == 'delta':
                        write_disposition = beam.io.BigQueryDisposition.WRITE_APPEND
                    elif current_load_type == 'merge':
                        staging_table_name = f"{table_name}_staging_{uuid.uuid4().hex}"
                        write_disposition = beam.io.BigQueryDisposition.WRITE_TRUNCATE
                        destination_table_for_write = f"{project_id}.{bq_dataset}.{staging_table_name}"
                        destination_table_for_query = destination_table_for_write
                        merge_tasks.append({
                            'target_table_id': target_table_id,
                            'staging_table_id': destination_table_for_query,
                            'merge_keys': table_config['merge_config']['keys'],
                            'schema_fields': [f['name'] for f in schema['fields']]
                        })

            additional_bq_params = {}
            if table_config.get('partitioning_config') and table_config['partitioning_config'].get('column'):
                additional_bq_params['timePartitioning'] = {
                    'type': table_config['partitioning_config'].get('type', 'DAY').upper(),
                    'field': table_config['partitioning_config']['column']
                }
            if table_config.get('clustering_config') and table_config['clustering_config'].get('columns'):
                clustering_fields = table_config['clustering_config']['columns'][:4]
                additional_bq_params['clustering'] = {'fields': clustering_fields}

            transform_function = TRANSFORM_MAPPING.get(table_name, generic_transform)

            read_partitioning_config = table_config.get('read_partitioning_config')
            
            if load_type == 'backfill' and read_partitioning_config and read_partitioning_config.get('column'):
                logging.info(f"Usando leitura particionada para a tabela '{table_name}'.")
                rows = read_from_jdbc_partitioned(
                    pipeline=pipeline,
                    jdcb_url=JDBC_URL,
                    app_config=app_config,
                    db_creds=db_creds,
                    table_name=table_name,
                    base_query=final_query,
                    partition_column=read_partitioning_config.get('column'),
                    num_partitions=read_partitioning_config.get('num_partitions', 1),
                )
            else:
                logging.info(f"Usando leitura padrão (não particionada) para a tabela '{table_name}'.")
                rows = (
                    pipeline
                    | f"Read {table_name}" >> ReadFromJdbc(
                        driver_class_name=app_config["database"]["driver_class_name"],
                        table_name=table_name,
                        jdbc_url=JDBC_URL,
                        username=db_creds["user"],
                        password=db_creds["password"],
                        query=final_query,
                        driver_jars=app_config["database"]["driver_jars"],
                    )
                    | f"To Dict {table_name}" >> beam.Map(lambda r: r._asdict())
                )

            transformed_data = rows | f"Transform {table_name}" >> beam.Map(transform_function)
            
            processed_rows = transformed_data | f'Add Metadata {table_name}' >> beam.ParDo(AddMetadataDoFn(table_name, job_execution_id))

            _ = processed_rows | f"Write {table_name} to BigQuery" >> beam.io.WriteToBigQuery(
                table=destination_table_for_write,
                schema=schema,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition=write_disposition,
                additional_bq_parameters=additional_bq_params,
                insert_retry_strategy='RETRY_NEVER',
                custom_gcs_temp_location=app_config['dataflow']['parameters']['temp_location']
            )
    if load_type == 'merge':
        if not merge_tasks:
            logging.info("Nenhuma tarefa de MERGE para executar.")
            return

        client = bigquery.Client(project=project_id)
        
        for task in merge_tasks:
            staging_table_id = task['staging_table_id']
            target_table_id = task['target_table_id']
            logging.info(f"Iniciando processo de MERGE para a tabela de destino '{target_table_id}'")
            
            try:
                staging_table = client.get_table(staging_table_id)
                if staging_table.num_rows == 0:
                    logging.warning(f"Tabela de staging '{staging_table_id}' está vazia. Nenhum dado para mesclar. Pulando MERGE.")
                    continue

                execute_merge(
                    project_id=project_id,
                    gcp_region=app_config['region'],
                    target_table_id=target_table_id,
                    staging_table_id=staging_table_id,
                    merge_keys=task['merge_keys'],
                    schema_fields=task['schema_fields']
                )
                logging.info(f"MERGE concluído com sucesso para '{target_table_id}'.")
            except NotFound:
                logging.warning(f"Tabela de staging '{staging_table_id}' não foi encontrada. O job pode não ter produzido dados.")
            except Exception as e:
                logging.error(f"Erro durante a execução do MERGE de '{staging_table_id}' para '{target_table_id}': {e}")
            finally:
                try:
                    logging.info(f"Tentando deletar a tabela de staging '{staging_table_id}'...")
                    client.delete_table(staging_table_id, not_found_ok=True)
                    logging.info(f"Tabela de staging '{staging_table_id}' deletada com sucesso.")
                except Exception as e:
                    logging.critical(f"FALHA CRÍTICA AO LIMPAR: Não foi possível deletar a tabela de staging '{staging_table_id}'. Ação manual pode ser necessária. Erro: {e}")


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
