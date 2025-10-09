import logging
import math
import mysql.connector
from datetime import datetime, timezone
from google.cloud import bigquery

import apache_beam as beam
from apache_beam.io.jdbc import ReadFromJdbc



def get_high_water_mark(project_id, dataset_id, table_id, column_name, column_type):
    try:
        client = bigquery.Client(project=project_id)
        query = f"SELECT MAX({column_name}) as hwm FROM `{project_id}.{dataset_id}.{table_id}`"
        logging.info(f"Executando query para HWM: {query}")
        query_job = client.query(query)
        results = query_job.result()
        row = next(iter(results))
        hwm = row.hwm

        if hwm is None:
            logging.warning(f"Tabela '{table_id}' vazia ou HWM nulo. Iniciando carga completa.")
            if column_type.upper() in ['TIMESTAMP', 'DATETIME']:
                return datetime(1970, 1, 1, tzinfo=timezone.utc).strftime('%Y-%m-%d %H:%M:%S.%f')
            else:
                return 0

        if column_type.upper() in ['TIMESTAMP', 'DATETIME'] and isinstance(hwm, datetime):
            hwm = hwm.strftime('%Y-%m-%d %H:%M:%S.%f')

        logging.info(f"High-water mark encontrado: {hwm}")
        return hwm

    except Exception as e:
        logging.warning(f"Falha ao obter HWM para '{table_id}', assumindo carga inicial: {e}")
        if column_type.upper() in ['TIMESTAMP', 'DATETIME']:
            return datetime(1970, 1, 1, tzinfo=timezone.utc).strftime('%Y-%m-%d %H:%M:%S.%f')
        else:
            return 0


def execute_merge(project_id, gcp_region, target_table_id, staging_table_id, merge_keys, schema_fields):
    """Função separada para MERGE pós-pipeline"""
    if not staging_table_id:
        logging.info(f"Nenhuma tabela de staging para {target_table_id}. Pulando MERGE.")
        return

    on_clause = " AND ".join([f"T.{key} = S.{key}" for key in merge_keys])
    update_cols = [c for c in schema_fields if c not in merge_keys]
    update_set_clause = ", ".join([f"T.{c} = S.{c}" for c in update_cols])
    columns_list = ", ".join(schema_fields)
    values_list = ", ".join([f"S.{c}" for c in schema_fields])

    merge_query = f"""
        MERGE `{target_table_id}` AS T
        USING `{staging_table_id}` AS S
        ON {on_clause}
        WHEN MATCHED THEN
            UPDATE SET {update_set_clause}
        WHEN NOT MATCHED THEN
            INSERT ({columns_list})
            VALUES ({values_list})
    """

    client = bigquery.Client(project=project_id, location=gcp_region)
    logging.info(f"Executando MERGE: {merge_query}")
    client.query(merge_query).result()
    logging.info("MERGE concluído com sucesso.")

    # Remove tabela de staging
    logging.info(f"Removendo tabela de staging: {staging_table_id}")
    client.delete_table(staging_table_id, not_found_ok=True)


def read_from_jdbc_partitioned(pipeline, jdcb_url, app_config, db_creds, table_name, base_query, partition_column="id", num_partitions=20):
    """
    Lê uma tabela grande do MySQL em paralelo simulando particionamento,
    dividindo o range [min, max] da coluna de partição.
    """
    min_id, max_id = get_min_max_id(db_creds, table_name, partition_column)

    if min_id is None or max_id is None:
        raise ValueError(f"Não foi possível determinar min/max da coluna {partition_column} para {table_name}")

    step = math.ceil((max_id - min_id + 1) / num_partitions)
    partitions = []

    for i in range(num_partitions):
        start = min_id + i * step
        end = min(start + step - 1, max_id)
        query = f"{base_query.strip()} WHERE {partition_column} BETWEEN {start} AND {end}"

        logging.info(f"[{table_name}] Partição {i+1}/{num_partitions}: {partition_column} BETWEEN {start} AND {end}")

        p = (
            pipeline
            | f"Read {table_name} chunk {i}" >> ReadFromJdbc(
                driver_class_name=app_config["database"]["driver_class_name"],
                table_name=table_name,
                jdbc_url=jdcb_url,
                username=db_creds["user"],
                password=db_creds["password"],
                query=query,
                driver_jars=app_config["database"]["driver_jars"],
            )
            | f"To Dict {table_name} chunk {i}" >> beam.Map(lambda r: r._asdict())
        )

        partitions.append(p)

    # Junta as partições lidas em um único PCollection
    return partitions | f"Merge partitions {table_name}" >> beam.Flatten()


def get_min_max_id(db_creds, table_name, column):
    """Obtém o menor e o maior valor da coluna de partição direto no banco."""
    conn = mysql.connector.connect(
        host=db_creds["host"],
        port=db_creds["port"],
        user=db_creds["user"],
        password=db_creds["password"],
        database=db_creds["database"],
    )
    cursor = conn.cursor()
    cursor.execute(f"SELECT MIN({column}), MAX({column}) FROM {table_name}")
    min_id, max_id = cursor.fetchone()
    cursor.close()
    conn.close()
    return min_id, max_id