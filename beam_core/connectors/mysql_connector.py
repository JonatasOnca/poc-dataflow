import logging
import math
from datetime import datetime, timezone

import apache_beam as beam
import mysql.connector
from apache_beam.io.jdbc import ReadFromJdbc
from google.cloud import bigquery


def get_high_water_mark(project_id, dataset_id, table_id, column_name, column_type):
    """ """
    is_timestamp_type = column_type.upper() in ["TIMESTAMP", "DATETIME"]
    default_hwm = datetime(1970, 1, 1, tzinfo=timezone.utc) if is_timestamp_type else 0

    try:
        client = bigquery.Client(project=project_id)
        table_ref = f"`{project_id}.{dataset_id}.{table_id}`"
        query = f"SELECT MAX(`{column_name}`) as hwm FROM {table_ref}"
        logging.info(f"Executando query para HWM: {query}")
        query_job = client.query(query)
        results = query_job.result()
        row = next(iter(results))
        hwm = row.hwm

        if hwm is None:
            logging.warning(f"Tabela '{table_id}' vazia ou HWM nulo. Usando valor padrão.")
            final_hwm = default_hwm
        else:
            final_hwm = hwm
        logging.info(f"High-water mark encontrado: {final_hwm}")

        if is_timestamp_type and isinstance(final_hwm, datetime):
            return final_hwm.isoformat()

        return final_hwm

    except Exception as e:
        logging.warning(
            f"Falha ao obter HWM para '{table_id}' (tabela pode não existir). Usando valor padrão. Erro: {e}"
        )
        if is_timestamp_type:
            return default_hwm.isoformat()
        return default_hwm


def execute_merge(
    project_id, bq_location, target_table_id, staging_table_id, merge_keys, schema_fields
):
    """Executa MERGE no BigQuery entre uma tabela de staging e a tabela alvo.

    Observações:
    - Usa bq_location (ex.: 'US') para garantir que o job execute na mesma
        localização do dataset.
    - A limpeza da tabela de staging NÃO é feita aqui; deve ser orquestrada
        pelo chamador, evitando deleção duplicada e facilitando observabilidade.
    """
    if not staging_table_id:
        logging.info(f"Nenhuma tabela de staging para {target_table_id}. Pulando MERGE.")
        return

    client = bigquery.Client(project=project_id, location=bq_location)

    try:
        on_clause = " AND ".join([f"T.{key} = S.{key}" for key in merge_keys])
        update_cols = [c for c in schema_fields if c not in merge_keys]
        update_clause = ""
        if update_cols:
            update_set_clause = ", ".join([f"T.{c} = S.{c}" for c in update_cols])
            update_clause = f"WHEN MATCHED THEN UPDATE SET {update_set_clause}"
        columns_list = ", ".join(schema_fields)
        values_list = ", ".join([f"S.{c}" for c in schema_fields])

        merge_query = f"""
            MERGE `{target_table_id}` AS T
            USING `{staging_table_id}` AS S
            ON {on_clause}
            {update_clause}
            WHEN NOT MATCHED THEN
                INSERT ({columns_list})
                VALUES ({values_list})
        """

        logging.info(f"Executando MERGE para a tabela '{target_table_id}'")
        client.query(merge_query).result()
        logging.info(f"MERGE para '{target_table_id}' concluído com sucesso.")

    except Exception as e:
        logging.error(
            f"Erro durante a execução do MERGE de '{staging_table_id}' para '{target_table_id}': {e}"
        )
        raise


def read_from_jdbc_partitioned(
    pipeline,
    jdcb_url,
    app_config,
    db_creds,
    table_name,
    base_query,
    partition_column="id",
    num_partitions=20,
):
    """
    Lê uma tabela grande do MySQL em paralelo simulando particionamento,
    dividindo o range [min, max] da coluna de partição.
    """
    min_id, max_id = get_min_max_id(db_creds, table_name, partition_column)

    if min_id is None or max_id is None:
        raise ValueError(
            f"Não foi possível determinar min/max da coluna {partition_column} para {table_name}"
        )

    step = math.ceil((max_id - min_id + 1) / num_partitions)
    partitions = []

    for i in range(num_partitions):
        start = min_id + i * step
        end = min(start + step - 1, max_id)
        query = f"{base_query.strip()} WHERE {partition_column} BETWEEN {start} AND {end}"

        logging.info(
            f"[{table_name}] Partição {i+1}/{num_partitions}: {partition_column} BETWEEN {start} AND {end}"
        )

        p = (
            pipeline
            | f"Read {table_name} chunk {i}"
            >> ReadFromJdbc(
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
