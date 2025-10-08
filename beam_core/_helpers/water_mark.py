import logging
from datetime import datetime, timezone
from google.cloud import bigquery

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