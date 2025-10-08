import logging

from google.cloud import bigquery


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
