import logging
from google.cloud import bigquery

def get_last_watermark(project_id: str, dataset_id: str, table_id: str, pipeline_name: str) -> str:
    """
    Busca o valor da última marca d'água processada com sucesso para uma pipeline específica.

    Args:
        project_id: O ID do projeto GCP.
        dataset_id: O dataset da tabela de controle (ex: 'meta_controle').
        table_id: O nome da tabela de controle (ex: 'watermarks').
        pipeline_name: O identificador único da pipeline (ex: 'mysql_pedidos_to_gcs').

    Returns:
        O valor da última marca d'água como string. Se nenhuma for encontrada,
        retorna um valor padrão para a primeira execução.
    """
    client = bigquery.Client(project=project_id)
    table_ref = f"`{project_id}.{dataset_id}.{table_id}`"
    
    # Usar query parametrizada para segurança
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("pipeline_name", "STRING", pipeline_name),
        ]
    )
    
    query = f"""
        SELECT last_watermark_value
        FROM {table_ref}
        WHERE pipeline_name = @pipeline_name
        LIMIT 1
    """

    try:
        logging.info(f"Buscando watermark para a pipeline: {pipeline_name}")
        query_job = client.query(query, job_config=job_config)
        
        # Pega o resultado. O iterador estará vazio se não encontrar nada.
        for row in query_job.result():
            logging.info(f"Watermark encontrada: {row.last_watermark_value}")
            return row.last_watermark_value
            
        # Se o loop terminar sem retornar, significa que não há entrada para esta pipeline
        logging.warning(f"Nenhuma watermark encontrada para '{pipeline_name}'. Usando valor padrão de início.")
        return "1970-01-01 00:00:00" # Valor seguro para a primeira execução

    except Exception as e:
        logging.error(f"Falha ao buscar watermark do BigQuery: {e}", exc_info=True)
        # Em caso de falha, é mais seguro parar a pipeline do que arriscar processar dados errados
        raise

def update_watermark(project_id: str, dataset_id: str, table_id: str, pipeline_name: str, new_watermark_value: str):
    """
    Atualiza ou insere o valor da marca d'água para uma pipeline.
    Esta função deve ser chamada pelo ORQUESTRADOR no final de uma execução bem-sucedida.

    Args:
        project_id: O ID do projeto GCP.
        dataset_id: O dataset da tabela de controle.
        table_id: O nome da tabela de controle.
        pipeline_name: O identificador único da pipeline.
        new_watermark_value: O novo valor da marca d'água a ser salvo.
    """
    client = bigquery.Client(project=project_id)
    table_ref = f"`{project_id}.{dataset_id}.{table_id}`"

    # A query MERGE é ideal pois ela faz um "upsert":
    # - UPDATE se a pipeline_name já existe
    # - INSERT se é a primeira vez
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("pipeline_name", "STRING", pipeline_name),
            bigquery.ScalarQueryParameter("new_watermark", "STRING", str(new_watermark_value)),
        ]
    )

    merge_query = f"""
    MERGE {table_ref} AS T
    USING (SELECT @pipeline_name AS pipeline_name) AS S
    ON T.pipeline_name = S.pipeline_name
    WHEN MATCHED THEN
      UPDATE SET
        last_watermark_value = @new_watermark,
        last_successful_run_completed_at = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN
      INSERT (pipeline_name, last_watermark_value, last_successful_run_completed_at)
      VALUES (@pipeline_name, @new_watermark, CURRENT_TIMESTAMP())
    """
    
    try:
        logging.info(f"Atualizando watermark para '{pipeline_name}' com o valor '{new_watermark_value}'")
        query_job = client.query(merge_query, job_config=job_config)
        query_job.result() # Espera a query terminar
        logging.info("Watermark atualizada com sucesso.")
    except Exception as e:
        logging.error(f"Falha ao atualizar a watermark no BigQuery: {e}", exc_info=True)
        raise