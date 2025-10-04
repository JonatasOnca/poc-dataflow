CREATE OR REPLACE TABLE `abemcomum-saev-prod.meta_controle.watermarks`
(
  -- O nome único que identifica a sua pipeline. Ex: 'mysql_pedidos_to_gcs'
  pipeline_name STRING NOT NULL,
  
  -- O valor da última marca d'água processada com sucesso.
  -- Usamos STRING para ser flexível (pode guardar um timestamp, um datetime ou um ID numérico).
  last_watermark_value STRING,
  
  -- O timestamp de quando a ÚLTIMA EXECUÇÃO BEM-SUCEDIDA foi concluída.
  -- Essencial para monitorar a "saúde" e o atraso da pipeline.
  last_successful_run_completed_at TIMESTAMP,
  
  -- Opcional, mas útil: O timestamp de quando a última execução (mesmo que tenha falhado) começou.
  last_run_started_at TIMESTAMP,

  -- Opcional, mas útil: O status da última execução para debugging rápido. Ex: 'SUCCESS', 'RUNNING', 'FAILED'
  pipeline_status STRING
)
CLUSTER BY pipeline_name
OPTIONS (
  description="Tabela de controle para armazenar o estado (marcas d'água) de pipelines de ingestão incremental."
);