CREATE OR REPLACE EXTERNAL TABLE `seu-projeto.seu_dataset.tabela_pedidos_externa`
WITH PARTITION COLUMNS (
  -- Opcional, mas recomendado: defina os tipos das colunas de partição
  ano INT64,
  mes STRING, -- Usamos STRING para '09', '10' etc.
  dia STRING
)
OPTIONS (
  -- O URI aponta para o diretório raiz e usa wildcards '*' para descobrir as partições
  uris = ['gs://seu-bucket-landing/mysql/pedidos/*'],
  format = 'PARQUET',
  
  -- A MÁGICA ACONTECE AQUI:
  hive_partitioning_mode = 'AUTO',
  -- Prefixo comum antes da primeira partição
  hive_partitioning_source_uri_prefix = 'gs://seu-bucket-landing/mysql/pedidos/'
);