# MySQL -> BigQuery (Dataflow Flex Template)

Pipeline Apache Beam/Dataflow para ingestão de múltiplas tabelas MySQL no BigQuery com suporte a backfill, delta e merge.

## Como usar

1. Configure o `config.yaml` (projeto, região, buckets, datasets, tabelas).
2. Gere a imagem e o template flex:
   - `make setup-gcp`
   - `make build-image`
   - `make build-template`
3. Publique configs e assets:
   - `make upload-config`
   - `make upload-assets`
4. Execute o job:
   - `make run-job`

Para testes locais:
- `make test-local` (DirectRunner)
- `make docker-test-local` (container local)

## Estrutura

- `main.py`: orquestração do pipeline.
- `beam_core/_helpers`: utilitários (leitura de arquivos, metadados, transforms).
- `beam_core/connectors`: HWM, MERGE, Secret Manager, leitura JDBC (particionada e simples).
- `queries/` e `schemas/`: SQLs de origem e schemas BQ.
- `Makefile`: alvos para build/deploy/execução local.

## Boas práticas implementadas

- Filtragem de colunas pelos schemas antes de gravar no BigQuery (evita erros de campos extras).
- MERGE com controle do `bq_location` e limpeza de staging orquestrada após execução.
- Evita logging de configs sensíveis.

## Requisitos

- Python 3.11
- gcloud, gsutil
- Permissões: Dataflow Worker, Storage, Secret Manager, BigQuery Data Editor, Artifact Registry Reader.

## Troubleshooting

- Erros de schema: valide se o JSON em `schemas/` contém todos os campos e tipos.
- Erros de autenticação: garanta `GOOGLE_APPLICATION_CREDENTIALS` configurado ou `gcloud auth application-default login`.
- Conexão JDBC: verifique `driver_jars` e credenciais no Secret Manager.
