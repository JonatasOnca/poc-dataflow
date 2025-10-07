# Extrai variáveis do config.yaml para usar nos comandos
get_config = python3 -c "import yaml; from jinja2 import Template; from apache_beam.io.filesystems import FileSystems; yaml_as_bytes = FileSystems.open('config.yaml').read(); yaml_as_text = yaml_as_bytes.decode('utf-8'); initial_data = yaml.safe_load(yaml_as_text); template = Template(yaml_as_text); rendered_yaml_text = template.render(initial_data); data = yaml.safe_load(rendered_yaml_text); print(data['$(1)']['$(2)'])"

# Configurações do GCP
PROJECT_ID := $(shell $(call get_config,gcp,project_id))
PROJECT_NUMBER := $(shell gcloud projects describe $(PROJECT_ID) --format='value(projectNumber)')
SERVICE_ACCOUNT_EMAIL := "${PROJECT_NUMBER}-compute@developer.gserviceaccount.com"

REGION := $(shell $(call get_config,gcp,region))
NETWORK := $(shell $(call get_config,gcp,network))
SUBNETWORK := $(shell $(call get_config,gcp,subnetwork))
BUCKET_NAME := $(shell $(call get_config,gcp,bucket_name))

# Configurações do Artefact Registry
AR_REPO := $(shell $(call get_config,artefact_registry,repository))
AR_LOCATION := $(shell $(call get_config,artefact_registry,location))
IMAGE_NAME := $(shell $(call get_config,artefact_registry,image_name))
IMAGE_TAG := latest
IMAGE_URI := $(AR_LOCATION)-docker.pkg.dev/$(PROJECT_ID)/$(AR_REPO)/$(IMAGE_NAME):$(IMAGE_TAG)

# Configurações do Dataflow
TEMPLATE_NAME := $(shell $(call get_config,dataflow,job_name))
TEMPLATE_FILE := $(shell $(call get_config,dataflow,template_file_name))

QUERIES_LOCAL_PATH := queries/
SCHEMAS_LOCAL_PATH := schemas/

GCS_BASE_PATH := $(BUCKET_NAME)/$(IMAGE_NAME)
TEMPLATE_PATH := $(GCS_BASE_PATH)/dataflow/templates/$(TEMPLATE_FILE)
CONFIG_GCS_PATH := $(GCS_BASE_PATH)/config/config.yaml
QUERIES_GCS_PATH := $(GCS_BASE_PATH)/queries/
SCHEMAS_GCS_PATH := $(GCS_BASE_PATH)/schemas/


# ---------Lista de Chunks---------
# ALL
# TABELAS_PEQUENAS_L1
# TABELAS_PEQUENAS_L2
# TABELAS_PEQUENAS_L3
# TABELAS_MEDIAS
# aluno_teste_resposta
# aluno_teste_resposta_historico
# aluno_teste
# report_question
# report_descriptor
# report_race
# turma_aluno
# aluno
# report_subject
# report_not_evaluated
# infrequencia
# report_edition
# transferencia
# turma
CHUNK_NAME := aluno_teste_resposta
CHUNK_NAME_HYPHEN := $(subst _,-,$(CHUNK_NAME))
# ------Tipos de carga------
# backfill
# delta
# merge
LOAD_TYPE := backfill

# Comandos do Makefile
.PHONY: teste sa all setup-gcp build-image build-template upload-config upload-assets run-job docker-test-local clean-env clean cria-venv ativa-venv test-local

variaveis:

	@echo "---------------------------------------"
	@echo "ID do Projeto: $(PROJECT_ID)"
	@echo "Projeto Number: $(PROJECT_NUMBER)"
	@echo "Service Account Email: $(SERVICE_ACCOUNT_EMAIL)"

	@echo "Regiao: $(REGION)"
	@echo "Network: $(NETWORK)"
	@echo "SubNetwork: $(SUBNETWORK)"
	@echo "Bucket Name: $(BUCKET_NAME)"

	@echo "AR Repo: $(AR_REPO)"
	@echo "AR Location: $(AR_LOCATION)"
	@echo "Image Name: $(IMAGE_NAME)"
	@echo "Image Tag: $(IMAGE_TAG)"
	@echo "Image URI: $(IMAGE_URI)"

	@echo "Template Name: $(TEMPLATE_NAME)"
	@echo "Template File: $(TEMPLATE_FILE)"

	@echo "Queries Local Path: $(QUERIES_LOCAL_PATH)"
	@echo "Schemas Local Path: $(SCHEMAS_LOCAL_PATH)"

	@echo "GCS Base Path: $(GCS_BASE_PATH)"
	@echo "Template Path: $(TEMPLATE_PATH)"
	@echo "Config GCS Path: $(CONFIG_GCS_PATH)"
	@echo "Queries GCS Path: $(QUERIES_GCS_PATH)"
	@echo "Schemas GCS Path: $(SCHEMAS_GCS_PATH)"

	@echo "---------------------------------------"

sa:
	@echo "---------------------------------------"
	@echo "------------ACESSSOS--------------------"
	@echo "---------------------------------------"
	@echo "--   Dataflow Worker.                --"
	@echo "--   Storage Object Admin.           --"
	@echo "--   Secret Manager Secret Accessor. --"
	@echo "--   BigQuery Data Editor.           --"
	@echo "--   Artifact Registry Reader.       --"
	@echo "---------------------------------------"
	@echo "ID do Projeto: $(PROJECT_ID)"
	@echo "Número do Projeto: $(PROJECT_NUMBER)"
	@echo "A conta de serviço que precisa de permissões é:"
	@echo "${PROJECT_NUMBER}-compute@developer.gserviceaccount.com"
	@echo "---------------------------------------"

# O alvo 'all' é para o deploy completo na nuvem
all: setup-gcp build-image build-template upload-config upload-assets run-job

update-files: upload-config setup-gcp build-image build-template upload-config upload-assets

# Cria o ambiente virtual
cria-venv:
	@echo "Cria o ambiente virtual criado"
	python3.11 -m venv .venv

# Ativa o ambiente virtual e instala as dependências #WIP - Nao esta funcionando
ativa-venv:	
	@echo "Ative o .venv: source .venv/bin/activate"
	source .venv/bin/activate
	@echo "instala o pip e setuptools com: python -m ensurepip --upgrade"
	python -m ensurepip --upgrade
	@echo "Atualiza o pip: python -m pip install --upgrade pip"
	python -m pip install --upgrade pip
	@echo "Instale as dependências com: pip install -r requirements.txt"
	pip install -r requirements.txt
	
# Configura a infraestrutura necessária no GCP
setup-gcp:
	@echo "Verificando/Criando bucket GCS..."
	@gcloud storage buckets describe $(BUCKET_NAME) >/dev/null 2>&1 || gcloud storage buckets create $(BUCKET_NAME) --project=$(PROJECT_ID) --location=$(REGION)

	@echo "Verificando/Criando repositório do Artifact Registry..."
	@gcloud artifacts repositories describe $(AR_REPO) --project=$(PROJECT_ID) --location=$(AR_LOCATION) >/dev/null 2>&1 || gcloud artifacts repositories create $(AR_REPO) --repository-format=docker --project=$(PROJECT_ID) --location=$(AR_LOCATION)

# Constrói a imagem Docker e envia para o Artifact Registry
build-image:
	@echo "Construindo e enviando a imagem Docker para $(IMAGE_URI)..."
	gcloud builds submit --tag $(IMAGE_URI) . --project=$(PROJECT_ID)

# Cria o arquivo de especificação do Flex Template no GCS
build-template: metadata.json
	@echo "Criando o Flex Template em $(TEMPLATE_PATH)..."
	@gcloud dataflow flex-template build $(TEMPLATE_PATH) \
		--image "$(IMAGE_URI)" \
		--sdk-language "PYTHON" \
		--project=$(PROJECT_ID) \
 		--metadata-file "metadata.json"

# Envia o arquivo de configuração para o GCS
upload-config:
	@echo "Enviando config.yaml para $(CONFIG_GCS_PATH)..."
	@gcloud config set project $(PROJECT_ID)
	@gsutil cp config.yaml $(CONFIG_GCS_PATH)
	@echo ">>> Sincronizando! '$(CONFIG_GCS_PATH)'..."

# Envia os arquivos de assets (SQL, JSON) para o GCS.
upload-assets: 
	@echo ">>> Sincronizando pastas de assets para o bucket '$(BUCKET_NAME)'..."
	gcloud config set project $(PROJECT_ID)
	gsutil -m rsync -r -d $(QUERIES_LOCAL_PATH) $(QUERIES_GCS_PATH)
	gsutil -m rsync -r -d $(SCHEMAS_LOCAL_PATH) $(SCHEMAS_GCS_PATH)
	@echo ">>> Sincronizando! '$(BUCKET_NAME)'..."

update-config:
	@echo "--- Atualiza o Dockerfile.local ---"
	python3 utils/config_modifier.py config.yaml config.local.yaml


run-job: upload-config
	@echo "Executando o job Dataflow '$(TEMPLATE_NAME)' a partir do template..."
	gcloud dataflow flex-template run "$(TEMPLATE_NAME)-$(CHUNK_NAME_HYPHEN)-$(LOAD_TYPE)-`date +%Y%m%d-%H%M%S`" \
		--template-file-gcs-location "$(TEMPLATE_PATH)" \
		--project=$(PROJECT_ID) \
		--region=$(REGION) \
		--parameters=config_file=$(CONFIG_GCS_PATH),chunk_name=$(CHUNK_NAME),load_type=$(LOAD_TYPE)

# Executa o job do Dataflow a partir do template Localmente
docker-test-local:
	@echo "--- Atualiza o Dockerfile.local ---"
	python3 utils/config_modifier.py config.yaml config.local.yaml
	@echo "--- Construindo imagem Docker local para ARM64 (usando Dockerfile.local) ---"
	# Usamos -f para especificar qual Dockerfile usar
	@docker build --platform=linux/amd64 -f Dockerfile.local -t mysql-to-bq-local-test . --load
	@echo "\n--- Executando contêiner de teste localmente ---"
	@docker run --rm -it \
	  --network="host" \
	  -v "$(CURDIR)/config.local.yaml:/app/config.local.yaml:ro" \
	  -v "$(HOME)/.config/gcloud/application_default_credentials.json:/gcp/creds.json:ro" \
	  -e "GOOGLE_APPLICATION_CREDENTIALS=/gcp/creds.json" \
	  -e "GOOGLE_CLOUD_PROJECT=$(PROJECT_ID)" \
	  mysql-to-bq-local-test \
	  python main.py --config_file /app/config.local.yaml --chunk_name $(CHUNK_NAME) --load_type $(LOAD_TYPE)

# Executa o job do Dataflow localmente
test-local:
	@echo "--- Iniciando Teste Local com DirectRunner ---"
	@echo "Lembrete: Certifique-se de que:"
	@echo "1. O ambiente virtual está ativado ('source .venv/bin/activate')."
	@echo "2. Você está autenticado localmente ('gcloud auth application-default login')."
	@echo "3. Sua rede local tem acesso ao banco de dados MySQL."
	@echo "----------------------------------------------------"
	
	python3 utils/config_modifier.py config.yaml config.local.yaml
	
	# Executa o pipeline localmente
	python3 main.py --config_file config.local.yaml --chunk_name $(CHUNK_NAME) --load_type $(LOAD_TYPE)
	
# 	@echo "--- Teste Local Concluído. Limpando arquivo de configuração temporário. ---"
# 	@rm config.local.yaml

# Limpa os anbiente (opcional)
clean-env:
	rm -rf .venv
# 	rm -f metadata.json
# 	rm -f config.local.yaml

# Limpa os arquivos gerados (opcional)
clean:
	@echo "Limpando arquivos temporários..."
	# Remove diretórios __pycache__ recursivamente
	find . -depth -name "__pycache__" -exec rm -rf {} \;
	# Remove arquivos de bytecode Python .pyc
	find . -name "*.pyc" -exec rm -f {} \;

# Cria o arquivo metadata.json dinamicamente se não existir
metadata.json:
	@echo '{"name": "$(TEMPLATE_NAME)", "description": "Template flexível para ingestão MySQL -> BQ (múltiplas tabelas)", "parameters": [{"name": "config_file", "label": "Arquivo de Configuração", "helpText": "Caminho GCS para o arquivo config.yaml"}]}' > metadata.json