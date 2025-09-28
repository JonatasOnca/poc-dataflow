# Extrai variáveis do config.yaml para usar nos comandos
get_config = python3 -c "import yaml; f=open('config.yaml'); d=yaml.safe_load(f); print(d['$(1)']['$(2)'])"

# Configurações do GCP
PROJECT_ID := $(shell $(call get_config,gcp,project_id))
REGION := $(shell $(call get_config,gcp,region))
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
TEMPLATE_PATH := $(BUCKET_NAME)/templates/$(TEMPLATE_FILE)
CONFIG_GCS_PATH := $(BUCKET_NAME)/config/config.yaml

# Comandos do Makefile
# Adicionado 'upload-config' e dependência em 'run-job'
.PHONY: all setup-gcp build-image build-template upload-config run-job clean venv

# O alvo 'all' agora inclui a etapa de upload
all: setup-gcp build-image build-template upload-config run-job

# Cria o ambiente virtual e instala as dependências
venv:
	python3.9 -m venv .venv
	@echo "Ambiente virtual criado. Ative com: source .venv/bin/activate"
	@echo "Depois, instale as dependências com: pip install -r requirements.txt"

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
	gcloud dataflow flex-template build $(TEMPLATE_PATH) \
		--image "$(IMAGE_URI)" \
		--sdk-language "PYTHON" \
		--metadata-file "metadata.json" \
		--project=$(PROJECT_ID)

# --- NOVO ALVO ---
# Envia o arquivo de configuração para o GCS
upload-config:
	@echo "Enviando config.yaml para $(CONFIG_GCS_PATH)..."
	gsutil cp config.yaml $(CONFIG_GCS_PATH)

# --- ALVO ATUALIZADO ---
# Executa o job do Dataflow a partir do template, agora dependendo de 'upload-config'
run-job: upload-config
	@echo "Executando o job Dataflow '$(TEMPLATE_NAME)' a partir do template..."
	gcloud dataflow flex-template run "$(TEMPLATE_NAME)-`date +%Y%m%d-%H%M%S`" \
		--template-file-gcs-location "$(TEMPLATE_PATH)" \
		--project=$(PROJECT_ID) \
		--region=$(REGION) \
		--parameters=config_file=$(CONFIG_GCS_PATH)

# Limpa os arquivos gerados (opcional)
clean:
	rm -rf .venv
	rm -f metadata.json

# Cria o arquivo metadata.json dinamicamente se não existir
metadata.json:
	@echo '{"name": "$(TEMPLATE_NAME)", "description": "Template flexível para ingestão MySQL -> BQ (múltiplas tabelas)", "parameters": [{"name": "config_file", "label": "Arquivo de Configuração", "helpText": "Caminho GCS para o arquivo config.yaml"}]}' > metadata.json