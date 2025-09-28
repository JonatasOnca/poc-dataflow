# Extrai variáveis do config.yaml para usar nos comandos
get_config = python3 -c "import yaml; f=open('config.yaml'); d=yaml.safe_load(f); print(d['$(1)']['$(2)'])"

# Configurações do GCP
PROJECT_ID := $(shell $(call get_config,gcp,project_id))
PROJECT_NUMBER := $(shell gcloud projects describe $(PROJECT_ID) --format='value(projectNumber)')
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
.PHONY: all setup-gcp build-image build-template upload-config run-job clean venv test-local

SA:
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

# Envia o arquivo de configuração para o GCS
upload-config:
	@echo "Enviando config.yaml para $(CONFIG_GCS_PATH)..."
	gsutil cp config.yaml $(CONFIG_GCS_PATH)

# Executa o job do Dataflow a partir do template
run-job: upload-config
	@echo "Executando o job Dataflow '$(TEMPLATE_NAME)' a partir do template..."
	gcloud dataflow flex-template run "$(TEMPLATE_NAME)-`date +%Y%m%d-%H%M%S`" \
		--template-file-gcs-location "$(TEMPLATE_PATH)" \
		--project=$(PROJECT_ID) \
		--region=$(REGION) \
		--parameters=config_file=$(CONFIG_GCS_PATH)

# Executa o job do Dataflow localmente
test-local:
	@echo "--- Iniciando Teste Local com DirectRunner ---"
	@echo "Lembrete: Certifique-se de que:"
	@echo "1. O ambiente virtual está ativado ('source .venv/bin/activate')."
	@echo "2. Você está autenticado localmente ('gcloud auth application-default login')."
	@echo "3. Sua rede local tem acesso ao banco de dados MySQL."
	@echo "----------------------------------------------------"
	
	python utils/config_modifier.py config.yaml config.local.yaml
	
	# Executa o pipeline localmente
	python main.py --config_file config.local.yaml
	
	@echo "--- Teste Local Concluído. Limpando arquivo de configuração temporário. ---"
	@rm config.local.yaml

# Limpa os anbiente (opcional)
clean-env:
	rm -rf .venv
	rm -f metadata.json
	rm -f config.local.yaml

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