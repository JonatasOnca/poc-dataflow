# ==============================================================================
# Makefile para o Projeto Dataflow Flex Template (Python)
# ==============================================================================

# --- Configuração do Projeto ---
# Altere estas variáveis para corresponderem ao seu ambiente GCP.
# O PROJECT_ID é pego dinamicamente do seu gcloud config.
PROJECT_ID ?= $(shell gcloud config get-value project)
REGION ?= us-central1
BUCKET_NAME ?= "abemcomum-saev-prod" # ⚠️ IMPORTANTE: Altere para o seu bucket!
REPO_NAME ?= "dataflow-templates" # Repositório do Artifact Registry
IMAGE_NAME ?= "mysql-to-bq-template"
TEMPLATE_NAME ?= "mysql_to_bq_flex.json"

CONNECTION_SECRET ?= "dataflow-mysql-credentials"
MYSQL_QUERY ?= "SELECT * FROM Saev.genero "
OUTPUT_TABLE ?= "genero"


# --- Variáveis Automáticas (não precisam ser alteradas) ---
SHELL := /bin/bash
VENV_DIR := .venv
PYTHON := $(VENV_DIR)/bin/python
PIP := $(VENV_DIR)/bin/pip
IMAGE_TAG := latest
IMAGE_PATH := $(REGION)-docker.pkg.dev/$(PROJECT_ID)/$(REPO_NAME)/$(IMAGE_NAME):$(IMAGE_TAG)
TEMPLATE_GCS_PATH := gs://$(BUCKET_NAME)/templates/$(TEMPLATE_NAME)
JOB_NAME_PREFIX := mysql-to-bq-job

# Define o alvo padrão que será executado ao rodar `make` sem argumentos.
.DEFAULT_GOAL := help

# Declara alvos que não são arquivos.
.PHONY: help install lint build run clean

# ==============================================================================
# Alvos (Comandos)
# ==============================================================================

help: ## 💬 Mostra esta mensagem de ajuda
	@echo "Makefile para o projeto de ingestão MySQL -> BigQuery"
	@echo "----------------------------------------------------"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-20s %s\n", $$1, $$2}'

setup: ## Cria o repositório no Artifact Registry (executar apenas uma vez)
	@echo "--> Configurando o repositório no Artifact Registry: $(REPO_NAME)"
	@gcloud artifacts repositories create $(REPO_NAME) \
		--repository-format=docker \
		--location=$(REGION) \
		--description="Repositório para PoC de Flex Templates" || echo "Repositório já existe."
	@echo "--> Configurando autenticação do Docker..."
	@gcloud auth configure-docker $(REGION)-docker.pkg.dev

install: ## 📦 Cria um ambiente virtual e instala as dependências
	@echo "--> Configurando o ambiente virtual em [$(VENV_DIR)]..."
	@if [ ! -d "$(VENV_DIR)" ]; then \
		python3 -m venv $(VENV_DIR); \
	fi
	@echo "--> Instalando dependências do requirements.txt..."
	@$(PIP) install --upgrade pip
	@$(PIP) install -r requirements.txt
	@echo "✅ Ambiente pronto!"

lint: ## 💅 Roda um linter para verificar a qualidade do código
	@echo "--> Verificando o estilo do código com PyMySQL..."
	@$(VENV_DIR)/bin/pymysql .
	@echo "✅ Verificação de estilo concluída."

build: ## 🏗️ Constrói a imagem Docker e cria o Flex Template no GCS
	@echo "--> Construindo o Flex Template..."
	@echo "    Imagem: $(IMAGE_PATH)"
	@echo "    Template GCS: $(TEMPLATE_GCS_PATH)"
	@gcloud dataflow flex-template build $(TEMPLATE_GCS_PATH) \
		--image "$(IMAGE_PATH)" \
		--sdk-language "PYTHON" \
		--metadata-file "metadata.json"
	@echo "✅ Template construído com sucesso!"

run: ## 🚀 Executa o job no Dataflow (requer passagem de parâmetros)
	@echo "--> Executando o job do Dataflow a partir do template..."
	@echo "    ⚠️ Certifique-se de passar os parâmetros na linha de comando."
	@echo "    Exemplo: make run CONNECTION_SECRET=projects/.../secrets/..."
	@gcloud dataflow flex-template run "$(JOB_NAME_PREFIX)-$$(date +%Y%m%d-%H%M%S)" \
		--template-file-gcs-location "$(TEMPLATE_GCS_PATH)" \
		--region "$(REGION)" \
		--parameters connection_secret="$(CONNECTION_SECRET)" \
		--parameters mysql_query="$(MYSQL_QUERY)" \
		--parameters output_table="$(OUTPUT_TABLE)"
	@echo "✅ Job iniciado!"

clean: ## 🧹 Remove o ambiente virtual e arquivos temporários
	@echo "--> Limpando o projeto..."
	@rm -rf $(VENV_DIR)
	@rm -rf `find . -name __pycache__`
	@rm -f `find . -type f -name '*.py[co]' `
	@echo "✅ Limpeza concluída."