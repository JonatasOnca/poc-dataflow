# --- Etapa 1: Preparar o ambiente Java ---
FROM python:3.9-slim AS java-builder

RUN apt-get update && \
    apt-get install -y default-jre-headless && \
    apt-get clean


# --- Etapa 2: Construir a imagem final ---
# A imagem final OBRIGATORIAMENTE usa a base do Google
FROM gcr.io/dataflow-templates-base/python39-template-launcher-base

USER root

# Copiar a instalação do Java da Etapa 1
COPY --from=java-builder /usr/lib/jvm/ /usr/lib/jvm/

# Definir as variáveis de ambiente para o Java
ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH=$JAVA_HOME/bin:$PATH

# Definir as variáveis de ambiente para o Flex Template
ENV FLEX_TEMPLATE_PYTHON_PY_FILE=/app/main.py
ENV FLEX_TEMPLATE_PYTHON_REQUIREMENTS_FILE=/app/requirements.txt
ENV FLEX_TEMPLATE_PYTHON_SETUP_FILE=/app/setup.py

# Definir o diretório de trabalho
WORKDIR /app

# Copiar e instalar as dependências
COPY requirements.txt setup.py ./
RUN pip install --no-cache-dir -r requirements.txt

# Copiar o resto do código do projeto
COPY . .