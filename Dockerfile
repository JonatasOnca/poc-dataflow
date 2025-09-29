# --- Etapa 1: Preparar o ambiente Java ---
# MUDOU: Atualizado para a base Python 3.11
FROM python:3.11-slim AS java-builder

RUN apt-get update && \
    apt-get install -y default-jre-headless && \
    apt-get clean


# --- Etapa 2: Construir a imagem final ---
# MUDOU: Atualizado para a imagem base de templates do Dataflow para Python 3.11
FROM gcr.io/dataflow-templates-base/python311-template-launcher-base

# Usar ARG para facilitar a atualização da versão do driver
ARG MYSQL_DRIVER_VERSION=8.0.33

USER root

# Copiar a instalação do Java da Etapa 1
COPY --from=java-builder /usr/lib/jvm/ /usr/lib/jvm/

# Definir as variáveis de ambiente para o Java
ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH=$JAVA_HOME/bin:$PATH

# --- Instalar o driver JDBC do MySQL (sem alterações) ---
# 1. Instalar 'wget' para baixar o driver
RUN apt-get update && apt-get install -y wget && apt-get clean

# 2. Criar um diretório para os drivers, baixar e colocar o JAR lá
RUN mkdir -p /app/drivers && \
    wget https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/${MYSQL_DRIVER_VERSION}/mysql-connector-j-${MYSQL_DRIVER_VERSION}.jar -O /app/drivers/mysql-connector-j.jar

# 3. Adicionar o driver ao classpath do Beam
ENV BEAM_JAVA_CLASSPATH="/app/drivers/mysql-connector-j.jar"
# --- FIM DA SEÇÃO DO DRIVER ---

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

# Voltar para o usuário padrão (boa prática)
USER dataflow