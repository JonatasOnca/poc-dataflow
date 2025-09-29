# --- Etapa 1: Preparar o ambiente Java ---
# Usamos a base Python 3.11 e a nomeamos como 'java-builder'
FROM python:3.11-slim AS java-builder

# Otimização: Combinamos todos os comandos apt em uma única camada para reduzir o tamanho da imagem.
# --no-install-recommends evita instalar pacotes desnecessários.
# rm -rf /var/lib/apt/lists/* limpa o cache do apt.
RUN apt-get update && \
    apt-get install -y --no-install-recommends default-jre-headless && \
    rm -rf /var/lib/apt/lists/*


# --- Etapa 2: Construir a imagem final ---
# Usamos a imagem base obrigatória do Dataflow para Python 3.11
FROM gcr.io/dataflow-templates-base/python311-template-launcher-base:latest

# Manter como root para simplificar e garantir permissões durante o build e execução.
USER root

# Copiar a instalação do Java da Etapa 1
COPY --from=java-builder /usr/lib/jvm/ /usr/lib/jvm/

# Definir as variáveis de ambiente para o Java
ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH=$JAVA_HOME/bin:$PATH

# --- NOVO: Adicionar Driver JDBC do MySQL ---
# Criamos um diretório para as bibliotecas e usamos ADD para baixar o JAR do MySQL diretamente do repositório Maven.
# O uso de ADD com URL é uma forma concisa de baixar e copiar o arquivo em um único passo.
RUN mkdir -p /app/libs
ADD https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/9.4.0/mysql-connector-j-9.4.0.jar /app/libs/
# COPY drivers/mysql-connector-j-9.4.0.jar /app/libs/mysql-connector-j-9.4.0.jar

RUN ls -l /app/libs/


# Definir as variáveis de ambiente para o Flex Template
ENV FLEX_TEMPLATE_PYTHON_PY_FILE=/app/main.py
ENV FLEX_TEMPLATE_PYTHON_REQUIREMENTS_FILE=/app/requirements.txt
ENV FLEX_TEMPLATE_PYTHON_SETUP_FILE=/app/setup.py

# Definir o diretório de trabalho
WORKDIR /app

# Otimização de cache: Copiar e instalar dependências do Python ANTES do código-fonte.
# Assim, se você mudar apenas seu código .py, esta camada não precisará ser reconstruída.
COPY requirements.txt setup.py ./
RUN pip install --no-cache-dir -r requirements.txt

# Copiar o resto do código do projeto
COPY . .