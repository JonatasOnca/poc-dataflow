# 1. Usar a imagem base oficial do Google como ponto de partida.
# Esta imagem já contém o ambiente necessário para lançar Flex Templates.
FROM gcr.io/dataflow-templates-base/python311-template-launcher-base

# 2. Mudar para o usuário 'root' para ter permissão de instalar pacotes do sistema.
USER root

# 3. Em um único passo, instalar o Java (JRE) e o `wget`, e depois limpar o cache.
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        default-jre-headless \
        wget \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# 4. Criar os diretórios de cache do Beam e de drivers da aplicação.
RUN mkdir -p /root/.apache_beam/cache/jars && \
    mkdir -p /app/drivers

# 5. Baixar o driver PostgreSQL (que o Beam insiste em buscar) e colocá-lo no CACHE.
# Isso resolve o erro 'postgresql-42.2.16.jar: HTTP Error 403'.
RUN wget -O /root/.apache_beam/cache/jars/postgresql-42.2.16.jar \
    https://repo.maven.apache.org/maven2/org/postgresql/postgresql/42.2.16/postgresql-42.2.16.jar

# 6. Baixar o JAR de Expansão do Beam (o outro que falhou) e colocá-lo no CACHE.
# Isso resolve o erro 'beam-sdks-java-extensions-schemaio-expansion-service-2.68.0.jar: HTTP Error 403'.
RUN wget -O /root/.apache_beam/cache/jars/beam-sdks-java-extensions-schemaio-expansion-service-2.68.0.jar \
    https://repo.maven.apache.org/maven2/org/apache/beam/beam-sdks-java-extensions-schemaio-expansion-service/2.68.0/beam-sdks-java-extensions-schemaio-expansion-service-2.68.0.jar

# 7. Baixar o conector JDBC do MySQL e colocá-lo no diretório de drivers da aplicação.
RUN wget -O /app/drivers/mysql-connector-j-8.0.33.jar \
    https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/8.0.33/mysql-connector-j-8.0.33.jar

# 8. Definir o diretório de trabalho padrão para os próximos comandos.
WORKDIR /app

# 9. Definir as variáveis de ambiente padrão.
ENV FLEX_TEMPLATE_PYTHON_PY_FILE=/app/main.py
ENV FLEX_TEMPLATE_PYTHON_REQUIREMENTS_FILE=/app/requirements.txt
ENV FLEX_TEMPLATE_PYTHON_SETUP_FILE=/app/setup.py

# 10. Otimização de cache do Docker: Copie e instale as dependências primeiro.
COPY requirements.txt .
COPY setup.py .
COPY main.py .
RUN pip install --no-cache-dir -r requirements.txt

# 11. Finalmente, copie o resto do código da sua aplicação para o contêiner.
COPY . .