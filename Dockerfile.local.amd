# 1. Usar a imagem base oficial do Google como ponto de partida.
# Esta imagem já contém o ambiente necessário para lançar Flex Templates.
FROM gcr.io/dataflow-templates-base/python311-template-launcher-base

# 2. Mudar para o usuário 'root' para ter permissão de instalar pacotes do sistema.
USER root

# 3. Em um único passo (para otimizar o tamanho da imagem), atualizar os pacotes,
#    instalar o Java (JRE) e o `wget` (para baixar drivers/jars), e depois limpar o cache.
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        default-jre-headless \
        wget \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# 4. Criar um diretório para os drivers e baixar o conector JDBC do MySQL.
#    É uma boa prática fixar a versão do driver para garantir builds consistentes.
RUN mkdir -p /app/drivers && \
    wget -O /app/drivers/mysql-connector-j-8.0.33.jar \
    https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/8.0.33/mysql-connector-j-8.0.33.jar && \
    wget -O /app/drivers/postgresql-42.2.16.jar \
    https://repo.maven.apache.org/maven2/org/postgresql/postgresql/42.2.16/postgresql-42.2.16.jar


# 5. NOVO PASSO: Baixar o JAR de expansão do Beam que estava falhando (403 Forbidden).
# Isso garante que ele esteja disponível localmente para o Dataflow Launcher.
RUN mkdir -p /app/beam_jars && \
    wget -O /app/beam_jars/beam-sdks-java-extensions-schemaio-expansion-service-2.68.0.jar \
    https://repo.maven.apache.org/maven2/org/apache/beam/beam-sdks-java-extensions-schemaio-expansion-service/2.68.0/beam-sdks-java-extensions-schemaio-expansion-service-2.68.0.jar

# 6. Definir o diretório de trabalho padrão para os próximos comandos.
WORKDIR /app

# 7. Definir as variáveis de ambiente padrão que o serviço de Flex Templates usa
#    para encontrar os arquivos do seu pipeline Python.
ENV FLEX_TEMPLATE_PYTHON_PY_FILE=/app/main.py
ENV FLEX_TEMPLATE_PYTHON_REQUIREMENTS_FILE=/app/requirements.txt
# A linha abaixo é opcional se você não usa um arquivo setup.py
ENV FLEX_TEMPLATE_PYTHON_SETUP_FILE=/app/setup.py

# 8. Otimização de cache do Docker: Copie e instale as dependências primeiro.
#    Isso evita reinstalar tudo a cada pequena mudança no código-fonte.
COPY requirements.txt .
# Se você tiver um setup.py, descomente a linha abaixo
COPY setup.py .
# Se você tiver um main.py, descomente a linha abaixo
COPY main.py .
RUN pip install --no-cache-dir -r requirements.txt

# 9. Finalmente, copie o resto do código da sua aplicação para o contêiner.
COPY . .