# Define a imagem base para o template do Dataflow.
FROM gcr.io/dataflow-templates-base/python311-template-launcher-base

# Mude para o usuário root para poder instalar pacotes de sistema.
USER root

# Atualize a lista de pacotes e instale o Java (JRE).
# --no-install-recommends mantém a imagem menor.
# Limpar o cache do apt na mesma camada RUN também reduz o tamanho final da imagem.
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-11-jre-headless && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Define o diretório de trabalho dentro do contêiner.
WORKDIR /dataflow/template

# Atualiza o pip para garantir que a versão mais recente seja usada.
RUN pip install --upgrade pip

# Copia o arquivo de dependências para o diretório de trabalho.
COPY requirements.txt .

# Instala as dependências Python especificadas no requirements.txt.
RUN pip install --no-cache-dir -r requirements.txt

# Cria a estrutura de diretórios esperada pelo script Python para o driver JDBC.
RUN mkdir -p _drivers

# Copia o script principal da aplicação e o driver JDBC para o contêiner.
COPY main.py .
COPY _drivers/mysql-connector-j-8.0.33.jar _drivers/

# Mude de volta para o usuário não-root 'dataflow' por segurança.
# A imagem base original usa 'dataflow' como usuário padrão.
USER dataflow

# Define a variável de ambiente exigida pelo lançador do Flex Template.
ENV FLEX_TEMPLATE_PYTHON_PY_FILE="/dataflow/template/main.py"