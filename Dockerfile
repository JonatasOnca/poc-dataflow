# Usamos a imagem base obrigatória do Dataflow para Python 3.11
FROM gcr.io/dataflow-templates-base/python311-template-launcher-base

# Manter como root para simplificar e garantir permissões durante o build e execução.
USER root

# Atualize a lista de pacotes e instale o Java (JRE).
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-17-jre-headless && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Cria a estrutura de diretórios esperada pelo script Python para o driver JDBC.
RUN mkdir -p _drivers
COPY _drivers/mysql-connector-j-8.0.33.jar _drivers/


# Definir o diretório de trabalho
WORKDIR /app

# Assim, se você mudar apenas seu código .py, esta camada não precisará ser reconstruída.
COPY beam_core/_helpers/ beam_core/_helpers/
COPY main.py .
COPY requirements.txt .
COPY setup.py .

# Definir as variáveis de ambiente para o Flex Template
ENV FLEX_TEMPLATE_PYTHON_PY_FILE=/app/main.py
ENV FLEX_TEMPLATE_PYTHON_REQUIREMENTS_FILE=/app/requirements.txt
ENV FLEX_TEMPLATE_PYTHON_SETUP_FILE=/app/setup.py

RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt