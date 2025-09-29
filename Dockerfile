# --- Etapa 1: Preparar o ambiente Java ---
# Corrigido: 'AS' em maiúsculas para consistência com 'FROM'.
FROM python:3.9-slim AS java-builder

RUN apt-get update && \
    apt-get install -y default-jre-headless && \
    apt-get clean


# --- Etapa 2: Construir a imagem final ---
FROM gcr.io/dataflow-templates-base/python39-template-launcher-base

# Mudar para usuário root para poder copiar arquivos e instalar dependências.
# Manteremos o usuário root para simplificar e garantir a compatibilidade.
USER root

# Copiar a instalação completa do Java da etapa 'java-builder'.
COPY --from=java-builder /usr/lib/jvm/ /usr/lib/jvm/
COPY --from=java-builder /usr/share/ca-certificates /usr/share/ca-certificates

# Definir as variáveis de ambiente para o Java.
# Corrigido: Usando o formato ENV key=value para evitar warnings.
ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH=$JAVA_HOME/bin:$PATH

# REMOVIDO: A linha "USER beam" foi removida pois o usuário não existe na imagem base.

# Definir as variáveis de ambiente obrigatórias para o Flex Template.
# Corrigido: Usando o formato ENV key=value para evitar warnings.
ENV FLEX_TEMPLATE_PYTHON_PY_FILE=/app/main.py
ENV FLEX_TEMPLATE_PYTHON_REQUIREMENTS_FILE=/app/requirements.txt
ENV FLEX_TEMPLATE_PYTHON_SETUP_FILE=/app/setup.py

# Definir o diretório de trabalho.
WORKDIR /app

# Copiar os arquivos de dependência do Python.
COPY requirements.txt setup.py ./

# Instalar as dependências (agora como root, o que vai funcionar).
RUN pip install --no-cache-dir -r requirements.txt

# Copiar o resto do código do projeto.
COPY . .