# Usar a imagem base oficial do Google para Flex Templates com Python 3.9
# Esta imagem já contém o SDK do Apache Beam e o mecanismo de lançamento de templates.
FROM gcr.io/dataflow-templates-base/python39-template-launcher-base

# Definir variáveis de ambiente que o launcher na imagem base usará para
# encontrar os arquivos do seu projeto.
ENV FLEX_TEMPLATE_PYTHON_PY_FILE=/app/main.py
ENV FLEX_TEMPLATE_PYTHON_REQUIREMENTS_FILE=/app/requirements.txt
ENV FLEX_TEMPLATE_PYTHON_SETUP_FILE=/app/setup.py

# Definir diretório de trabalho
WORKDIR /app

# Copiar os arquivos de dependência primeiro para aproveitar o cache do Docker
COPY requirements.txt setup.py ./

# Instalar as dependências adicionais do projeto.
# A imagem base já contém o apache-beam, mas o pip irá lidar com isso.
RUN pip install --no-cache-dir -r requirements.txt

# Copiar todo o código do projeto para a imagem
COPY . .