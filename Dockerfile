# Usar a imagem base oficial do Apache Beam com Python 3.9
FROM gcr.io/dataflow-templates-base/python39-template-launcher-base

# Define o diretório de trabalho dentro do container
WORKDIR /dataflow/template

# Copia os arquivos de dependências
COPY requirements.txt .

# Instala as dependências
RUN pip install --no-cache-dir -r requirements.txt

# Copia o código do pipeline
COPY main.py .

# Define o SDK do Apache Beam para o template
ENV FLEX_TEMPLATE_PYTHON_PY_FILE="/dataflow/template/main.py"