# Use the official Google base image for Dataflow templates
FROM gcr.io/dataflow-templates-base/python311-template-launcher-base

# Set the working directory
WORKDIR /dataflow/template

# ADICIONADO: Instala o Java Runtime, que é essencial para o ReadFromJdbc
RUN apt-get update && \
    apt-get install -y openjdk-17-jre-headless && \
    rm -rf /var/lib/apt/lists/*

# Copy and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy your application code and the JDBC driver
COPY main.py .
COPY _drivers/ _drivers/

# Set the entrypoint for the Flex Template
ENV FLEX_TEMPLATE_PYTHON_PY_FILE="/dataflow/template/main.py"