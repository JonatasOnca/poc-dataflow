# Copyright 2025 TecOnca Data Solutions.


import logging
import json
import yaml

from jinja2 import Template
from apache_beam.io.filesystems import FileSystems


def load_yaml(bucket_yaml: str) -> str:
    """Lê um arquivo .yaml do GCS."""
    # --- Etapa 1: Ler o arquivo como texto bruto ---
    try:
        with FileSystems.open(bucket_yaml) as file:
            # Lê todo o conteúdo do arquivo para uma string
            # 1. file.read() retorna um objeto de bytes
            yaml_as_bytes = file.read()

            # 2. Precisamos decodificar os bytes para uma string (usando utf-8)
            # para que o Jinja2 e o PyYAML possam processá-la.
            yaml_as_text = yaml_as_bytes.decode("utf-8")

    except FileNotFoundError as e:
        logging.error(f"Arquivo YAML não encontrado: {bucket_yaml}")
        raise e

    # --- Etapa 2: Carregar o YAML uma vez para obter as variáveis ---
    # Usamos safe_load para segurança
    try:
        # Este dicionário conterá o bloco 'variables' que precisamos
        initial_data = yaml.safe_load(yaml_as_text)
    except yaml.YAMLError as e:
        logging.error(f"Erro ao analisar o YAML inicial: {e}")
        raise

    try:
        # --- Etapa 3: Renderizar o texto usando Jinja2 ---
        # Criamos um objeto de template Jinja2 a partir do nosso texto
        template = Template(yaml_as_text)

        # Renderizamos o template, passando os dados que carregamos
        # para que o Jinja saiba o que substituir.
        rendered_yaml_text = template.render(initial_data)
    except yaml.YAMLError as e:
        logging.error(f"Erro ao renderizar YAML via Jinja2: {e}")
        raise
    # --- Etapa 4: Carregar o YAML final (já processado) ---
    # Agora que os placeholders {{ }} foram substituídos,
    # podemos carregar o YAML como um dicionário Python limpo.
    return yaml.safe_load(rendered_yaml_text)


def load_query(bucket_query: str) -> str:
    """Lê um arquivo .sql do GCS."""
    try:
        with FileSystems.open(bucket_query) as file:
            # 1. file.read() retorna um objeto de bytes
            yaml_as_bytes = file.read()

        # 2. Precisamos decodificar os bytes para uma string (usando utf-8)
        # para que o Jinja2 e o PyYAML possam processá-la.
        yaml_as_text = yaml_as_bytes.decode("utf-8")
        return yaml_as_text
    except Exception as e:
        logging.warning(f"Problemas ao abrir o Bucket '{bucket_query} de Querys': {e}")


def load_schema(bucket_schema: str) -> dict:
    """Lê um arquivo .json do GCS."""
    try:
        with FileSystems.open(bucket_schema) as file:
            return json.load(file)
    except Exception as e:
        logging.warning(f"Problemas ao abrir o Bucket '{bucket_schema} de Schemas': {e}")
