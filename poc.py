# Copyright 2025 TecOnca Data Solutions.


import logging
import json
from apache_beam.io.filesystems import FileSystems

def load_schema(bucket_schema: str) -> dict:
    """Lê um arquivo .json do GCS."""
    try:
        with FileSystems.open(bucket_schema) as file:
                print(file)
                return json.load(file)
    except Exception as e:
        logging.warning(f"Problemas ao abrir o Bucket '{bucket_schema} de Schemas': {e}")


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    load_schema('gs://abemcomum-saev-prod/mysql-to-bq-dataflow-multiple-tables/schemas/turma_professor.json')
    load_schema('gs://abemcomum-saev-prod/mysql-to-bq-dataflow-multiple-tables/schemas/turma_aluno.json')
    