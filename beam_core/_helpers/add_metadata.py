import logging
import hashlib
import json

import apache_beam as beam
from datetime import datetime, timezone

class AddMetadataDoFn(beam.DoFn):
    """
    Um DoFn para adicionar metadados a cada registro.
    - _ingestion_timestamp: Horário UTC da ingestão.
    - _source_name: Nome da tabela de origem.
    - _job_id: ID único da execução da pipeline.
    - _row_hash: Hash do conteúdo da linha.
    """
    def __init__(self, source_name, job_id):
        self.source_name = source_name
        self.job_id = job_id

    def process(self, element):
        # Converte o elemento para um dicionário mutável, se necessário
        if not isinstance(element, dict):
            # Tenta converter de Row para dict, comum em conectores JDBC
            try:
                mutable_element = element._asdict()
            except AttributeError:
                logging.error(f"Não foi possível converter o elemento para dicionário: {element}")
                return
        else:
            mutable_element = dict(element)

        formato_bq = '%Y-%m-%d %H:%M:%S'
        # Adiciona os metadados
        mutable_element['_ingestion_timestamp'] = datetime.now(timezone.utc).strftime(formato_bq)
        mutable_element['_source_name'] = self.source_name
        mutable_element['_job_id'] = self.job_id

        # Cria uma representação estável do dicionário para o hash
        # Exclui campos de metadados pré-existentes do cálculo do hash
        hashable_element = {
            k: v for k, v in mutable_element.items()
            if not k.startswith('_')
        }
        
        row_str = json.dumps(hashable_element, sort_keys=True, default=str)
        mutable_element['_row_hash'] = hashlib.sha256(row_str.encode('utf-8')).hexdigest()

        yield mutable_element