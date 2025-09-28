# main.py

import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions
from mysql_connector import ReadFromMySql, get_secret

# --- Opções do Pipeline drasticamente simplificadas ---
class MySqlToBigQueryOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        # Parâmetros que sobraram, pois são específicos da execução
        parser.add_argument('--mysql_query', required=True, help='Query a ser executada no MySQL')
        parser.add_argument('--output_table', required=True, help='Tabela de saida no BigQuery (project:dataset.table)')
        
        # O único parâmetro de conexão necessário
        parser.add_argument(
            '--connection_secret',
            required=True,
            help='ID do Secret no Secret Manager contendo o JSON de conexão'
        )

# --- Função principal que constrói e executa o pipeline ---
def run():
    options = PipelineOptions(save_main_session=True, streaming=False)
    custom_options = options.view_as(MySqlToBigQueryOptions)
    gcp_options = options.view_as(GoogleCloudOptions)

    # Busca o dicionário de conexão completo do Secret Manager
    connection_creds = get_secret(custom_options.connection_secret)
    
    with beam.Pipeline(options=options) as p:
        rows = (
            p
            | 'Start' >> beam.Create([None]) 
            # Instancia o DoFn com o dicionário de credenciais
            | 'ReadFromMySQL' >> beam.ParDo(
                ReadFromMySql(
                    connection_info=connection_creds,
                    query=custom_options.mysql_query
                )
            )
        )

        rows | 'WriteToBigQuery' >> beam.io.WriteToBigQuery(
            table=custom_options.output_table,
            schema='SCHEMA_AUTODETECT',
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            custom_gcs_temp_location=gcp_options.temp_location
        )

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()