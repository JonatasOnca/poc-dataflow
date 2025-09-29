# transforms/data_validation.py

import apache_beam as beam
import traceback

class OutputTags:
    SUCCESS = 'success'
    FAILURE = 'failure'

class MapAndValidate(beam.DoFn):
    """
    Uma DoFn que aplica uma função de mapeamento e captura exceções.
    - Se o mapeamento for bem-sucedido, emite para a saída principal (success).
    - Se ocorrer um erro, emite o dado original e o erro para uma saída secundária (failure).
    """
    def __init__(self, map_function):
        self.map_function = map_function

    def process(self, element):
        try:
            mapped_element = self.map_function(element)
            yield mapped_element
        except Exception as e:
            error_record = {
                "original_data": str(element),
                "error_message": str(e),
                "traceback": traceback.format_exc()
            }
            yield beam.pvalue.TaggedOutput(OutputTags.FAILURE, error_record)