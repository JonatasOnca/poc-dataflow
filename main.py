import argparse
import yaml

import logging
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.filesystems import FileSystems
from beam_core import pipeline

def run_main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--config_file',
        required=True,
        help='Caminho para o arquivo de configuração YAML.'
    )
    known_args, pipeline_args = parser.parse_known_args()

    with FileSystems.open(known_args.config_file) as f:
        app_config = yaml.safe_load(f)

    pipeline_options = PipelineOptions(
        pipeline_args,
        runner=app_config['dataflow']['parameters']['runner'],
        project=app_config['gcp']['project_id'],
        region=app_config['gcp']['region'],
        staging_location=app_config['dataflow']['parameters']['staging_location'],
        temp_location=app_config['dataflow']['parameters']['temp_location'],
        job_name=app_config['dataflow']['job_name'],
        setup_file='./setup.py'
    )

    logging.info("Iniciando o pipeline com a seguinte configuração: %s", app_config)
    pipeline.run(app_config=app_config, pipeline_options=pipeline_options)

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run_main()