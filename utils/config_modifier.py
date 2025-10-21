import sys

import yaml


def main():
    """
    Lê um arquivo de configuração YAML, altera o runner para DirectRunner
    e salva em um novo arquivo.
    """
    if len(sys.argv) != 3:
        print("Uso: python config_modifier.py <arquivo_de_entrada> <arquivo_de_saida>")
        sys.exit(1)

    input_file = sys.argv[1]
    output_file = sys.argv[2]

    try:
        with open(input_file) as f:
            config = yaml.safe_load(f)

        # Navega na estrutura do YAML e altera o valor desejado
        if (
            "dataflow" in config
            and "parameters" in config["dataflow"]
            and "runner" in config["dataflow"]["parameters"]
        ):
            config["dataflow"]["parameters"]["runner"] = "DirectRunner"
            config["dataflow"][
                "job_name"
            ] = "test-mysql-to-bq-multiple-ingestion-local-job"  # Altera o nome do job para clareza
            print(f"Runner alterado para 'DirectRunner' no arquivo '{output_file}'.")
        else:
            print("A chave 'runner' não foi encontrada na estrutura esperada do config.yaml.")
            sys.exit(1)

        if (
            "source_db" in config
            and "secret_version" in config["source_db"]
            and "latest" in config["source_db"]["secret_version"]
        ):
            config["source_db"]["secret_version"] = 1

        with open(output_file, "w") as f:
            yaml.dump(config, f, default_flow_style=False, sort_keys=False)

    except FileNotFoundError:
        print(f"Erro: O arquivo de entrada '{input_file}' não foi encontrado.")
        sys.exit(1)
    except Exception as e:
        print(f"Ocorreu um erro: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
