import time

from .bronze_pipeline import process_bronze_data
from .utils import clean_directory, list_files_in_directory


def run_pipeline() -> bool:
    """Executa o pipeline completo de processamento de dados."""
    clean_directory("output/")
    # Aguarda 2 segundos para garantir que o sistema de arquivos
    # tenha finalizado a limpeza do diretório antes de processar novos dados
    time.sleep(2)
    files = list_files_in_directory("input/")

    # A camada bronze processa cada arquivo individualmente, por isso
    # `process_bronze_data` recebe o caminho do arquivo como parâmetro.
    for file in files:
        process_bronze_data(file)

    # As camadas silver e gold operam sobre os dados já processados na camada
    # bronze e, por isso, não precisam de um caminho de arquivo como parâmetro.
    # Para habilitá-las, descomente as linhas abaixo:
    # process_silver_data()
    # process_gold_data()

    return True