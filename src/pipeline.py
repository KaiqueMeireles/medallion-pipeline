import time

# from .gold_pipeline import process_gold_data

from .bronze_pipeline import process_bronze_data
from .silver_pipeline import process_silver_data
from .utils import clean_directory, list_files_in_directory


def run_pipeline() -> bool:
    """Executa o pipeline completo de processamento de dados."""
    clean_directory("output/")
    # Aguarda 2 segundos para garantir que o sistema de arquivos
    # tenha finalizado a limpeza do diretório antes de processar novos dados
    time.sleep(3)
    
    raw_files = list_files_in_directory("input/")
    for file in raw_files:
        process_bronze_data(file)

    time.sleep(3)

    bronze_files = list_files_in_directory("output/bronze/")
    for file in bronze_files:
        process_silver_data(file)
    
    # silver_files = list_files_in_directory("output/silver/")
    # for file in silver_files:
        # process_gold_data(file)

    return True