import time

from .gold_pipeline import process_gold_data
from .bronze_pipeline import process_bronze_data
from .silver_pipeline import process_silver_data
from .utils import clean_directory, list_files_in_directory


def run_pipeline() -> bool:
    # Executa pipeline completo (bronze → silver → gold) com delays entre etapas.
    # Retorna True se todas as etapas executadas com sucesso.
    clean_directory("output/")
    # Aguarda 3 segundos entre cada etapa para garantir
    # que o sistema de arquivos tenha finalizado a limpeza 
    # do diretório antes de processar novos dados
    time.sleep(3)
    
    raw_files = list_files_in_directory("input/")
    for file in raw_files:
        process_bronze_data(file)
    print(f"Camada bronze processada.")
    time.sleep(3)

    bronze_files = list_files_in_directory("output/bronze/")
    for file in bronze_files:
        process_silver_data(file)
    print(f"Camada silver processada.")
    time.sleep(3)
    
    silver_files = list_files_in_directory("output/silver/")
    process_gold_data(silver_files)
    print(f"Camada gold processada.")
    return True