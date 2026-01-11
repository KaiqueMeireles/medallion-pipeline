import time

from .bronze_pipeline import process_bronze_data
from .gold_pipeline import process_gold_data
from .silver_pipeline import process_silver_data
from .utils import clean_directory, list_files_in_directory


def run_pipeline() -> bool:
    """Executa o pipeline completo de processamento de dados."""
    clean_directory("output/")
    time.sleep(2)
    files = list_files_in_directory("input/")

    for file in files:
        process_bronze_data(file)
        # process_silver_data(file)
        # process_gold_data(file)

    return True