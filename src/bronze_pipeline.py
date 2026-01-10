import os
import pandas as pd

from utils import read_from_file, export_to_file

def process_bronze_data(
    input_file_path: str,
    layer: str = "bronze",
    output_file_path: str = "output/bronze/"
) -> bool:
    """
    Processa os dados da camada bronze lendo de um arquivo CSV e exportando para outro arquivo CSV.

    Args:
        input_file_path (str): caminho do arquivo de entrada.
        output_file_path (str): caminho do arquivo de saída.

    Returns:
        bool: True se a exportação for bem-sucedida, False caso contrário.
    """
    # Lê os dados do arquivo de entrada
    data = read_from_file("csv", input_file_path)
    file_name = os.path.basename(input_file_path)

    data['_source_file_name'] = file_name
    data['_source_file_date'] = os.path.getmtime(input_file_path)
    data['_processed_date'] = pd.to_datetime('now')

    # Exporta os dados processados para o arquivo de saída
    success = export_to_file(f"{file_name}_bronze", "csv", output_file_path, data)

    return success