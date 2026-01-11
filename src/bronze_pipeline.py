import os

import pandas as pd

from .utils import export_to_file, read_from_file, extract_ingest_date

def process_bronze_data(input_file_path: str) -> bool:
    """
    Processa os dados da camada bronze lendo de um arquivo CSV
    e exportando para outro arquivo CSV.

    Args:
        input_file_path: Caminho do arquivo de entrada.

    Returns:
        True se a exportação for bem-sucedida, False caso contrário.
    """
    layer = "bronze"
    data = read_from_file("csv", input_file_path)
    file_name = os.path.basename(input_file_path)
    ingest_date = extract_ingest_date(input_file_path)

    # Adiciona metadados
    data["_source_file_folder"] = os.path.normpath(
        os.path.dirname(input_file_path)
    )
    data["_source_file_name"] = file_name
    data["_source_file_ingest_date"] = ingest_date
    data["_source_file_modified_date"] = pd.to_datetime(
        os.path.getmtime(input_file_path), unit="s"
    )
    data["_processed_date"] = pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S")

    # Monta o caminho com partição
    if ingest_date != "unknown":
        partition_folder = f"ingest_date={ingest_date}"
    else:
        partition_folder = "ingest_date=unknown"

    clean_name = file_name.replace(".csv", "")
    file_output = f"{partition_folder}/{clean_name}_bronze.csv"
    success = export_to_file(file_output, data, layer)

    return success