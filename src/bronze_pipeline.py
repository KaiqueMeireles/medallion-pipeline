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
        True se a exportação for bem-sucedida. Lança uma exceção em caso de falha.
    """
    layer = "bronze"
    data = read_from_file("csv", input_file_path, dtype=str)

    # Monta os metadados
    source_file_folder = os.path.normpath(
        os.path.dirname(input_file_path)
    ).replace("\\", "/")
    source_file_name = os.path.basename(input_file_path)
    source_file_ingest_date = extract_ingest_date(input_file_path)
    source_file_modified_date = pd.to_datetime(
        os.path.getmtime(input_file_path), unit="s", utc=True
    )
    processed_date = pd.Timestamp.now(tz="UTC")

    # Monta o caminho com partição
    if source_file_ingest_date != "unknown":
        partition_folder = f"ingest_date={source_file_ingest_date}"
    else:
        partition_folder = "ingest_date=unknown"

    # Adiciona metadados ao DataFrame
    data["_source_file_folder"] = source_file_folder
    data["_source_file_name"] = source_file_name
    data["_source_file_ingest_date"] = source_file_ingest_date
    data["_source_file_modified_ts"] = source_file_modified_date
    data["_processed_ts"] = processed_date

    # Exporta os dados
    clean_name = os.path.splitext(source_file_name)[0]
    file_output = f"{partition_folder}/{clean_name}_bronze.csv"
    success = export_to_file(file_output, data, layer)

    return success