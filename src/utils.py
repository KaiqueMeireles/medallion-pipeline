import os
import pandas as pd


def read_from_file(file_type: str, file_path: str) -> pd.DataFrame:
    """Lê os dados de um arquivo CSV e retorna como um DataFrame."""
    if(file_type.lower() != "csv"):
        raise ValueError(
            "Tipo de arquivo ainda não suportado. Utilize apenas arquivos do tipo 'csv'."
            )
    else:
        if os.path.exists(file_path) is False:
            raise FileNotFoundError(f"O arquivo {file_path} não foi encontrado.")
        else:
            return pd.read_csv(file_path)


def ensure_directory(directory_path: str) -> None:
    """Verifica se o diretório especificado existe e o cria caso não exista."""
    if not os.path.exists(directory_path):
        os.makedirs(directory_path)
        return False
    return os.path.isdir(directory_path)


def export_to_file(file_type: str, output_file_path: str, data: pd.DataFrame) -> bool:
    """
    Exporta os dados de um DataFrame para um arquivo no formato especificado.

    Atualmente, apenas arquivos CSV são suportados.
    """
    if file_type.lower() != "csv":
        raise ValueError(
            "Tipo de arquivo ainda não suportado. Utilize apenas arquivos do tipo 'csv'."
        )

    # Garante que o diretório de saída exista, se um diretório foi especificado
    output_dir = os.path.dirname(output_file_path)
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)

    data.to_csv(output_file_path, index=False)
    return True