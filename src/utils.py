import logging
import os
import shutil

import pandas as pd


def extract_ingest_date(file_path: str) -> str:
    """Extrai a data de ingestão do caminho do arquivo.

    Args:
        file_path: Caminho completo do arquivo

    Returns:
        Data no formato YYYY-MM-DD ou 'unknown' se não conseguir extrair
    """
    try:
        folder_name = os.path.basename(os.path.dirname(file_path))
        # Extrai a parte após "ingest_date="
        if "ingest_date=" in folder_name:
            return folder_name.split("ingest_date=")[-1]
        return "unknown"
    except (AttributeError, IndexError, TypeError):
        return "unknown"


def _ensure_directory(directory_path: str) -> None:
    """Verifica se o diretório especificado existe e o cria caso não exista."""
    if not os.path.exists(directory_path):
        os.makedirs(directory_path)


def clean_directory(directory_path: str) -> None:
    """Remove todos os arquivos e subpastas de um diretório e o recria vazio.

    Possui trava de segurança para impedir deleção de pastas fora do 'output'.
    """
    # Impede que delete outra pasta que não esteja dentro de 'output/'
    # Normaliza o caminho e garante que ele esteja dentro de <cwd>/output
    output_root = os.path.abspath(os.path.join(os.getcwd(), "output"))
    target_path = os.path.abspath(directory_path)
    if not (target_path == output_root or target_path.startswith(output_root + os.sep)):
        raise ValueError(
            f"SEGURANÇA: A função clean_directory só pode apagar pastas "
            f"dentro de 'output'. Tentativa de apagar: {directory_path}"
        )

    if not os.path.exists(directory_path):
        os.makedirs(directory_path, exist_ok=True)
        return

    # Deleção Recursiva
    try:
        shutil.rmtree(directory_path)
        os.makedirs(directory_path, exist_ok=True)

    except Exception as e:
        logging.error(f"Erro ao limpar o diretório {directory_path}. Motivo: {e}")
        raise e


def read_from_file(file_type: str, file_path: str, **kwargs) -> pd.DataFrame:
    """
    Lê os dados de um arquivo CSV e retorna como um DataFrame.

    Args:
        file_type: Tipo do arquivo ('csv')
        file_path: Caminho completo
        **kwargs: Argumentos extras para o pandas (ex: dtype=str, sep=';')
    """
    if file_type.lower() != "csv":
        raise ValueError(
            "Tipo de arquivo ainda não suportado. "
            "Utilize apenas arquivos do tipo 'csv'."
        )

    if not os.path.exists(file_path):
        raise FileNotFoundError(f"O arquivo {file_path} não foi encontrado.")

    return pd.read_csv(file_path, **kwargs)


def export_to_file(
    file_name: str,
    data: pd.DataFrame,
    layer: str,
    file_type: str = "csv",
) -> bool:
    """
    Exporta os dados de um DataFrame para um arquivo no formato especificado.

    Args:
        file_name: Nome do arquivo (ex: 'dados_processados.csv')
        data: DataFrame com os dados a serem exportados
        layer: Camada do pipeline ('bronze', 'silver', 'gold')
        file_type: Tipo do arquivo (atualmente apenas 'csv')

    Returns:
        True se o arquivo foi exportado com sucesso
    """
    if file_type.lower() != "csv":
        raise ValueError(
            "Tipo de arquivo ainda não suportado. "
            "Utilize apenas arquivos do tipo 'csv'."
        )

    if layer.lower() not in ["bronze", "silver", "gold"]:
        raise ValueError(
            "Camada inválida. "
            "Utilize apenas 'bronze', 'silver' ou 'gold'."
        )

    # Constrói o caminho: output/[camada]/arquivo.csv
    output_dir = os.path.join("output", layer.lower())
    output_file_path = os.path.join(output_dir, file_name)

    # Garante que o diretório completo do arquivo existe
    output_file_dir = os.path.dirname(output_file_path)
    _ensure_directory(output_file_dir)

    data.to_csv(output_file_path, index=False)

    return True


def list_files_in_directory(
    directory_path: str,
    file_type: str = "csv",
) -> list:
    """Lista arquivos recursivamente em todas as subpastas.

    Args:
        directory_path: Caminho do diretório raiz
        file_type: Extensão do arquivo (padrão: 'csv')

    Returns:
        Lista com caminhos completos dos arquivos encontrados
    """
    accepted_file_types = ["csv"]

    if not os.path.exists(directory_path):
        raise FileNotFoundError(
            f"O diretório {directory_path} não foi encontrado."
        )

    if file_type.lower() not in accepted_file_types:
        raise ValueError(
            "Tipo de arquivo ainda não suportado. "
            f"Utilize apenas arquivos do tipo {', '.join(accepted_file_types)}."
        )

    files = []

    # os.walk desce em todas as pastas automaticamente
    for root, _, filenames in os.walk(directory_path):
        for filename in filenames:
            # Verifica se termina com .csv
            if filename.lower().endswith(f".{file_type.lower()}"):
                # Monta o caminho completo
                full_path = os.path.join(root, filename)
                files.append(full_path)

    return files