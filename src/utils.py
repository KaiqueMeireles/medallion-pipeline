import os
import shutil

import pandas as pd


def extract_ingest_date(file_path: str) -> str:
    # Extrai a data de ingestão (ingest_date=YYYY-MM-DD) do caminho do arquivo.
    # Retorna 'unknown' se padrão não for encontrado.
    try:
        folder_name = os.path.basename(os.path.dirname(file_path))
        # Extrai a parte após "ingest_date="
        if "ingest_date=" in folder_name:
            return folder_name.split("ingest_date=")[-1]
        return "unknown"
    except (AttributeError, IndexError, TypeError):
        return "unknown"


def _ensure_directory(directory_path: str) -> None:
    # Cria diretório se não existir.
    # Utiliza exist_ok=True para evitar erro se já criado.
    if not os.path.exists(directory_path):
        os.makedirs(directory_path)


def clean_directory(directory_path: str) -> None:
    """
    Remove todos os arquivos e subpastas do diretório.

    Possui trava de segurança para impedir deleção fora de 'output/'.

    Args:
        directory_path: Caminho do diretório a limpar.

    Raises:
        ValueError: Se tentar deletar fora da pasta 'output/'.
        Exception: Se houver erro durante a deleção.
    """
    # Impede que delete outra pasta que não esteja dentro de 'output/'
    # Normaliza o caminho e garante que ele esteja dentro de <cwd>/output
    output_root = os.path.abspath(os.path.join(os.getcwd(), "output"))
    target_path = os.path.abspath(directory_path)
    if not (target_path == output_root or
            target_path.startswith(output_root + os.sep)):
        raise ValueError(
            "SEGURANÇA: A função clean_directory só pode apagar pastas "
            "dentro de 'output'. Tentativa de apagar: " + directory_path
        )

    if not os.path.exists(directory_path):
        os.makedirs(directory_path, exist_ok=True)
        return

    # Deleção Recursiva
    try:
        shutil.rmtree(directory_path)
        os.makedirs(directory_path, exist_ok=True)

    except Exception as e:
        print(f"Erro ao limpar o diretório {directory_path}. Motivo: {e}")
        raise e


def read_from_file(file_type: str, file_path: str, **kwargs) -> pd.DataFrame:
    # Lê arquivo CSV e retorna como DataFrame com opções customizáveis.
    # Valida tipo de arquivo e existência antes de ler.
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
    # Exporta DataFrame para output/[layer]/[file_name] com validação.
    # Cria diretório automaticamente se não existir.
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
    # Lista arquivos recursivamente em todas as subpastas.
    # Filtra por extensão especificada e retorna caminhos completos.
    accepted_file_types = ["csv"]

    if not os.path.exists(directory_path):
        raise FileNotFoundError(
            f"O diretório {directory_path} não foi encontrado."
        )

    if file_type.lower() not in accepted_file_types:
        raise ValueError(
            "Tipo de arquivo ainda não suportado. "
            f"Utilize apenas arquivos do tipo "
            f"{', '.join(accepted_file_types)}."
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