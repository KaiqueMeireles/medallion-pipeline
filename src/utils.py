import os
import pandas as pd


def _ensure_directory(
    directory_path: str
) -> None:
    """Verifica se o diretório especificado existe e o cria caso não exista."""
    if not os.path.exists(directory_path):
        os.makedirs(directory_path)
        return False
    return os.path.isdir(directory_path)


def read_from_file(
    file_type: str,
    file_path: str
) -> pd.DataFrame:
    """Lê os dados de um arquivo CSV e retorna como um DataFrame."""
    if(file_type.lower() != "csv"):
        raise ValueError(
            "Tipo de arquivo ainda não suportado. "
            "Utilize apenas arquivos do tipo 'csv'."
            )
    else:
        if os.path.exists(file_path) is False:
            raise FileNotFoundError(
                f"O arquivo {file_path} não foi encontrado."
                )
        else:
            return pd.read_csv(file_path)


def export_to_file(
    file_name: str,
    data: pd.DataFrame,
    layer: str,
    output_folder: str = "output/",
    file_type: str = "csv"
) -> bool:
    """
    Exporta os dados de um DataFrame para um arquivo no formato especificado.
    
    Args:
        file_name: Nome do arquivo (ex: 'dados_processados.csv')
        data: DataFrame com os dados a serem exportados
        layer: Camada do pipeline ('bronze', 'silver' e 'gold')
        file_type: Tipo do arquivo (atualmente apenas 'csv')
    
    Returns:
        True se o arquivo foi exportado com sucesso
    """
    if file_type.lower() != "csv":
        raise ValueError(
            "Tipo de arquivo ainda não suportado. "
            "Utilize apenas arquivos do tipo 'csv'."
        )
    
    if(layer.lower() not in ["bronze", "silver", "gold"]):
        raise ValueError(
            "Camada inválida. "
            "Utilize apenas 'bronze', 'silver' ou 'gold'."
        )

    # Constrói o caminho: output/[camada]/arquivo.csv
    output_dir = os.path.join(output_folder, layer.lower())
    _ensure_directory(output_dir)
    
    output_file_path = os.path.join(output_dir, file_name)
    data.to_csv(output_file_path, index=False)
    
    return True