import os
import pandas as pd
import unicodedata

from typing import Callable

from .utils import export_to_file, read_from_file, extract_ingest_date


def _mark_empty_strings(df: pd.DataFrame) -> pd.DataFrame:
    """Substitui strings vazias por NaN em todo o DataFrame."""
    return df.replace("", pd.NA)


def _drop_empty_ids(df: pd.DataFrame, id_column: str) -> pd.DataFrame:
    """Remove linhas onde a coluna de ID está vazia ou nula."""
    return df.dropna(subset=[id_column]).copy()


def _clean_state_code(state: str) -> str:
    """Limpa e valida o código do estado (UF)."""
    if pd.isna(state) or state == "":
        return None
    
    state = str(state).strip().upper()
    
    # Lista oficial das 27 UFs do Brasil
    valid_states = {
        'AC', 'AL', 'AP', 'AM', 'BA', 'CE', 'DF', 'ES', 'GO', 'MA',
        'MT', 'MS', 'MG', 'PA', 'PB', 'PR', 'PE', 'PI', 'RJ', 'RN',
        'RS', 'RO', 'RR', 'SC', 'SP', 'SE', 'TO'
    }
    
    if state in valid_states:
        return state
        
    return None


def _clean_string(value: str) -> str:
    """Remove espaços extras e converte para minúsculo."""
    if pd.isna(value) or value == "":
        return None
    
    value = str(value).strip().lower()
    
    # NFD separa as letras dos acentos ('ã' vira 'a' + '~').
    separated_string = unicodedata.normalize('NFD', value)
    
    # Filtra apenas os caracteres que não são marcas de acentuação.
    # Pega a string separada em caracteres e remove os que são do tipo 'Mn'.
    # Mn é o tipo para caracteres de marcação de acentuação.
    return ''.join(
        c for c in separated_string if unicodedata.category(c) != 'Mn'
    )


def _clean_phone(phone: str) -> str:
    """
    Remove caracteres não numéricos e retorna apenas os dígitos.
    Formato de saída: DDD + número (sem parênteses ou traços).
    """
    if pd.isna(phone) or phone == "" or str(phone).lower() == "invalid_phone":
        return None
    
    # Removendo o código do país, assumindo que são todos do Brasil
    phone = str(phone).replace("+55", "")
    
    # Mantém APENAS números
    # Ex: "(11) 4002-8922" vira "1140028922"
    digits = ''.join(filter(str.isdigit, phone))
    
    if len(digits) < 10 or len(digits) > 11:
        return None
        
    return digits


def _sort_rows_by(
    df: pd.DataFrame, 
    columns: str | list[str],
    ascending: bool = True
) -> pd.DataFrame:
    """Ordena o DataFrame pelas colunas especificadas."""
    return df.sort_values(
        by=columns,
        ascending=ascending
        ).reset_index(drop=True)


def _drop_duplicate_rows(
    df: pd.DataFrame,
    subset: str | list[str]
) -> pd.DataFrame:
    """Remove linhas duplicadas com base nas colunas especificadas."""
    return df.drop_duplicates(subset=subset).reset_index(drop=True)


def _clean_monetary_value(value: str | float) -> float | None:
    """
    Converte valores para float, tratando padrões brasileiros.

    Regras:
    - Strings com vírgula: "2.026,00" → 2026.00
    - Valores negativos: retorna None
    - Valores vazios ou inválidos: retorna None
    """
    if pd.isna(value):
        return None

    try:
        if isinstance(value, str):
            val_str = value.strip()
            # Trata separador de milhares e decimais: "2.026,00" -> "2026.00"
            if ',' in val_str and '.' in val_str:
                val_str = val_str.replace('.', '')
            val_str = val_str.replace(',', '.')
            num = float(val_str)
        else:
            num = float(value)

        return num if num >= 0 else None

    except (ValueError, TypeError):
        return None


def _clean_quantity(value: int | float | str | None) -> int:
    """
    Normaliza valores de quantidade para um inteiro.

    Regras:
    - Valores nulos ou vazios: retorna 0.
    - Strings numéricas (incluindo com ponto decimal): convertidas para int.
    - Palavras em inglês específicas: "one", "two", "three", "four" → 1, 2, 3, 4.
    - Outros valores inválidos ou não conversíveis: retornam 0.
    """
    if pd.isna(value) or value == "":
        return 0
    val_str = str(value).strip().lower()
    
    # Vou manter a lista até o four pois só vi acontecer com o 'two'.
    # Em um cenário real, seria melhor ter uma lista mais completa.
    text_to_int = {
        'one': 1,
        'two': 2,
        'three': 3,
        'four': 4
        }
    
    if val_str in text_to_int:
        return text_to_int[val_str]
    try:
        return int(float(val_str))
    except ValueError:
        return 0


def _clean_shipment_dates(df: pd.DataFrame) -> pd.DataFrame:
    """Limpa e converte as colunas de datas de envio e entrega."""
    df_shipments = df.copy()
    
    not_shipped_status = ['label_created']
    not_delivered_status = not_shipped_status + ['in_transit', 'lost']  
    
    # Se o status diz que não enviou, limpamos o timestamp de envio
    not_shipped = df['delivery_status'].isin(not_shipped_status)
    df_shipments.loc[not_shipped, ['shipped_ts', 'delivered_ts']] = pd.NA
    
    # Se o status diz que não entregou, limpamos o timestamp de entrega
    not_delivered = df['delivery_status'].isin(not_delivered_status)
    df_shipments.loc[not_delivered, 'delivered_ts'] = pd.NA
    
    # O pd.to_datetime converterá pd.NA em NaT (Not a Time) automaticamente
    for col in ['shipped_ts', 'delivered_ts']:
        df_shipments[col] = pd.to_datetime(
            df_shipments[col],
            format='mixed',
            dayfirst=True,
            errors='coerce',
            utc=True
        )
    
    return df_shipments


def _delivery_date_validation(df: pd.DataFrame) -> pd.DataFrame:
    """
    Valida se há casos onde delivered_ts é anterior a shipped_ts.
    Imprime aviso e limpa ambas as datas caso encontre inconsistências.
    """
    if 'delivered_ts' not in df.columns or 'shipped_ts' not in df.columns:
        return df
    
    # Remove NaT para não gerar problemas
    mask_valid = df['delivered_ts'].notna() & df['shipped_ts'].notna()
    
    if not mask_valid.any():
        return df
    
    # Verifica se shipped_ts > delivered_ts
    mask_invalid = mask_valid & (df['shipped_ts'] > df['delivered_ts'])
    
    if mask_invalid.any():
        # Limpa ambas as datas para os casos inválidos
        df.loc[mask_invalid, ['shipped_ts', 'delivered_ts']] = pd.NaT
        print(
            f"Foram encontrados {mask_invalid.sum()} registros "
            f"com 'delivered_ts' anterior a 'shipped_ts'. "
            f"As datas foram limpas nesses registros."
        )
    return df


def _process_customers_table(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df = _drop_empty_ids(df, 'customer_id')
    
    df['state'] = df['state'].apply(_clean_state_code)
    
    df['city'] = df['city'].apply(_clean_string)
    
    df['created_ts'] = pd.to_datetime(
        df['created_ts'],
        format='mixed',
        dayfirst=True,
        errors='coerce',
        utc=True
    )
    
    if 'phone' not in df.columns:
        df['phone'] = pd.NA
    else:
        df['phone'] = df['phone'].apply(_clean_phone)
    
    # Ordena por ID e DATA (a mais recente fica em cima)
    # Aqui assumindo que queremos garantir ordem de ID primeiro
    df = _sort_rows_by(df, ["customer_id", "created_ts"], ascending=[True, False])
    
    # Agora que o mais recente está no topo, o drop_duplicates mantém ele
    df = _drop_duplicate_rows(df, "customer_id")
    
    # Definição da ordem exata das colunas na tabela final
    column_order = [
        "customer_id",
        "state",
        "city",
        "created_ts",
        "phone",
        "_source_file_folder",
        "_source_file_name",
        "_source_file_ingest_date",
        "_source_file_modified_ts",
        "_processed_ts"
    ]
    df = df.reindex(columns=column_order)
    
    return df


def _process_order_items_table(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df = _drop_empty_ids(df, 'order_id')
    df = _drop_empty_ids(df, 'product_id')
    
    df['quantity'] = df['quantity'].apply(_clean_quantity)
    
    df['unit_price'] = df['unit_price'].apply(_clean_monetary_value)
    
    df['discount_amount'] = df['discount_amount'].apply(_clean_monetary_value)
    
    # Ordena por order_id e product_id
    df = _sort_rows_by(df, ["order_id", "product_id"], ascending=[True, True])
    
    # Garante que não haja duplicatas de order_id + product_id
    df = _drop_duplicate_rows(df, ["order_id", "product_id"])
    
    # Definição da ordem exata das colunas na tabela final
    column_order = [
        "order_id",
        "product_id",
        "quantity",
        "unit_price",
        "discount_amount",
        "_source_file_folder",
        "_source_file_name",
        "_source_file_ingest_date",
        "_source_file_modified_ts",
        "_processed_ts"
    ]
    
    df = df.reindex(columns=column_order)
    
    return df


def _process_orders_table(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df = _drop_empty_ids(df, 'order_id')
    df = _drop_empty_ids(df, 'customer_id')
    
    df['order_ts'] = pd.to_datetime(
        df['order_ts'],
        format='mixed',
        dayfirst=True,
        errors='coerce',
        utc=True
    )
    
    df['status'] = df['status'].apply(_clean_string)
    df['payment_method'] = df['payment_method'].apply(_clean_string)
    
    df['total_amount'] = df['total_amount'].apply(_clean_monetary_value)
    
    df['currency'] = df['currency'].apply(_clean_string)
    
    if 'sales_channel' not in df.columns:
        df['sales_channel'] = pd.NA
    else:
        df['sales_channel'] = df['sales_channel'].apply(_clean_string)
    
    # Ordena por order_id, order_ts e customer_id (a mais recente fica em cima)
    # Aqui queremos garantir ordem de ID primeiro
    df = _sort_rows_by(df, ["order_id", "order_ts", "customer_id"], ascending=[True, False, True])
    
    # Agora que o mais recente está no topo, o drop_duplicates mantém ele
    df = _drop_duplicate_rows(df, "order_id")
    
    # Definição da ordem exata das colunas na tabela final
    column_order = [
        "order_id",
        "customer_id",
        "order_ts",
        "status",
        "payment_method",
        "total_amount",
        "currency",
        "sales_channel",
        "_source_file_folder",
        "_source_file_name",
        "_source_file_ingest_date",
        "_source_file_modified_ts",
        "_processed_ts"
    ]
    
    df = df.reindex(columns=column_order)
    
    return df


def _process_products_table(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df = _drop_empty_ids(df, 'product_id')
    
    df['category'] = df['category'].apply(_clean_string)
    
    df['brand'] = df['brand'].apply(_clean_string)
    
    df['created_ts'] = pd.to_datetime(
        df['created_ts'],
        format='mixed',
        dayfirst=True,
        errors='coerce',
        utc=True
    )
    
    # Ordena por ID e DATA (a mais recente fica em cima)
    # Aqui assumindo que queremos garantir ordem de ID primeiro
    df = _sort_rows_by(df, ["product_id", "created_ts"], ascending=[True, False])
    
    # Agora que o mais recente está no topo, o drop_duplicates mantém ele
    df = _drop_duplicate_rows(df, "product_id")
    
    # Definição da ordem exata das colunas na tabela final
    column_order = [
        "product_id",
        "category",
        "brand",
        "created_ts",
        "_source_file_folder",
        "_source_file_name",
        "_source_file_ingest_date",
        "_source_file_modified_ts",
        "_processed_ts"
    ]

    df = df.reindex(columns=column_order)
    
    return df


def _process_shipments_table(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df = _drop_empty_ids(df, 'order_id')
    
    df['carrier'] = df['carrier'].apply(_clean_string)
    
    df['shipping_cost'] = df['shipping_cost'].apply(_clean_monetary_value)
        
    df['delivery_status'] = df['delivery_status'].apply(_clean_string)
    
    df = _clean_shipment_dates(df)
    
    # Ordena por order_id e datas de envio/entrega (mais recentes primeiro)
    df = _sort_rows_by(df, ["order_id", "shipped_ts", "delivered_ts"], ascending=[True, False, False])
    
    # Agora que o mais recente está no topo, o drop_duplicates mantém ele
    df = _drop_duplicate_rows(df, "order_id")
    
    # Definição da ordem exata das colunas na tabela final
    column_order = [
        "order_id",
        "carrier",
        "shipping_cost",
        "shipped_ts",
        "delivered_ts",
        "delivery_status",
        "_source_file_folder",
        "_source_file_name",
        "_source_file_ingest_date",
        "_source_file_modified_ts",
        "_processed_ts"
    ]
    df = df.reindex(columns=column_order)
    
    df = _delivery_date_validation(df)
    
    return df


def _address_processing_function(file_name: str) -> Callable:
    file_name = file_name.lower()
    
    if "order_items" in file_name:
        return _process_order_items_table
    elif "orders" in file_name:
        return _process_orders_table
    elif "customers" in file_name:
        return _process_customers_table
    elif "products" in file_name:
        return _process_products_table
    elif "shipments" in file_name:
        return _process_shipments_table
    else:
        raise ValueError(
            f"Nenhum processador definido para o arquivo {file_name}."
        )


def process_silver_data(bronze_file_path: str) -> bool:
    """
    Função principal para processar os dados da camada silver.
    
    1. Lê o arquivo Bronze.
    2. Identifica qual tabela é pelo nome do arquivo.
    3. Chama a função de limpeza específica.
    4. Atualiza metadados.
    5. Salva na Silver.
    """
    layer = "silver"
    
    df = read_from_file("csv", bronze_file_path, dtype=str)
    
    df = _mark_empty_strings(df)
    
    source_file_name = os.path.basename(bronze_file_path)
    source_file_ingest_date = extract_ingest_date(bronze_file_path)

    df = _address_processing_function(source_file_name)(df)

    df["_processed_ts"] = pd.Timestamp.now(tz="UTC")

    # Monta o caminho com partição
    if source_file_ingest_date != "unknown":
        partition_folder = f"ingest_date={source_file_ingest_date}"
    else:
        partition_folder = "ingest_date=unknown"

    clean_name = os.path.splitext(source_file_name)[0]
    clean_name = clean_name.replace("_bronze", "")
    file_output = f"{partition_folder}/{clean_name}_silver.csv"
    success = export_to_file(file_output, df, layer)

    return success