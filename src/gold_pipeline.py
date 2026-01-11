import pandas as pd
import numpy as np
from .utils import export_to_file

def _load_silver_table(
    file_list: list,
    table_name: str,
    id_columns: list
) -> pd.DataFrame:
    # Lê, concatena e deduplica arquivos silver mantendo versão mais recente.
    # Filtra por table_name e remove duplicatas baseado em _source_file_modified_ts.
    relevant_files = [
        file_name for file_name in file_list if table_name in file_name
    ]

    # Não travar a pipeline inteira se houver falha em arquivo específico.
    if not relevant_files:
        print(f"Nenhum arquivo encontrado para '{table_name}'.")
        return pd.DataFrame()

    # Lê todas as partições (ingest_date) e concatena.
    silver_tables = [
        pd.read_csv(file_name, low_memory=False)
        for file_name in relevant_files
    ]
    all_silver_tables = pd.concat(silver_tables, ignore_index=True)

    # Remove duplicatas pegando a última versão.
    if "_source_file_modified_ts" in all_silver_tables.columns:
        all_silver_tables = all_silver_tables.sort_values(
            "_source_file_modified_ts", ascending=False
        )

    return all_silver_tables.drop_duplicates(
        subset=id_columns, keep="first"
    )


def _calc_amounts(df_fact_order_items: pd.DataFrame) -> pd.DataFrame:
    """
    Agrupa itens por pedido e calcula totais monetários.

    Calcula: bruto, desconto e líquido para cada pedido.

    Args:
        df_fact_order_items: DataFrame com itens de pedidos.

    Returns:
        DataFrame com agregações (gross_amount, discount_total, net_amount).
    """
    df_calc = df_fact_order_items.copy()
    df_calc['item_gross_amount'] = (
        df_calc['quantity'] * df_calc['unit_price']
    )

    orders_agg = df_calc.groupby("order_id").agg(
        gross_amount=("item_gross_amount", "sum"),
        discount_total=("discount_amount", "sum")
    ).reset_index()

    # Arredonda valores monetários para 2 casas decimais.
    orders_agg["gross_amount"] = orders_agg["gross_amount"].round(2)
    orders_agg["discount_total"] = orders_agg["discount_total"].round(2)

    # Calcula: líquido = bruto - desconto.
    orders_agg["net_amount"] = (
        orders_agg["gross_amount"] - orders_agg["discount_total"]
    ).round(2)

    return orders_agg


def _prepare_logistics_data(df_shipments: pd.DataFrame) -> pd.DataFrame:
    """
    Prepara dados de logística com cálculos de tempo de entrega.

    Converte datas, calcula tempo de entrega e flag de atraso.

    Args:
        df_shipments: DataFrame com dados de envio.

    Returns:
        DataFrame com dados de logística processados.
    """
    df_shipments_calc = df_shipments.copy()

    # Garante tipos numéricos
    df_shipments_calc["shipping_cost"] = pd.to_numeric(
        df_shipments_calc["shipping_cost"], errors="coerce"
    ).fillna(0)

    # Converte datas (valores inválidos viram NaT).
    df_shipments_calc["shipped_ts"] = pd.to_datetime(
        df_shipments_calc["shipped_ts"], errors='coerce', utc=True
    )
    df_shipments_calc["delivered_ts"] = pd.to_datetime(
        df_shipments_calc["delivered_ts"], errors='coerce', utc=True
    )

    # Calcula tempo de entrega em horas (NaN para não entregues).
    df_shipments_calc["delivery_time_hours"] = (
        (df_shipments_calc["delivered_ts"] -
         df_shipments_calc["shipped_ts"]).dt.total_seconds() / 3600
    ).round(2)

    # Marca entrega atrasada (NaN para não entregues).
    df_shipments_calc["is_late"] = (
        df_shipments_calc["delivery_time_hours"] > 72.0
    )

    return df_shipments_calc


def _create_dim_customers(df_customers: pd.DataFrame) -> pd.DataFrame:
    # Seleciona colunas de cliente e ordena por ID.
    # Retorna DataFrame vazio se entrada estiver vazia.
    if df_customers.empty:
        return pd.DataFrame()

    df_dim_customers = df_customers[[
        "customer_id",
        "state",
        "city",
        "created_ts"
    ]].copy()

    return df_dim_customers.sort_values(
        by="customer_id",
        ascending=True
    ).reset_index(drop=True)


def _create_dim_products(df_products: pd.DataFrame) -> pd.DataFrame:
    # Seleciona colunas de produto e ordena por ID.
    # Retorna DataFrame vazio se entrada estiver vazia.
    if df_products.empty:
        return pd.DataFrame()

    df_dim_products = df_products[[
        "product_id",
        "category",
        "brand",
        "created_ts"
    ]].copy()

    return df_dim_products.sort_values(
        by="product_id",
        ascending=True
    ).reset_index(drop=True)


def _create_fact_order_items(
    df_order_items: pd.DataFrame
) -> pd.DataFrame:
    """
    Cria tabela fato de itens com cálculos de valor líquido.

    Converte tipos, calcula item_net_amount e ordena os dados.

    Args:
        df_order_items: DataFrame com itens de pedidos.

    Returns:
        DataFrame contendo tabela fato de itens.
    """
    df_fact_order_items = df_order_items.copy()

    # Garante tipos numéricos
    for col in ["quantity", "unit_price", "discount_amount"]:
        df_fact_order_items[col] = pd.to_numeric(
            df_fact_order_items[col], errors="coerce"
        ).fillna(0)

    df_fact_order_items["item_net_amount"] = (
        (df_fact_order_items["quantity"] *
         df_fact_order_items["unit_price"])
        - df_fact_order_items["discount_amount"]
    ).round(2)

    # Definição da ordem exata das colunas na tabela final
    column_order = [
        "order_id",
        "product_id",
        "quantity",
        "unit_price",
        "discount_amount",
        "item_net_amount"
    ]

    df_fact_order_items = df_fact_order_items.reindex(columns=column_order)

    return df_fact_order_items.sort_values(
        by=["order_id", "product_id"],
        ascending=True
    ).reset_index(drop=True)


def _create_fact_orders(
    df_orders: pd.DataFrame,
    df_fact_order_items: pd.DataFrame,
    df_shipments: pd.DataFrame
) -> pd.DataFrame:
    """
    Cria tabela fato de pedidos.

    Recebe df_fact_order_items (com item_net_amount calculado),
    agrega e cruza com dados de logística.

    Args:
        df_orders: DataFrame com dados de pedidos.
        df_fact_order_items: DataFrame com itens (já processados).
        df_shipments: DataFrame com dados de envio (opcional).

    Returns:
        DataFrame contendo tabela fato de pedidos.
    """

    fact_orders_agg = _calc_amounts(df_fact_order_items)
    df_fact_orders = df_orders.merge(fact_orders_agg, on="order_id", how="left")

    # Join com dados de logística (shipments).
    if not df_shipments.empty:
        df_shipments_processed = _prepare_logistics_data(df_shipments)

        shipment_cols = [
            "order_id", "carrier", "shipping_cost",
            "shipped_ts", "delivered_ts",
            "delivery_time_hours", "is_late"
        ]
        df_fact_orders = df_fact_orders.merge(
            df_shipments_processed[shipment_cols],
            on="order_id", how="left"
        )
    else:
        # Cria colunas vazias se não houver dados de entrega.
        # Usa tipos apropriados: NaT (datetime), np.nan (float), pd.NA (text/bool).
        df_fact_orders["carrier"] = pd.NA
        df_fact_orders["shipping_cost"] = np.nan
        df_fact_orders["shipped_ts"] = pd.NaT
        df_fact_orders["delivered_ts"] = pd.NaT
        df_fact_orders["delivery_time_hours"] = np.nan
        df_fact_orders["is_late"] = pd.NA

    # Renomeia status se necessário.
    if "status" in df_fact_orders.columns:
        df_fact_orders = df_fact_orders.rename(
            columns={"status": "status_final"}
        )
    df_fact_orders["order_date"] = pd.to_datetime(
        df_fact_orders["order_ts"], errors='coerce', utc=True
    )

    # Definição da ordem exata das colunas na tabela final
    column_order = [
        "order_id", "customer_id", "order_date", "order_ts",
        "gross_amount", "discount_total", "net_amount",
        "payment_method", "status_final", "carrier", "shipping_cost",
        "shipped_ts", "delivered_ts", "delivery_time_hours", "is_late"
    ]

    df_fact_orders = df_fact_orders.reindex(columns=column_order)

    return df_fact_orders.sort_values(
        by=["order_id", "customer_id"],
        ascending=True
    ).reset_index(drop=True)


def process_gold_data(silver_files: list) -> bool:
    """
    Orquestra a execução do pipeline gold.

    Carrega dados da silver, transforma em dimensões e fatos,
    e salva na camada gold.

    Args:
        silver_files: Lista de arquivos silver para processar.

    Returns:
        True se pipeline executado com sucesso.
    """
    layer = "gold"

    df_orders = _load_silver_table(
        silver_files, "orders_silver.csv", ["order_id"]
    )
    df_items = _load_silver_table(
        silver_files, "order_items_silver.csv", ["order_id", "product_id"]
    )
    df_customers = _load_silver_table(
        silver_files, "customers_silver.csv", ["customer_id"]
    )
    df_products = _load_silver_table(
        silver_files, "products_silver.csv", ["product_id"]
    )
    df_shipments = _load_silver_table(
        silver_files, "shipments_silver.csv", ["order_id"]
    )

    dim_customers = _create_dim_customers(df_customers)
    dim_products = _create_dim_products(df_products)

    fact_order_items = _create_fact_order_items(df_items)
    fact_orders = _create_fact_orders(
        df_orders, fact_order_items, df_shipments
    )

    export_to_file("dim_customers.csv", dim_customers, layer)
    export_to_file("dim_products.csv", dim_products, layer)
    export_to_file("fact_order_items.csv", fact_order_items, layer)
    export_to_file("fact_orders.csv", fact_orders, layer)
    return True