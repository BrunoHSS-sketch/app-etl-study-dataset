import polars as pl
from polars import LazyFrame
from src.utils.logger import logger

def type_transform(lf: dict[str, pl.LazyFrame]) -> dict[str, pl.LazyFrame]:
    logger.info("Converting column types")
    return {
        "clientes.csv": lf["clientes.csv"].with_columns(pl.col("cliente_id").cast(pl.String)),
        "produtos.csv": lf["produtos.csv"].with_columns(pl.col("produto_id").cast(pl.String)),
        "pedidos.csv": lf["pedidos.csv"].with_columns(pl.col("pedido_id").cast(pl.String), pl.col("cliente_id").cast(pl.String)),
        "itens_pedido.csv": lf["itens_pedido.csv"].with_columns(pl.col("pedido_id").cast(pl.String), pl.col("item_id").cast(pl.String), pl.col("produto_id").cast(pl.String))
    }

def transforming_data(l: dict[str,pl.LazyFrame]) -> pl.LazyFrame:
    lazy_frame = (
        l["itens_pedido.csv"]
        .join(l["produtos.csv"], on="produto_id", how="left")
        .join(l["pedidos.csv"], on="pedido_id", how="left")
        .join(l["clientes.csv"], on="cliente_id", how="left")
        .with_columns(
            (pl.col("quantidade") * pl.col("preco_unitario")).alias("total_itens")
        )
        .select([
            "data_pedido",
            "nome",
            "estado",
            "produto_nome",
            "nome_categoria",
            "quantidade",
            "preco_unitario",
            "total_itens",
            "status"
        ])
    )
    return lazy_frame