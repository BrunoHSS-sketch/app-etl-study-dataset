import os

import polars as pl
from src.utils.logger import logger

def loader(lf: pl.LazyFrame, file_name: str = "ecommerce.xlsx"):
    df = lf.collect()
    logger.info("Initializing table creation")
    processed_path = os.getenv("DATA_PROCESSED_PATH")

    if not processed_path:
        raise ValueError("DATA_PROCESSED_PATH not found in .env")

    output_file = os.path.join(processed_path, file_name)
    df.write_excel(output_file, autofit=True)
    logger.success("Tabel create")
    return output_file