import os

import polars as pl
from src.utils.logger import logger
from src.etl.transform import type_transform

def extraction_data(p: str) -> dict[str, pl.LazyFrame]:
    import glob

    frames = {}

    if not os.path.exists(p):
        raise FileNotFoundError(f"Directory not found: {p}")

    path_file = glob.glob(os.path.join(p, "*.csv"), recursive=True)

    if not path_file:
        logger.error("File not found")
        raise FileNotFoundError("File not found in variable path_file")

    logger.info(f"file found: {path_file}")

    for file in path_file:
        logger.info(f"Reading {file}")
        lf = pl.scan_csv(file)
        file_name = os.path.basename(file)
        frames[file_name] = lf
        logger.info(f"File ready for loading: {file}")

    return frames
