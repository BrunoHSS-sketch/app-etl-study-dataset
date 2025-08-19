import os

from loguru import logger

os.makedirs("logs", exist_ok=True)

logger.add(
    "logs/etl.log",
    rotation= "10 MB",
    retention= "10 days",
    compression= "zip",
    level= "INFO",
    format= "{time: YYYY-MM-DD HH:mm:ss} | {level} | {message}"
)
