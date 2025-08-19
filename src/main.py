import os

from dotenv import load_dotenv
from etl.extract import extraction_data
from etl.transform import type_transform
from etl.transform import transforming_data
from etl.load import loader

load_dotenv()
data_path = os.getenv("DATA_PATH")

if not data_path:
    raise ValueError("DATA_PATH not found in .env file!")

if __name__ == '__main__':
    lf = extraction_data(data_path)
    lf = type_transform(lf)
    lf = transforming_data(lf)
    loader(lf)