import json
from datetime import date
import logging

logger = logging.getLogger(__name__)


def load_data():

    file_path = f"./data/YT_data_{date.today()}.json"

    try:
        logger.info(f"processing file: YT_data_{date.today()}.json")

        with open(file_path, "r", encoding="utf-8") as raw_data:
            data = json.load(raw_data)

        return data
    
    except FileNotFoundError as e:
        logger.error(f"File not fount: {file_path}")

    except json.JSONDecodeError:
        logger.error(f"Invalid json in file: {file_path}")

