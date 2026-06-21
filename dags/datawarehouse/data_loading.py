import json
from datetime import date
import logging

logger = logging.getLogger(__name__)


def load_data():
    """
    Load today's extracted JSON file from the Airflow data directory.

    Returns:
        list[dict]: List of video records loaded from the JSON file.
    """
    file_path = f"/opt/airflow/data/youtube_data_{date.today()}.json"

    try:
        logger.info(f"Loading file: {file_path}")
        with open(file_path, 'r', encoding='utf-8') as raw_data:
            data = json.load(raw_data)
        logger.info(f"Successfully loaded {len(data)} records from {file_path}")
        return data

    except FileNotFoundError:
        logger.error(f"File not found: {file_path} — has the extraction DAG run today?")
        raise
    except json.JSONDecodeError as e:
        logger.error(f"Error decoding JSON from {file_path}: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error loading {file_path}: {e}")
        raise
