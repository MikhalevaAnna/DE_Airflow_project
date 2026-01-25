# DAG_SCHEDULE_INTERVAL: @daily
# DAG_TAGS: ['data_processing', 'etl']
# DAG_START_DATE: 2024-01-01
# DAG_RETRIES: 2
# DAG_RETRY_DELAY: 3
# DAG_CATCHUP: false

import pandas as pd
import logging

logger = logging.getLogger(__name__)

def extract_data():
    """Извлечение данных из источника"""
    logger.info("Extracting data...")
    # Здесь мог бы быть код для извлечения данных
    data = {"column1": [1, 2, 3], "column2": ["A", "B", "C"]}
    df = pd.DataFrame(data)
    logger.info(f"Extracted {len(df)} rows")
    return df

def transform_data():
    """Трансформация данных"""
    logger.info("Transforming data...")
    # Простая трансформация
    df = extract_data()  # Это демо, в реальности параметры
    df['column3'] = df['column1'] * 2
    logger.info("Data transformation completed")
    return df

def load_data():
    """Загрузка данных в хранилище"""
    logger.info("Loading data...")
    # Здесь мог бы быть код для загрузки в БД
    df = transform_data()  # Это демо
    logger.info(f"Loaded {len(df)} rows")
    return True