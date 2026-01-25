"""
Auto-generated DAG from etl_create_df.py
Generated: 2026-01-25 18:03:57
Total tasks: 4
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging
import pandas as pd

# Default arguments for the DAG
default_args = {
    "owner": "data_engineer",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(seconds=30),
}

# === ВСТРОЕННЫЕ ФУНКЦИИ ИЗ ИСХОДНОГО ФАЙЛА ===

# Функция: extract
def extract(**kwargs):
    """Функция, адаптированная для Airflow"""
    logger = logging.getLogger(__name__)
    logger.info("Starting extract")
    try:
        """Извлечение данных"""
        return pd.DataFrame({'col': [1, 2, 3]})
    except Exception as e:
        logger.error(f"Error in extract: {e}")
        raise
    finally:
        logger.info("Completed extract")

# Функция: transform
def transform(df, **kwargs):
    """Функция, адаптированная для Airflow"""
    logger = logging.getLogger(__name__)
    logger.info("Starting transform")
    try:
        """Трансформация (принимает параметр!)"""
        df['new_col'] = df['col'] * 2
        return df
    except Exception as e:
        logger.error(f"Error in transform: {e}")
        raise
    finally:
        logger.info("Completed transform")

# Функция: load
def load(df, **kwargs):
    """Функция, адаптированная для Airflow"""
    logger = logging.getLogger(__name__)
    logger.info("Starting load")
    try:
        """Загрузка (принимает параметр!)"""
        df.to_csv('/tmp/output.csv', index=False)
    except Exception as e:
        logger.error(f"Error in load: {e}")
        raise
    finally:
        logger.info("Completed load")

# Функция: etl_pipeline
def etl_pipeline(**kwargs):
    """Функция, адаптированная для Airflow"""
    logger = logging.getLogger(__name__)
    logger.info("Starting etl_pipeline")
    try:
        """Главная функция пайплайна"""
        data = extract()
        transformed = transform(data)
        load(transformed)
    except Exception as e:
        logger.error(f"Error in etl_pipeline: {e}")
        raise
    finally:
        logger.info("Completed etl_pipeline")

with DAG(
    dag_id="generated_etl_create_df",
    default_args=default_args,
    start_date=datetime(2026, 1, 25),
    schedule_interval="0 2 * * *",
    catchup=False,
    tags=["etl", "daily"]
) as dag:

    # Python задачи:
    execute_etl_pipeline = PythonOperator(
        task_id="execute_etl_pipeline",
        python_callable=etl_pipeline,
        doc_md="""Главная функция пайплайна""",
    )