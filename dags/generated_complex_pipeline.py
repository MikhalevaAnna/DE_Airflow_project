"""
Auto-generated DAG from complex_pipeline.py
Generated: 2026-01-25 18:03:57
Total tasks: 4
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging

# Default arguments for the DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(seconds=30),
}

# === ВСТРОЕННЫЕ ФУНКЦИИ ИЗ ИСХОДНОГО ФАЙЛА ===

# Функция: main_pipeline
def main_pipeline(**kwargs):
    """Функция, адаптированная для Airflow"""
    logger = logging.getLogger(__name__)
    logger.info("Starting main_pipeline")
    try:
        """
        Главный пайплайн обработки данных
        """
        # Функция вызывает другие функции
        data = extract()
        processed = transform(data)
        load(processed)
    except Exception as e:
        logger.error(f"Error in main_pipeline: {e}")
        raise
    finally:
        logger.info("Completed main_pipeline")

# Функция: extract
def extract(**kwargs):
    """Функция, адаптированная для Airflow"""
    logger = logging.getLogger(__name__)
    logger.info("Starting extract")
    try:
        return "raw_data"
    except Exception as e:
        logger.error(f"Error in extract: {e}")
        raise
    finally:
        logger.info("Completed extract")

# Функция: transform
def transform(data, **kwargs):
    """Функция, адаптированная для Airflow"""
    logger = logging.getLogger(__name__)
    logger.info("Starting transform")
    try:
        return f"processed_{data}"
    except Exception as e:
        logger.error(f"Error in transform: {e}")
        raise
    finally:
        logger.info("Completed transform")

# Функция: load
def load(data, **kwargs):
    """Функция, адаптированная для Airflow"""
    logger = logging.getLogger(__name__)
    logger.info("Starting load")
    try:
        logger.info(f"Loaded: {data}")
    except Exception as e:
        logger.error(f"Error in load: {e}")
        raise
    finally:
        logger.info("Completed load")

with DAG(
    dag_id="generated_complex_pipeline",
    default_args=default_args,
    start_date=datetime(2026, 1, 25),
    schedule_interval="@once",
    catchup=False,
    tags=["py-auto"]
) as dag:

    # Python задачи:
    execute_main_pipeline = PythonOperator(
        task_id="execute_main_pipeline",
        python_callable=main_pipeline,
        doc_md="""Главный пайплайн обработки данных""",
    )