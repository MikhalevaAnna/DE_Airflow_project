"""
Auto-generated DAG from data_pipeline.py
Generated: 2026-01-25 18:03:57
Total tasks: 3
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging

# Default arguments for the DAG
default_args = {
    "owner": "data_team",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(seconds=30),
}

# === ВСТРОЕННЫЕ ФУНКЦИИ ИЗ ИСХОДНОГО ФАЙЛА ===

# Функция: extract_data
def extract_data(**kwargs):
    """Функция, адаптированная для Airflow"""
    logger = logging.getLogger(__name__)
    logger.info("Starting extract_data")
    try:
        """Извлечение данных из источника"""
        logger.info("Extracting data from source")
    except Exception as e:
        logger.error(f"Error in extract_data: {e}")
        raise
    finally:
        logger.info("Completed extract_data")

# Функция: transform_data
def transform_data(**kwargs):
    """Функция, адаптированная для Airflow"""
    logger = logging.getLogger(__name__)
    logger.info("Starting transform_data")
    try:
        """Преобразование данных"""
        logger.info("Transforming data")
    except Exception as e:
        logger.error(f"Error in transform_data: {e}")
        raise
    finally:
        logger.info("Completed transform_data")

# Функция: load_data
def load_data(**kwargs):
    """Функция, адаптированная для Airflow"""
    logger = logging.getLogger(__name__)
    logger.info("Starting load_data")
    try:
        """Загрузка данных в хранилище"""
        logger.info("Loading data to warehouse")
    except Exception as e:
        logger.error(f"Error in load_data: {e}")
        raise
    finally:
        logger.info("Completed load_data")

with DAG(
    dag_id="generated_data_pipeline",
    default_args=default_args,
    start_date=datetime(2026, 1, 25),
    schedule_interval="@daily",
    catchup=False,
    tags=["etl", "data"],
    description="Daily data processing pipeline"
) as dag:

    # Python задачи:
    execute_extract_data = PythonOperator(
        task_id="execute_extract_data",
        python_callable=extract_data,
        doc_md="""Извлечение данных из источника""",
    )

    execute_transform_data = PythonOperator(
        task_id="execute_transform_data",
        python_callable=transform_data,
        doc_md="""Преобразование данных""",
    )

    execute_load_data = PythonOperator(
        task_id="execute_load_data",
        python_callable=load_data,
        doc_md="""Загрузка данных в хранилище""",
    )


    # Линейные зависимости (по умолчанию)
    execute_extract_data >> execute_transform_data >> execute_load_data