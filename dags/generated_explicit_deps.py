"""
Auto-generated DAG from explicit_deps.py
Generated: 2026-01-25 18:03:57
Total tasks: 3
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

# Функция: extract
def extract(**kwargs):
    """Функция, адаптированная для Airflow"""
    logger = logging.getLogger(__name__)
    logger.info("Starting extract")
    try:
        """Извлечь данные"""
        return "data"
    except Exception as e:
        logger.error(f"Error in extract: {e}")
        raise
    finally:
        logger.info("Completed extract")

# Функция: transform
def transform(**kwargs):
    """Функция, адаптированная для Airflow"""
    logger = logging.getLogger(__name__)
    logger.info("Starting transform")
    try:
        """Преобразовать данные"""
        return "transformed"
    except Exception as e:
        logger.error(f"Error in transform: {e}")
        raise
    finally:
        logger.info("Completed transform")

# Функция: load
def load(**kwargs):
    """Функция, адаптированная для Airflow"""
    logger = logging.getLogger(__name__)
    logger.info("Starting load")
    try:
        """Загрузить данные"""
        logger.info("loaded")
    except Exception as e:
        logger.error(f"Error in load: {e}")
        raise
    finally:
        logger.info("Completed load")

with DAG(
    dag_id="generated_explicit_deps",
    default_args=default_args,
    start_date=datetime(2026, 1, 25),
    schedule_interval="@once",
    catchup=False,
    tags=["py-auto"]
) as dag:

    # Python задачи:
    execute_extract = PythonOperator(
        task_id="execute_extract",
        python_callable=extract,
        doc_md="""Извлечь данные""",
    )

    execute_transform = PythonOperator(
        task_id="execute_transform",
        python_callable=transform,
        doc_md="""Преобразовать данные""",
    )

    execute_load = PythonOperator(
        task_id="execute_load",
        python_callable=load,
        doc_md="""Загрузить данные""",
    )

    execute_extract >> execute_transform
    execute_transform >> execute_load