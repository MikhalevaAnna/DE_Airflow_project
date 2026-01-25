"""
Auto-generated DAG from simple_etl.py
Generated: 2026-01-25 18:03:57
Total tasks: 5
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging
import pandas as pd
import numpy as np
import json
import os
from pathlib import Path

# Default arguments for the DAG
default_args = {
    "owner": "data_engineer",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(seconds=5),
}

# === ВСТРОЕННЫЕ ФУНКЦИИ ИЗ ИСХОДНОГО ФАЙЛА ===

# Функция: generate_sample_data
def generate_sample_data(**kwargs):
    """Функция, адаптированная для Airflow"""
    logger = logging.getLogger(__name__)
    logger.info("Starting generate_sample_data")
    try:
        """Генерация тестовых данных"""
        logger.info("Генерируем тестовые данные...")

        # Создаем DataFrame с тестовыми данными
        data = {
            'user_id': range(1, 101),
            'name': [f'User_{i}' for i in range(1, 101)],
            'age': np.random.randint(18, 65, 100),
            'city': np.random.choice(['Moscow', 'SPb', 'Kazan', 'Novosibirsk'], 100),
            'signup_date': [datetime.now() - timedelta(days=np.random.randint(0, 365))
                            for _ in range(100)],
            'revenue': np.random.uniform(10, 1000, 100).round(2)
        }

        df = pd.DataFrame(data)
        logger.info(f"Сгенерировано {len(df)} записей")
        return df
    except Exception as e:
        logger.error(f"Error in generate_sample_data: {e}")
        raise
    finally:
        logger.info("Completed generate_sample_data")

# Функция: clean_data
def clean_data(**kwargs):
    """Функция, адаптированная для Airflow"""
    logger = logging.getLogger(__name__)
    logger.info("Starting clean_data")
    try:
        """Очистка данных"""
        logger.info("Начинаем очистку данных...")

        # Получаем данные
        df = generate_sample_data()

        # Удаляем дубликаты
        df = df.drop_duplicates(subset=['user_id'])

        # Заполняем пропуски
        df['age'] = df['age'].fillna(df['age'].median())

        # Удаляем выбросы в revenue
        q1 = df['revenue'].quantile(0.25)
        q3 = df['revenue'].quantile(0.75)
        iqr = q3 - q1
        df = df[(df['revenue'] >= q1 - 1.5 * iqr) & (df['revenue'] <= q3 + 1.5 * iqr)]

        logger.info(f"После очистки осталось {len(df)} записей")
        return df
    except Exception as e:
        logger.error(f"Error in clean_data: {e}")
        raise
    finally:
        logger.info("Completed clean_data")

# Функция: calculate_metrics
def calculate_metrics(**kwargs):
    """Функция, адаптированная для Airflow"""
    logger = logging.getLogger(__name__)
    logger.info("Starting calculate_metrics")
    try:
        """Расчет метрик"""
        logger.info("Расчет метрик...")

        df = clean_data()

        metrics = {
            'total_users': len(df),
            'avg_age': df['age'].mean().round(2),
            'total_revenue': df['revenue'].sum().round(2),
            'avg_revenue_per_user': (df['revenue'].sum() / len(df)).round(2),
            'users_by_city': df['city'].value_counts().to_dict(),
            'top_10_users': df.nlargest(10, 'revenue')[['user_id', 'name', 'revenue']].to_dict('records')
        }

        logger.info(f"Метрики: {metrics}")
        return metrics
    except Exception as e:
        logger.error(f"Error in calculate_metrics: {e}")
        raise
    finally:
        logger.info("Completed calculate_metrics")

# Функция: save_to_json
def save_to_json(**kwargs):
    """Функция, адаптированная для Airflow"""
    logger = logging.getLogger(__name__)
    logger.info("Starting save_to_json")
    try:
        """Сохранение результатов в JSON"""
        logger.info("Сохранение в JSON...")

        metrics = calculate_metrics()

        # Сохраняем метрики
        import json
        from pathlib import Path

        output_dir = Path("/tmp/airflow_output")
        output_dir.mkdir(exist_ok=True)

        filename = output_dir / f"metrics_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"

        with open(filename, 'w') as f:
            json.dump(metrics, f, indent=2, default=str)

        logger.info(f"Метрики сохранены в {filename}")
        return str(filename)
    except Exception as e:
        logger.error(f"Error in save_to_json: {e}")
        raise
    finally:
        logger.info("Completed save_to_json")

# Функция: send_notification
def send_notification(**kwargs):
    """Функция, адаптированная для Airflow"""
    logger = logging.getLogger(__name__)
    logger.info("Starting send_notification")
    try:
        """Отправка уведомления о завершении"""
        logger.info("Отправка уведомления...")

        # Здесь могла бы быть интеграция с Slack/Telegram/Email
        filename = save_to_json()

        message = f"""
        ✅ ETL пайплайн успешно выполнен!
        📊 Результаты сохранены в: {filename}
        🕒 Время выполнения: {datetime.now()}
        """

        logger.info(message)

        # Для демонстрации просто записываем в лог
        notification_file = Path("/tmp/airflow_output/notifications.log")
        with open(notification_file, 'a') as f:
            f.write(f"{datetime.now()}: {message}\n")

        return True
    except Exception as e:
        logger.error(f"Error in send_notification: {e}")
        raise
    finally:
        logger.info("Completed send_notification")

with DAG(
    dag_id="generated_simple_etl",
    default_args=default_args,
    start_date=datetime(2026, 1, 25),
    schedule_interval="@hourly",
    catchup=False,
    tags=["etl", "simple", "data"]
) as dag:

    # Python задачи:
    execute_clean_data = PythonOperator(
        task_id="execute_clean_data",
        python_callable=clean_data,
        doc_md="""Очистка данных""",
    )