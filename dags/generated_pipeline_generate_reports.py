"""
Auto-generated DAG from pipeline_generate_reports.py
Generated: 2026-01-25 18:03:57
Total tasks: 7
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
import hashlib

# Default arguments for the DAG
default_args = {
    "owner": "data_team",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(seconds=60),
}

# === ВСТРОЕННЫЕ ФУНКЦИИ ИЗ ИСХОДНОГО ФАЙЛА ===

# Функция: extract_sales_data
def extract_sales_data(date_filter: str = None, **kwargs):
    """Функция, адаптированная для Airflow"""
    logger = logging.getLogger(__name__)
    logger.info("Starting extract_sales_data")
    try:
        """
        Извлечение данных о продажах из источника
        Возвращает DataFrame с продажами за указанную дату
        """
        logger.info(f"Извлекаем данные о продажах за {date_filter or 'все время'}")

        # Имитация извлечения данных из БД/API
        np.random.seed(42)

        # Генерация тестовых данных
        n_records = 1000
        data = {
            'sale_id': range(1, n_records + 1),
            'product_id': np.random.randint(1, 50, n_records),
            'quantity': np.random.randint(1, 10, n_records),
            'price': np.round(np.random.uniform(10, 1000, n_records), 2),
            'sale_date': pd.date_range(start='2024-01-01', periods=n_records, freq='H'),
            'customer_id': np.random.randint(1000, 2000, n_records),
            'region': np.random.choice(['Moscow', 'SPb', 'Kazan', 'Novosibirsk', 'Ekaterinburg'], n_records)
        }

        df = pd.DataFrame(data)

        if date_filter:
            # Фильтрация по дате если указана
            filter_date = pd.to_datetime(date_filter)
            df = df[df['sale_date'].dt.date == filter_date.date()]

        logger.info(f"Извлечено {len(df)} записей о продажах")
        return df
    except Exception as e:
        logger.error(f"Error in extract_sales_data: {e}")
        raise
    finally:
        logger.info("Completed extract_sales_data")

# Функция: calculate_revenue_metrics
def calculate_revenue_metrics(sales_df, **kwargs):
    """Функция, адаптированная для Airflow"""
    logger = logging.getLogger(__name__)
    logger.info("Starting calculate_revenue_metrics")
    try:
        """
        Расчет метрик выручки по регионам и продуктам
        """
        logger.info("Расчет метрик выручки")

        if sales_df.empty:
            logger.warning("Нет данных для расчета метрик")
            return {}

        # Расчет общей выручки
        sales_df['revenue'] = sales_df['quantity'] * sales_df['price']
        total_revenue = sales_df['revenue'].sum()

        # Метрики по регионам
        region_metrics = sales_df.groupby('region').agg({
            'revenue': 'sum',
            'quantity': 'sum',
            'sale_id': 'count'
        }).rename(columns={'sale_id': 'transaction_count'})

        # Метрики по продуктам (топ-10)
        product_metrics = sales_df.groupby('product_id').agg({
            'revenue': 'sum',
            'quantity': 'sum'
        }).sort_values('revenue', ascending=False).head(10)

        # Дополнительные метрики
        avg_revenue_per_transaction = total_revenue / len(sales_df)
        most_profitable_region = region_metrics['revenue'].idxmax()
        most_profitable_product = product_metrics['revenue'].idxmax()

        metrics = {
            'total_revenue': float(total_revenue),
            'total_transactions': len(sales_df),
            'avg_revenue_per_transaction': float(avg_revenue_per_transaction),
            'most_profitable_region': most_profitable_region,
            'most_profitable_product': int(most_profitable_product),
            'region_metrics': region_metrics.to_dict('index'),
            'top_products': product_metrics.to_dict('index'),
            'calculation_timestamp': datetime.now().isoformat()
        }

        logger.info(f"Рассчитано {len(metrics)} метрик")
        return metrics
    except Exception as e:
        logger.error(f"Error in calculate_revenue_metrics: {e}")
        raise
    finally:
        logger.info("Completed calculate_revenue_metrics")

# Функция: detect_anomalies
def detect_anomalies(sales_df, revenue_metrics, **kwargs):
    """Функция, адаптированная для Airflow"""
    logger = logging.getLogger(__name__)
    logger.info("Starting detect_anomalies")
    try:
        """
        Детекция аномалий в данных о продажах
        """
        logger.info("Детекция аномалий в данных")

        anomalies = []

        # Проверка на пустые данные
        if sales_df.empty:
            anomalies.append({
                'type': 'empty_data',
                'severity': 'high',
                'message': 'Нет данных о продажах'
            })
            return anomalies

        # Проверка аномальных цен
        price_stats = sales_df['price'].describe()
        price_q1 = price_stats['25%']
        price_q3 = price_stats['75%']
        price_iqr = price_q3 - price_q1
        price_upper_bound = price_q3 + 1.5 * price_iqr

        high_price_anomalies = sales_df[sales_df['price'] > price_upper_bound]
        if not high_price_anomalies.empty:
            anomalies.append({
                'type': 'high_price_anomaly',
                'severity': 'medium',
                'message': f'Обнаружено {len(high_price_anomalies)} аномально высоких цен',
                'details': high_price_anomalies[['sale_id', 'price']].to_dict('records')
            })

        # Проверка аномальных количеств
        quantity_anomalies = sales_df[sales_df['quantity'] > 20]
        if not quantity_anomalies.empty:
            anomalies.append({
                'type': 'high_quantity_anomaly',
                'severity': 'low',
                'message': f'Обнаружено {len(quantity_anomalies)} записей с большим количеством товаров',
                'details': quantity_anomalies[['sale_id', 'quantity']].to_dict('records')
            })

        # Проверка выручки по регионам
        if 'region_metrics' in revenue_metrics:
            for region, metrics in revenue_metrics['region_metrics'].items():
                if metrics['revenue'] == 0:
                    anomalies.append({
                        'type': 'zero_revenue_region',
                        'severity': 'medium',
                        'message': f'Регион {region} имеет нулевую выручку',
                        'region': region
                    })

        logger.info(f"Обнаружено {len(anomalies)} аномалий")
        return anomalies
    except Exception as e:
        logger.error(f"Error in detect_anomalies: {e}")
        raise
    finally:
        logger.info("Completed detect_anomalies")

# Функция: generate_daily_report
def generate_daily_report(sales_df, revenue_metrics, anomalies, **kwargs):
    """Функция, адаптированная для Airflow"""
    logger = logging.getLogger(__name__)
    logger.info("Starting generate_daily_report")
    try:
        """
        Генерация ежедневного отчета
        """
        logger.info("Генерация ежедневного отчета")

        report_date = datetime.now().strftime('%Y-%m-%d')

        # Базовая информация
        report = {
            'report_id': hashlib.md5(report_date.encode()).hexdigest()[:8],
            'report_date': report_date,
            'generation_timestamp': datetime.now().isoformat(),
            'data_summary': {
                'total_records': len(sales_df),
                'date_range': {
                    'min': sales_df['sale_date'].min().isoformat() if not sales_df.empty else None,
                    'max': sales_df['sale_date'].max().isoformat() if not sales_df.empty else None
                }
            },
            'revenue_summary': {
                'total_revenue': revenue_metrics.get('total_revenue', 0),
                'total_transactions': revenue_metrics.get('total_transactions', 0),
                'avg_per_transaction': revenue_metrics.get('avg_revenue_per_transaction', 0)
            },
            'top_performers': {
                'best_region': revenue_metrics.get('most_profitable_region'),
                'best_product': revenue_metrics.get('most_profitable_product')
            },
            'anomalies_detected': len(anomalies),
            'anomalies_details': anomalies
        }

        # Добавляем детализированные метрики если они есть
        if 'region_metrics' in revenue_metrics:
            report['region_details'] = revenue_metrics['region_metrics']

        if 'top_products' in revenue_metrics:
            report['product_details'] = revenue_metrics['top_products']

        # Сохранение отчета в файл
        reports_dir = Path("/tmp/sales_reports")
        reports_dir.mkdir(exist_ok=True)

        report_filename = reports_dir / f"sales_report_{report_date}.json"
        with open(report_filename, 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2, ensure_ascii=False)

        logger.info(f"Отчет сохранен в {report_filename}")
        return report
    except Exception as e:
        logger.error(f"Error in generate_daily_report: {e}")
        raise
    finally:
        logger.info("Completed generate_daily_report")

# Функция: send_report_to_storage
def send_report_to_storage(daily_report, **kwargs):
    """Функция, адаптированная для Airflow"""
    logger = logging.getLogger(__name__)
    logger.info("Starting send_report_to_storage")
    """
    Отправка отчета в хранилище (имитация)
    """
    logger.info("Отправка отчета в хранилище")

    try:
        # Имитация отправки в S3/GCS/БД
        report_date = daily_report['report_date']
        report_id = daily_report['report_id']

        # Здесь была бы реальная интеграция с хранилищем
        # Например: boto3 для S3, google.cloud.storage для GCS и т.д.

        storage_result = {
            'status': 'success',
            'timestamp': datetime.now().isoformat(),
            'report_id': report_id,
            'report_date': report_date,
            'storage_location': f"s3://analytics-bucket/sales_reports/{report_date}/{report_id}.json",
            'message': 'Отчет успешно отправлен в хранилище'
        }

        logger.info(storage_result['message'])
        return storage_result

    except Exception as e:
        logger.error(f"Ошибка при отправке отчета в хранилище: {e}")
        return {
            'status': 'error',
            'timestamp': datetime.now().isoformat(),
            'error': str(e),
            'message': 'Ошибка при отправке отчета в хранилище'
        }

# Функция: archive_old_reports
def archive_old_reports(days_to_keep: int = 30, **kwargs):
    """Функция, адаптированная для Airflow"""
    logger = logging.getLogger(__name__)
    logger.info("Starting archive_old_reports")
    """
    Архивация старых отчетов (имитация)
    """
    logger.info(f"Архивация отчетов старше {days_to_keep} дней")

    try:
        reports_dir = Path("/tmp/sales_reports")
        if not reports_dir.exists():
            logger.warning("Директория с отчетами не существует")
            return {'archived': 0, 'message': 'Нет отчетов для архивации'}

        cutoff_date = datetime.now() - timedelta(days=days_to_keep)
        archived_count = 0

        for report_file in reports_dir.glob("*.json"):
            # Извлекаем дату из имени файла
            filename_date_str = report_file.stem.replace('sales_report_', '')
            try:
                file_date = datetime.strptime(filename_date_str, '%Y-%m-%d')

                if file_date < cutoff_date:
                    # Имитация архивации
                    archive_path = reports_dir / "archive" / report_file.name
                    archive_path.parent.mkdir(exist_ok=True)

                    # В реальности: перемещение в архивное хранилище
                    # report_file.rename(archive_path)

                    archived_count += 1
                    logger.debug(f"Отчет {report_file.name} подготовлен к архивации")

            except ValueError:
                continue

        result = {
            'archived': archived_count,
            'cutoff_date': cutoff_date.strftime('%Y-%m-%d'),
            'message': f'Подготовлено к архивации {archived_count} отчетов'
        }

        logger.info(result['message'])
        return result

    except Exception as e:
        logger.error(f"Ошибка при архивации отчетов: {e}")
        return {
            'archived': 0,
            'error': str(e),
            'message': 'Ошибка при архивации отчетов'
        }

# Функция: daily_sales_pipeline
def daily_sales_pipeline(**kwargs):
    """Функция, адаптированная для Airflow"""
    logger = logging.getLogger(__name__)
    logger.info("Starting daily_sales_pipeline")
    try:
        """
        Основной пайплайн обработки ежедневных продаж
        """
        logger.info("Запуск ежедневного пайплайна продаж")

        # 1. Извлечение данных
        today = datetime.now().strftime('%Y-%m-%d')
        sales_data = extract_sales_data(today)

        # 2. Расчет метрик
        revenue_metrics = calculate_revenue_metrics(sales_data)

        # 3. Детекция аномалий
        anomalies = detect_anomalies(sales_data, revenue_metrics)

        # 4. Генерация отчета
        daily_report = generate_daily_report(sales_data, revenue_metrics, anomalies)

        # 5. Отправка в хранилище
        storage_result = send_report_to_storage(daily_report)

        # 6. Архивация старых отчетов (раз в неделю)
        if datetime.now().weekday() == 0:  # Понедельник
            archive_result = archive_old_reports(30)
        else:
            archive_result = {'skipped': 'Архивация выполняется только по понедельникам'}

        # Итоговый результат
        pipeline_result = {
            'pipeline_date': today,
            'status': 'completed',
            'steps': {
                'extraction': {'records': len(sales_data)},
                'metrics_calculation': {'metrics_count': len(revenue_metrics)},
                'anomaly_detection': {'anomalies_found': len(anomalies)},
                'report_generation': {'report_id': daily_report['report_id']},
                'storage_upload': storage_result,
                'archiving': archive_result
            },
            'timestamp': datetime.now().isoformat()
        }

        logger.info(f"Пайплайн завершен: {pipeline_result['status']}")
        return pipeline_result
    except Exception as e:
        logger.error(f"Error in daily_sales_pipeline: {e}")
        raise
    finally:
        logger.info("Completed daily_sales_pipeline")

with DAG(
    dag_id="generated_pipeline_generate_reports",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["data_processing", "etl", "analytics"],
    description="Ежедневная обработка данных аналитики"
) as dag:

    # Python задачи:
    execute_daily_sales_pipeline = PythonOperator(
        task_id="execute_daily_sales_pipeline",
        python_callable=daily_sales_pipeline,
        doc_md="""Основной пайплайн обработки ежедневных продаж""",
    )