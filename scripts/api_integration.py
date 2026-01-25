# DAG_SCHEDULE_INTERVAL: @hourly
# DAG_TAGS: ['api', 'integration', 'rest']
# DAG_OWNER: backend
# DAG_RETRIES: 3
# DAG_RETRY_DELAY: 5
import numpy as np
import logging
from datetime import datetime
import json
from pathlib import Path

logger = logging.getLogger(__name__)


def fetch_weather_data():
    """Получение данных о погоде"""
    logger.info("Получение данных о погоде...")

    # Пример API (можно заменить на реальное)
    # Для демонстрации используем открытое API
    cities = ['Moscow', 'Saint Petersburg', 'Kazan', 'Novosibirsk']

    weather_data = []

    for city in cities:
        try:
            # Используем openweathermap или mock данные
            # В реальном проекте здесь был бы реальный API ключ
            mock_response = {
                'city': city,
                'temperature': round(np.random.uniform(-10, 25), 1),
                'humidity': np.random.randint(30, 90),
                'pressure': np.random.randint(980, 1030),
                'description': np.random.choice(['clear', 'cloudy', 'rain', 'snow']),
                'timestamp': datetime.now().isoformat()
            }

            weather_data.append(mock_response)
            logger.info(f"Погода в {city}: {mock_response['temperature']}°C")

        except Exception as e:
            logger.error(f"Ошибка при получении погоды для {city}: {e}")

    return weather_data


def fetch_currency_rates():
    """Получение курсов валют"""
    logger.info("Получение курсов валют...")

    try:
        # Пример: получение курсов ЦБ РФ
        # В реальном проекте здесь был бы реальный API
        currencies = ['USD', 'EUR', 'GBP', 'CNY']

        currency_rates = {}
        for currency in currencies:
            # Mock данные
            if currency == 'USD':
                rate = round(np.random.uniform(85, 95), 2)
            elif currency == 'EUR':
                rate = round(np.random.uniform(90, 100), 2)
            elif currency == 'GBP':
                rate = round(np.random.uniform(105, 115), 2)
            else:
                rate = round(np.random.uniform(11, 13), 2)

            currency_rates[currency] = {
                'rate': rate,
                'change': round(np.random.uniform(-1, 1), 2),
                'timestamp': datetime.now().isoformat()
            }

        logger.info(f"Курсы валют: {currency_rates}")
        return currency_rates

    except Exception as e:
        logger.error(f"Ошибка при получении курсов валют: {e}")
        return {}


def fetch_stock_data():
    """Получение данных об акциях"""
    logger.info("Получение данных об акциях...")

    # Mock данные для демонстрации
    stocks = ['GAZP', 'SBER', 'LKOH', 'YNDX']

    stock_data = {}
    for stock in stocks:
        stock_data[stock] = {
            'price': round(np.random.uniform(100, 5000), 2),
            'change_percent': round(np.random.uniform(-5, 5), 2),
            'volume': np.random.randint(100000, 10000000),
            'timestamp': datetime.now().isoformat()
        }

    logger.info(f"Данные об акциях: {stock_data}")
    return stock_data


def save_api_data():
    """Сохранение данных API"""
    logger.info("Сохранение данных API...")

    # Собираем данные из разных источников
    data = {
        'timestamp': datetime.now().isoformat(),
        'weather': fetch_weather_data(),
        'currency': fetch_currency_rates(),
        'stocks': fetch_stock_data()
    }

    # Сохраняем в файл
    output_dir = Path("/tmp/api_data")
    output_dir.mkdir(exist_ok=True)

    filename = output_dir / f"api_data_{datetime.now().strftime('%Y%m%d_%H%M')}.json"

    with open(filename, 'w') as f:
        json.dump(data, f, indent=2, default=str)

    logger.info(f"Данные API сохранены в {filename}")

    # Также сохраняем отдельные файлы для каждой категории
    for category in ['weather', 'currency', 'stocks']:
        category_file = output_dir / f"{category}_{datetime.now().strftime('%Y%m%d')}.json"
        with open(category_file, 'w') as f:
            json.dump(data[category], f, indent=2, default=str)

    return data


def send_data_to_database():
    """Отправка данных в базу данных"""
    logger.info("Отправка данных в базу данных...")

    data = save_api_data()

    # Здесь была бы реальная интеграция с базой данных
    # Например: psycopg2 для PostgreSQL, pymysql для MySQL и т.д.

    # Для демонстрации просто логируем
    logger.info(f"Данные для отправки в БД:")
    logger.info(f"  - Погода: {len(data['weather'])} городов")
    logger.info(f"  - Валюты: {len(data['currency'])} валют")
    logger.info(f"  - Акции: {len(data['stocks'])} компаний")

    # Mock: считаем что отправка прошла успешно
    db_result = {
        'status': 'success',
        'timestamp': datetime.now().isoformat(),
        'records_inserted': len(data['weather']) + len(data['currency']) + len(data['stocks']),
        'message': 'Данные успешно отправлены в базу данных'
    }

    logger.info(db_result['message'])
    return db_result