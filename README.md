# Реализация генератора DAGs
## Задание:
Необходимо реализовать локальную программу, которая будет генерировать даги, просматривая папку с исходниками. </br>
Количество файлов исходников - один даг. </br>
### Описание задания:
1. Функционал не ограничен. </br>
2. Проект должен быть настроен под конфигурацию докера для задания. Не стоит делать кластерное решение. </br>
3. Дата старта **DAG** - день генерации скрипта. Время и частота запуска, должны задаваться в исходнике! Название любое... </br>
4. Настроить деление на таски, чем больше, тем лучше. Несколько **Python** функций - несколько тасок (определяем по ключевым словам **def**). Есть список **DDL** команд - разделяем на таски. </br>
5. Реализация должна работать и для **Python** скриптов и для **SQL** скриптов. </br>
6. Параметры подключения в **DAG** лежать не должны. </br>

## Реализация:
### Общее описание
**Генератор DAG** - это инструмент для автоматического создания **Airflow DAG** из **Python** и **SQL** файлов. </br>
Он анализирует исходные файлы, извлекает **Python** функции или **SQL** команды и создает готовые к использованию **DAG** файлы.</br>

### Архитектура
Генератор состоит из 3 модулей:</br>
- `dag_generator.py` - основной модуль, который координирует всю работу.</br>
- `python_parser.py` - парсит **Python** файлы, извлекает функции.</br>
- `sql_parser.py` - парсит **SQL** файлы, извлекает команды.</br>

### Как работает генератор
1. <ins>Описание директорий</ins>:
- `/opt/airflow/scripts/`  - здесь размещаются исходные файлы.</br>
- `/opt/airflow/dags/`     - сюда попадают сгенерированные **DAG**.</br>

2. <ins>Парсинг файлов</ins>:
- Для **.py** файлов - извлекаются функции, анализируются зависимости.</br>
- Для **.sql** файлов - последовательность команд разбивается на отдельные блоки.</br>

3. <ins>Генерация **DAG**</ins>:
- Создаются уникальные **task_id** для каждой задачи.</br>
- Адаптируются функции для работы в **Airflow**.</br>
- Добавляется логирование и обработа ошибок.</br>
- Создаются зависимости между задачами, если это возможно.</br>

4. <ins>Сохранение **DAG**</ins>:
- **DAG** сохраняются в `/opt/airflow/dags/`</br>
- Имя файла выглядит следующим образом: `generated_<имя_исходного_файла>.py`</br>

### Создание исходных файлов (эти примеры есть в папке scripts, а даги по ни сформированы в dags)
<ins>**Для Python файлов**</ins>
1. Базовый пример (независимые функции)
```
# DAG: data_pipeline
# schedule: @daily
# owner: data_team
# tags: [etl, data]
# description: Daily data processing pipeline

def extract_data():
    """Извлечение данных из источника"""
    print("Extracting data from source")

def transform_data():
    """Преобразование данных"""
    print("Transforming data")

def load_data():
    """Загрузка данных в хранилище"""
    print("Loading data to warehouse")
```
- Результат: 3 отдельные задачи в **DAG**

2. Если в файле присутствует сложная функция, тогда формируется одна задача
```
# DAG: complex_pipeline
# schedule: @once

def main_pipeline(**kwargs):
    """
    Главный пайплайн обработки данных
    """
    # Функция вызывает другие функции
    data = extract()
    processed = transform(data)
    load(processed)

def extract():
    return "raw_data"

def transform(data):
    return f"processed_{data}"

def load(data):
    print(f"Loaded: {data}")
```
- Результат: 1 задача (т.к. функции вызывают друг друга)

<ins>**Для SQL файлов**</ins>
1. Базовый пример
```
-- DAG: setup_database_simple
-- schedule: @once
-- owner: dba
-- tags: [setup, database]
-- description: Настройка базы данных

-- Создание таблиц
DROP TABLE IF EXISTS users_main;
CREATE TABLE IF NOT EXISTS users_main (
    id SERIAL PRIMARY KEY,
    username VARCHAR(50) UNIQUE NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

DROP TABLE IF EXISTS orders_main;
CREATE TABLE IF NOT EXISTS orders_main (
    id SERIAL PRIMARY KEY,
    user_id INT REFERENCES users(id),
    amount DECIMAL(10,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Создание индексов
CREATE INDEX idx_orders_main_user_id ON orders_main(user_id);
CREATE INDEX idx_orders_main_created_at ON orders_main(created_at);

-- Начальные данные
INSERT INTO users_main (username) VALUES ('admin'), ('user1');
```
- Результат: 7 отдельных задач (по одной на каждую **SQL** команду)

2. Сложный **SQL** с **CTE**
```
-- DAG: daily_sales_report
-- schedule: 0 1 * * *
-- owner: analytics
-- tags: [reporting, analytics, sales]
-- description: Ежедневный отчет по продажам
-- retries: 2
-- retry_delay: 300

-- Создаем таблицу заказов (orders)
DROP TABLE IF EXISTS orders_info;
CREATE TABLE IF NOT EXISTS orders_info (
    id SERIAL PRIMARY KEY,
    user_id INT NOT NULL,
    amount DECIMAL(10,2) NOT NULL,
    status VARCHAR(20) DEFAULT 'completed',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Создаем таблицу для ежедневных отчетов (daily_reports)
DROP TABLE IF EXISTS daily_reports;
CREATE TABLE IF NOT EXISTS daily_reports (
    id SERIAL PRIMARY KEY,
    report_date DATE NOT NULL UNIQUE,
    total_orders INT NOT NULL,
    total_revenue DECIMAL(12,2) NOT NULL,
    average_order_value DECIMAL(10,2),
    generated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Вставляем тестовые данные за последние 3 дня
-- Вчерашние заказы (для отчета)
INSERT INTO orders_info (user_id, amount, created_at) VALUES
(1, 100.50, CURRENT_DATE - INTERVAL '1 day' + INTERVAL '10:30'),
(2, 75.25, CURRENT_DATE - INTERVAL '1 day' + INTERVAL '14:15'),
(3, 200.00, CURRENT_DATE - INTERVAL '1 day' + INTERVAL '16:45'),
(1, 50.00, CURRENT_DATE - INTERVAL '1 day' + INTERVAL '20:10'),
(4, 300.75, CURRENT_DATE - INTERVAL '1 day' + INTERVAL '22:30');

-- Позавчерашние заказы (для проверки where условия)
INSERT INTO orders_info (user_id, amount, created_at) VALUES
(5, 125.00, CURRENT_DATE - INTERVAL '2 days' + INTERVAL '09:00'),
(2, 80.50, CURRENT_DATE - INTERVAL '2 days' + INTERVAL '15:20');

-- Сегодняшние заказы (будут в следующем отчете)
INSERT INTO orders_info (user_id, amount, created_at) VALUES
(3, 150.00, CURRENT_DATE + INTERVAL '8 hours'),
(6, 95.30, CURRENT_DATE + INTERVAL '12 hours');

-- Создаем индекс для ускорения запросов по дате
CREATE INDEX IF NOT EXISTS idx_orders_info_created_at ON orders_info(created_at);
CREATE INDEX IF NOT EXISTS idx_orders_info_user_created ON orders_info(user_id, created_at);

-- Основной запрос для генерации ежедневного отчета
WITH daily_stats AS (
    SELECT
        DATE(created_at) as report_date,
        COUNT(*) as total_orders,
        SUM(amount) as total_revenue
    FROM orders_info
    WHERE created_at >= CURRENT_DATE - INTERVAL '1 day'
    GROUP BY DATE(created_at)
)
INSERT INTO daily_reports (report_date, total_orders, total_revenue)
SELECT
    report_date,
    total_orders,
    total_revenue
FROM daily_stats
ON CONFLICT (report_date)
DO UPDATE SET
    total_orders = EXCLUDED.total_orders,
    total_revenue = EXCLUDED.total_revenue,
    generated_at = CURRENT_TIMESTAMP;

-- Дополнительный запрос для обновления средней стоимости заказа
UPDATE daily_reports
SET average_order_value = total_revenue / NULLIF(total_orders, 0)
WHERE report_date = CURRENT_DATE - INTERVAL '1 day';
```
- Результат: 11 отдельных задач (по одной на каждую **SQL** команду)

### Параметры DAG в комментариях
1. <ins>Общие параметры (работают для `.py` и `.sql`)</ins>
```
# DAG: my_dag_name           # Имя DAG, если нет, возьмет название файла с префиксом generated_ 
# schedule: @daily           # Расписание
# schedule_interval: 0 2 * * *  # Альтернатива schedule (минуты, часы, день месяца, месяц, день недели)
# owner: data_team           # Владелец
# tags: [etl, data]          # Теги (список)
# description: Мой DAG       # Описание
# catchup: false             # Отключить catchup
# retries: 3                 # Количество повторений
# retry_delay: 300           # Задержка между повторениями (сек)
# start_date: 2024-01-01     # Дата начала, если не установлена, возьмет текущую дату
# max_active_runs: 1         # Максимум одновременных запусков
```
2. <ins>Параметры только для **SQL** файлов</ins>
```
-- Для SQL файлов используйте двойной дефис в качестве комментариев
-- DAG: sql_pipeline
-- schedule: @hourly
-- owner: dba
```
### Правила именования
1. <ins>Для Python функций:</ins></br>
- Используйте осмысленные имена: _extract_data_, _transform_records_.</br>
- Избегайте специальных символов.</br>
- Документируйте функции _docstring_.</br>

2. <ins>Для SQL команд:</ins></br>
- Каждая команда должна заканчиваться `;`. </br>
- Команды на отдельных строках для лучшего парсинга. </br>
- Используйте комментарии для разделения логических блоков. </br>

### Стратегии генерации для Python файлов
1. <ins>Когда создаются отдельные задачи:</ins> </br>
- Независимые функции без параметров. </br>
- Нет вызовов других функций из того же файла. </br>
- Более одной функции в файле.</br>
   
2. <ins>Когда создается одна задача:</ins> </br>
- Только одна функция в файле. </br>
- Функции имеют параметры. </br>
- Функции вызывают друг друга. </br>
- Найдена функция с _pipeline_ в имени. </br>

### Особенности для SQL файлов
1. <ins>Автоматически определяются типы команд:</ins> </br>
- `CREATE TABLE, DROP TABLE` </br>
- `CREATE INDEX, DROP INDEX` </br>
- `INSERT, UPDATE, DELETE` </br>
- `SELECT, WITH (CTE)` </br>

2. <ins>Под них создаются уникальные имена задач:</ins> </br>
- `create_table_users_1`</br> 
- `insert_into_orders_2`</br>
- `create_index_idx_users_email_3`</br>

### Зависимости:
Все **SQL** задачи выполняются последовательно.</br>
Порядок соответствует порядку в исходном файле.</br>

### Запуск генератора
1. `git clone git@github.com:MikhalevaAnna/DE_Airflow_project.git`
2. `cd DE_Airflow_project`
3. `docker-compose up -d`
4. Создаем подключение в **Airflow** с параметрами </br>
   (Все настройки подключения прописаны в файле `docker-compose.yml`):
```
Connection Id:    postgres_default
Connection Type:  Postgres
Host:             pg_extra
Schema:           postgres
Login:            extradb_user
Port:             5432
```
5. По умолчанию используется подключение с именем `postgres_default`.
6. После клонирования очищаем папку **dags**.
7. Запускаем генератор в контейнере `Airflow`:</br>
`docker exec -it airflow-scheduler python /opt/airflow/generators/dag_generator.py`.


### Примеры использования 
1. Примеры **Python** и **SQl** скриптов, для которых нужно сформировать **Airflow DAG** находятся в папке `scripts`.
2. **Airflow DAG**, сформированные по этим скриптам, с помощью генератора, находятся в папке `dags`.
   
### Ограничения
1. <ins>Что поддерживается для SQL</ins>:
- Стандартные **SQL** команды.
- Многострочные выражения.
- **CTE** (**WITH** выражения).
- Комментарии `--` и `/* */`.
  
2. <ins>Не поддерживаются</ins>:
- Хранимые процедуры.
- Динамический **SQL** с переменными.
- Нестандартные диалекты **SQL**.

### Отладка
1. <ins>Если **DAG** не генерируется</ins>:
- Проверьте логи генератора.
- Убедитесь в правильном формате файла.
- Проверьте наличие функций/**SQL** команд.
- Убедитесь в правильности написания комментариев для **Python** (#) и **SQl** (--) файлов.

2. <ins>Если задачи не выполняются</ins>:
- Проверьте логи **Airflow**.
- Убедитесь в доступности подключений.
- Проверьте права доступа.

