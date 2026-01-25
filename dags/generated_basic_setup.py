"""
Auto-generated DAG from basic_setup.sql
Generated: 2026-01-25 18:03:57
Total tasks: 30
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable

# Default arguments for the DAG
default_args = {
    "owner": "analytics_team",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(seconds=5),
}

# Функции для выполнения SQL команд
def get_postgres_connection_id():
    """
    Получает ID соединения из переменных Airflow
    Порядок приоритета:
    1. Переменная: airflow_db_conn_id (из DAG конфигурации)
    2. Значение по умолчанию из переменной Airflow
    """
    import os
    from airflow.models import Variable
    
    conn_id = os.environ.get("AIRFLOW_DB_CONN_ID")
    if conn_id:
        return conn_id
    
    try:
        conn_id = Variable.get("airflow_db_conn_id", default_var=None)
        if conn_id:
            return conn_id
    except:
        pass
    
    return os.environ.get("POSTGRES_CONN_ID", "postgres_default")

def sql_fn_drop_table_sales_by_category_1_485d0f(**kwargs):
    """Execute DROP TABLE command 1"""
    logger = logging.getLogger(__name__)
    logger.info("Starting drop_table_sales_by_category_1")
    try:
        conn_id = get_postgres_connection_id()
        logger.info(f"Using PostgreSQL connection ID: {conn_id}")
        
        hook = PostgresHook(postgres_conn_id=conn_id)
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""DROP TABLE IF EXISTS sales_by_category CASCADE""")
            conn.commit()
            logger.info(f"Successfully executed: DROP TABLE")
        except Exception as e:
            conn.rollback()
            logger.error(f"Error executing DROP TABLE: {e}")
            raise
        finally:
            cursor.close()
            conn.close()
    except Exception as e:
        logger.error(f"Error in drop_table_sales_by_category_1: {e}")
        raise
    finally:
        logger.info("Completed drop_table_sales_by_category_1")

def sql_fn_drop_table_demand_forecast_2_0e2deb(**kwargs):
    """Execute DROP TABLE command 2"""
    logger = logging.getLogger(__name__)
    logger.info("Starting drop_table_demand_forecast_2")
    try:
        conn_id = get_postgres_connection_id()
        logger.info(f"Using PostgreSQL connection ID: {conn_id}")
        
        hook = PostgresHook(postgres_conn_id=conn_id)
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""DROP TABLE IF EXISTS demand_forecast CASCADE""")
            conn.commit()
            logger.info(f"Successfully executed: DROP TABLE")
        except Exception as e:
            conn.rollback()
            logger.error(f"Error executing DROP TABLE: {e}")
            raise
        finally:
            cursor.close()
            conn.close()
    except Exception as e:
        logger.error(f"Error in drop_table_demand_forecast_2: {e}")
        raise
    finally:
        logger.info("Completed drop_table_demand_forecast_2")

def sql_fn_drop_table_customer_analytics_3_22f585(**kwargs):
    """Execute DROP TABLE command 3"""
    logger = logging.getLogger(__name__)
    logger.info("Starting drop_table_customer_analytics_3")
    try:
        conn_id = get_postgres_connection_id()
        logger.info(f"Using PostgreSQL connection ID: {conn_id}")
        
        hook = PostgresHook(postgres_conn_id=conn_id)
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""DROP TABLE IF EXISTS customer_analytics CASCADE""")
            conn.commit()
            logger.info(f"Successfully executed: DROP TABLE")
        except Exception as e:
            conn.rollback()
            logger.error(f"Error executing DROP TABLE: {e}")
            raise
        finally:
            cursor.close()
            conn.close()
    except Exception as e:
        logger.error(f"Error in drop_table_customer_analytics_3: {e}")
        raise
    finally:
        logger.info("Completed drop_table_customer_analytics_3")

def sql_fn_drop_table_transactions_4_b91229(**kwargs):
    """Execute DROP TABLE command 4"""
    logger = logging.getLogger(__name__)
    logger.info("Starting drop_table_transactions_4")
    try:
        conn_id = get_postgres_connection_id()
        logger.info(f"Using PostgreSQL connection ID: {conn_id}")
        
        hook = PostgresHook(postgres_conn_id=conn_id)
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""DROP TABLE IF EXISTS transactions CASCADE""")
            conn.commit()
            logger.info(f"Successfully executed: DROP TABLE")
        except Exception as e:
            conn.rollback()
            logger.error(f"Error executing DROP TABLE: {e}")
            raise
        finally:
            cursor.close()
            conn.close()
    except Exception as e:
        logger.error(f"Error in drop_table_transactions_4: {e}")
        raise
    finally:
        logger.info("Completed drop_table_transactions_4")

def sql_fn_drop_table_products_5_503d97(**kwargs):
    """Execute DROP TABLE command 5"""
    logger = logging.getLogger(__name__)
    logger.info("Starting drop_table_products_5")
    try:
        conn_id = get_postgres_connection_id()
        logger.info(f"Using PostgreSQL connection ID: {conn_id}")
        
        hook = PostgresHook(postgres_conn_id=conn_id)
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""DROP TABLE IF EXISTS products CASCADE""")
            conn.commit()
            logger.info(f"Successfully executed: DROP TABLE")
        except Exception as e:
            conn.rollback()
            logger.error(f"Error executing DROP TABLE: {e}")
            raise
        finally:
            cursor.close()
            conn.close()
    except Exception as e:
        logger.error(f"Error in drop_table_products_5: {e}")
        raise
    finally:
        logger.info("Completed drop_table_products_5")

def sql_fn_drop_table_customers_6_68d1ee(**kwargs):
    """Execute DROP TABLE command 6"""
    logger = logging.getLogger(__name__)
    logger.info("Starting drop_table_customers_6")
    try:
        conn_id = get_postgres_connection_id()
        logger.info(f"Using PostgreSQL connection ID: {conn_id}")
        
        hook = PostgresHook(postgres_conn_id=conn_id)
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""DROP TABLE IF EXISTS customers CASCADE""")
            conn.commit()
            logger.info(f"Successfully executed: DROP TABLE")
        except Exception as e:
            conn.rollback()
            logger.error(f"Error executing DROP TABLE: {e}")
            raise
        finally:
            cursor.close()
            conn.close()
    except Exception as e:
        logger.error(f"Error in drop_table_customers_6: {e}")
        raise
    finally:
        logger.info("Completed drop_table_customers_6")

def sql_fn_create_table_customers_7_5db892(**kwargs):
    """Execute CREATE TABLE command 7"""
    logger = logging.getLogger(__name__)
    logger.info("Starting create_table_customers_7")
    try:
        conn_id = get_postgres_connection_id()
        logger.info(f"Using PostgreSQL connection ID: {conn_id}")
        
        hook = PostgresHook(postgres_conn_id=conn_id)
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""CREATE TABLE IF NOT EXISTS customers (
    customer_id SERIAL PRIMARY KEY,
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL,
    email VARCHAR(100) NOT NULL UNIQUE,
    phone VARCHAR(20),
    registration_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_activity_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    is_active BOOLEAN DEFAULT TRUE
)""")
            conn.commit()
            logger.info(f"Successfully executed: CREATE TABLE")
        except Exception as e:
            conn.rollback()
            logger.error(f"Error executing CREATE TABLE: {e}")
            raise
        finally:
            cursor.close()
            conn.close()
    except Exception as e:
        logger.error(f"Error in create_table_customers_7: {e}")
        raise
    finally:
        logger.info("Completed create_table_customers_7")

def sql_fn_create_table_products_8_57fad7(**kwargs):
    """Execute CREATE TABLE command 8"""
    logger = logging.getLogger(__name__)
    logger.info("Starting create_table_products_8")
    try:
        conn_id = get_postgres_connection_id()
        logger.info(f"Using PostgreSQL connection ID: {conn_id}")
        
        hook = PostgresHook(postgres_conn_id=conn_id)
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""CREATE TABLE IF NOT EXISTS products (
    product_id SERIAL PRIMARY KEY,
    product_name VARCHAR(200) NOT NULL,
    category VARCHAR(100),
    supplier_id INTEGER,
    price DECIMAL(10,2) NOT NULL CHECK (price >= 0),
    cost DECIMAL(10,2) NOT NULL CHECK (cost >= 0),
    stock_quantity INTEGER DEFAULT 0 CHECK (stock_quantity >= 0),
    reorder_level INTEGER DEFAULT 10,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)""")
            conn.commit()
            logger.info(f"Successfully executed: CREATE TABLE")
        except Exception as e:
            conn.rollback()
            logger.error(f"Error executing CREATE TABLE: {e}")
            raise
        finally:
            cursor.close()
            conn.close()
    except Exception as e:
        logger.error(f"Error in create_table_products_8: {e}")
        raise
    finally:
        logger.info("Completed create_table_products_8")

def sql_fn_create_table_transactions_9_245717(**kwargs):
    """Execute CREATE TABLE command 9"""
    logger = logging.getLogger(__name__)
    logger.info("Starting create_table_transactions_9")
    try:
        conn_id = get_postgres_connection_id()
        logger.info(f"Using PostgreSQL connection ID: {conn_id}")
        
        hook = PostgresHook(postgres_conn_id=conn_id)
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""CREATE TABLE IF NOT EXISTS transactions (
    transaction_id SERIAL PRIMARY KEY,
    customer_id INTEGER NOT NULL REFERENCES customers(customer_id) ON DELETE RESTRICT,
    amount DECIMAL(12,2) NOT NULL CHECK (amount > 0),
    currency VARCHAR(3) DEFAULT 'USD',
    transaction_type VARCHAR(20) CHECK (transaction_type IN ('purchase', 'refund', 'deposit', 'withdrawal')),
    status VARCHAR(20) DEFAULT 'completed',
    transaction_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    description TEXT
)""")
            conn.commit()
            logger.info(f"Successfully executed: CREATE TABLE")
        except Exception as e:
            conn.rollback()
            logger.error(f"Error executing CREATE TABLE: {e}")
            raise
        finally:
            cursor.close()
            conn.close()
    except Exception as e:
        logger.error(f"Error in create_table_transactions_9: {e}")
        raise
    finally:
        logger.info("Completed create_table_transactions_9")

def sql_fn_create_index_idx_customers_email_10_b9df35(**kwargs):
    """Execute CREATE INDEX command 10"""
    logger = logging.getLogger(__name__)
    logger.info("Starting create_index_idx_customers_email_10")
    try:
        conn_id = get_postgres_connection_id()
        logger.info(f"Using PostgreSQL connection ID: {conn_id}")
        
        hook = PostgresHook(postgres_conn_id=conn_id)
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""CREATE INDEX IF NOT EXISTS idx_customers_email ON customers(email)""")
            conn.commit()
            logger.info(f"Successfully executed: CREATE INDEX")
        except Exception as e:
            conn.rollback()
            logger.error(f"Error executing CREATE INDEX: {e}")
            raise
        finally:
            cursor.close()
            conn.close()
    except Exception as e:
        logger.error(f"Error in create_index_idx_customers_email_10: {e}")
        raise
    finally:
        logger.info("Completed create_index_idx_customers_email_10")

def sql_fn_create_index_idx_customers_last_name_11_cfef34(**kwargs):
    """Execute CREATE INDEX command 11"""
    logger = logging.getLogger(__name__)
    logger.info("Starting create_index_idx_customers_last_name_11")
    try:
        conn_id = get_postgres_connection_id()
        logger.info(f"Using PostgreSQL connection ID: {conn_id}")
        
        hook = PostgresHook(postgres_conn_id=conn_id)
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""CREATE INDEX IF NOT EXISTS idx_customers_last_name ON customers(last_name)""")
            conn.commit()
            logger.info(f"Successfully executed: CREATE INDEX")
        except Exception as e:
            conn.rollback()
            logger.error(f"Error executing CREATE INDEX: {e}")
            raise
        finally:
            cursor.close()
            conn.close()
    except Exception as e:
        logger.error(f"Error in create_index_idx_customers_last_name_11: {e}")
        raise
    finally:
        logger.info("Completed create_index_idx_customers_last_name_11")

def sql_fn_create_index_idx_transactions_customer_id_12_e088d3(**kwargs):
    """Execute CREATE INDEX command 12"""
    logger = logging.getLogger(__name__)
    logger.info("Starting create_index_idx_transactions_customer_id_12")
    try:
        conn_id = get_postgres_connection_id()
        logger.info(f"Using PostgreSQL connection ID: {conn_id}")
        
        hook = PostgresHook(postgres_conn_id=conn_id)
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""CREATE INDEX IF NOT EXISTS idx_transactions_customer_id ON transactions(customer_id)""")
            conn.commit()
            logger.info(f"Successfully executed: CREATE INDEX")
        except Exception as e:
            conn.rollback()
            logger.error(f"Error executing CREATE INDEX: {e}")
            raise
        finally:
            cursor.close()
            conn.close()
    except Exception as e:
        logger.error(f"Error in create_index_idx_transactions_customer_id_12: {e}")
        raise
    finally:
        logger.info("Completed create_index_idx_transactions_customer_id_12")

def sql_fn_create_index_idx_transactions_date_13_47eaab(**kwargs):
    """Execute CREATE INDEX command 13"""
    logger = logging.getLogger(__name__)
    logger.info("Starting create_index_idx_transactions_date_13")
    try:
        conn_id = get_postgres_connection_id()
        logger.info(f"Using PostgreSQL connection ID: {conn_id}")
        
        hook = PostgresHook(postgres_conn_id=conn_id)
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""CREATE INDEX IF NOT EXISTS idx_transactions_date ON transactions(transaction_date)""")
            conn.commit()
            logger.info(f"Successfully executed: CREATE INDEX")
        except Exception as e:
            conn.rollback()
            logger.error(f"Error executing CREATE INDEX: {e}")
            raise
        finally:
            cursor.close()
            conn.close()
    except Exception as e:
        logger.error(f"Error in create_index_idx_transactions_date_13: {e}")
        raise
    finally:
        logger.info("Completed create_index_idx_transactions_date_13")

def sql_fn_create_index_idx_transactions_type_status_14_f8c78d(**kwargs):
    """Execute CREATE INDEX command 14"""
    logger = logging.getLogger(__name__)
    logger.info("Starting create_index_idx_transactions_type_status_14")
    try:
        conn_id = get_postgres_connection_id()
        logger.info(f"Using PostgreSQL connection ID: {conn_id}")
        
        hook = PostgresHook(postgres_conn_id=conn_id)
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""CREATE INDEX IF NOT EXISTS idx_transactions_type_status ON transactions(transaction_type, status)""")
            conn.commit()
            logger.info(f"Successfully executed: CREATE INDEX")
        except Exception as e:
            conn.rollback()
            logger.error(f"Error executing CREATE INDEX: {e}")
            raise
        finally:
            cursor.close()
            conn.close()
    except Exception as e:
        logger.error(f"Error in create_index_idx_transactions_type_status_14: {e}")
        raise
    finally:
        logger.info("Completed create_index_idx_transactions_type_status_14")

def sql_fn_create_index_idx_products_category_15_3b4c37(**kwargs):
    """Execute CREATE INDEX command 15"""
    logger = logging.getLogger(__name__)
    logger.info("Starting create_index_idx_products_category_15")
    try:
        conn_id = get_postgres_connection_id()
        logger.info(f"Using PostgreSQL connection ID: {conn_id}")
        
        hook = PostgresHook(postgres_conn_id=conn_id)
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""CREATE INDEX IF NOT EXISTS idx_products_category ON products(category)""")
            conn.commit()
            logger.info(f"Successfully executed: CREATE INDEX")
        except Exception as e:
            conn.rollback()
            logger.error(f"Error executing CREATE INDEX: {e}")
            raise
        finally:
            cursor.close()
            conn.close()
    except Exception as e:
        logger.error(f"Error in create_index_idx_products_category_15: {e}")
        raise
    finally:
        logger.info("Completed create_index_idx_products_category_15")

def sql_fn_create_index_idx_products_supplier_16_52e296(**kwargs):
    """Execute CREATE INDEX command 16"""
    logger = logging.getLogger(__name__)
    logger.info("Starting create_index_idx_products_supplier_16")
    try:
        conn_id = get_postgres_connection_id()
        logger.info(f"Using PostgreSQL connection ID: {conn_id}")
        
        hook = PostgresHook(postgres_conn_id=conn_id)
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""CREATE INDEX IF NOT EXISTS idx_products_supplier ON products(supplier_id)""")
            conn.commit()
            logger.info(f"Successfully executed: CREATE INDEX")
        except Exception as e:
            conn.rollback()
            logger.error(f"Error executing CREATE INDEX: {e}")
            raise
        finally:
            cursor.close()
            conn.close()
    except Exception as e:
        logger.error(f"Error in create_index_idx_products_supplier_16: {e}")
        raise
    finally:
        logger.info("Completed create_index_idx_products_supplier_16")

def sql_fn_truncate_17_16e3e8(**kwargs):
    """Execute TRUNCATE command 17"""
    logger = logging.getLogger(__name__)
    logger.info("Starting truncate_17")
    try:
        conn_id = get_postgres_connection_id()
        logger.info(f"Using PostgreSQL connection ID: {conn_id}")
        
        hook = PostgresHook(postgres_conn_id=conn_id)
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""TRUNCATE TABLE transactions, products, customers CASCADE""")
            conn.commit()
            logger.info(f"Successfully executed: TRUNCATE")
        except Exception as e:
            conn.rollback()
            logger.error(f"Error executing TRUNCATE: {e}")
            raise
        finally:
            cursor.close()
            conn.close()
    except Exception as e:
        logger.error(f"Error in truncate_17: {e}")
        raise
    finally:
        logger.info("Completed truncate_17")

def sql_fn_alter_sequence_18_7dbdf8(**kwargs):
    """Execute ALTER SEQUENCE command 18"""
    logger = logging.getLogger(__name__)
    logger.info("Starting alter_sequence_18")
    try:
        conn_id = get_postgres_connection_id()
        logger.info(f"Using PostgreSQL connection ID: {conn_id}")
        
        hook = PostgresHook(postgres_conn_id=conn_id)
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""ALTER SEQUENCE IF EXISTS customers_customer_id_seq RESTART WITH 1""")
            conn.commit()
            logger.info(f"Successfully executed: ALTER SEQUENCE")
        except Exception as e:
            conn.rollback()
            logger.error(f"Error executing ALTER SEQUENCE: {e}")
            raise
        finally:
            cursor.close()
            conn.close()
    except Exception as e:
        logger.error(f"Error in alter_sequence_18: {e}")
        raise
    finally:
        logger.info("Completed alter_sequence_18")

def sql_fn_insert_customers_19_ae52cd(**kwargs):
    """Execute INSERT command 19"""
    logger = logging.getLogger(__name__)
    logger.info("Starting insert_customers_19")
    try:
        conn_id = get_postgres_connection_id()
        logger.info(f"Using PostgreSQL connection ID: {conn_id}")
        
        hook = PostgresHook(postgres_conn_id=conn_id)
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""INSERT INTO customers (first_name, last_name, email, phone) VALUES
('Иван', 'Иванов', 'ivan.ivanov@example.com', '+79161234567'),
('Мария', 'Петрова', 'maria.petrova@example.com', '+79162345678'),
('Алексей', 'Сидоров', 'alexey.sidorov@example.com', '+79163456789'),
('Елена', 'Кузнецова', 'elena.kuznetsova@example.com', '+79164567890'),
('Дмитрий', 'Смирнов', 'dmitry.smirnov@example.com', '+79165678901')
ON CONFLICT (email) DO NOTHING""")
            conn.commit()
            logger.info(f"Successfully executed: INSERT")
        except Exception as e:
            conn.rollback()
            logger.error(f"Error executing INSERT: {e}")
            raise
        finally:
            cursor.close()
            conn.close()
    except Exception as e:
        logger.error(f"Error in insert_customers_19: {e}")
        raise
    finally:
        logger.info("Completed insert_customers_19")

def sql_fn_alter_sequence_20_72db1e(**kwargs):
    """Execute ALTER SEQUENCE command 20"""
    logger = logging.getLogger(__name__)
    logger.info("Starting alter_sequence_20")
    try:
        conn_id = get_postgres_connection_id()
        logger.info(f"Using PostgreSQL connection ID: {conn_id}")
        
        hook = PostgresHook(postgres_conn_id=conn_id)
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""ALTER SEQUENCE IF EXISTS products_product_id_seq RESTART WITH 1""")
            conn.commit()
            logger.info(f"Successfully executed: ALTER SEQUENCE")
        except Exception as e:
            conn.rollback()
            logger.error(f"Error executing ALTER SEQUENCE: {e}")
            raise
        finally:
            cursor.close()
            conn.close()
    except Exception as e:
        logger.error(f"Error in alter_sequence_20: {e}")
        raise
    finally:
        logger.info("Completed alter_sequence_20")

def sql_fn_insert_products_21_72be25(**kwargs):
    """Execute INSERT command 21"""
    logger = logging.getLogger(__name__)
    logger.info("Starting insert_products_21")
    try:
        conn_id = get_postgres_connection_id()
        logger.info(f"Using PostgreSQL connection ID: {conn_id}")
        
        hook = PostgresHook(postgres_conn_id=conn_id)
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""INSERT INTO products (product_name, category, price, cost, stock_quantity) VALUES
('Ноутбук Dell XPS 13', 'Электроника', 129999.99, 95000.00, 25),
('Смартфон iPhone 15', 'Электроника', 89999.99, 65000.00, 50),
('Кофеварка DeLonghi', 'Бытовая техника', 24999.99, 18000.00, 30),
('Футболка мужская', 'Одежда', 1999.99, 800.00, 100),
('Книга "Python для анализа данных"', 'Книги', 2999.99, 1500.00, 45),
('Наушники Sony WH-1000XM5', 'Электроника', 34999.99, 22000.00, 20),
('Чайник электрический', 'Бытовая техника', 4999.99, 3000.00, 60)
ON CONFLICT DO NOTHING""")
            conn.commit()
            logger.info(f"Successfully executed: INSERT")
        except Exception as e:
            conn.rollback()
            logger.error(f"Error executing INSERT: {e}")
            raise
        finally:
            cursor.close()
            conn.close()
    except Exception as e:
        logger.error(f"Error in insert_products_21: {e}")
        raise
    finally:
        logger.info("Completed insert_products_21")

def sql_fn_alter_sequence_22_ffa9b7(**kwargs):
    """Execute ALTER SEQUENCE command 22"""
    logger = logging.getLogger(__name__)
    logger.info("Starting alter_sequence_22")
    try:
        conn_id = get_postgres_connection_id()
        logger.info(f"Using PostgreSQL connection ID: {conn_id}")
        
        hook = PostgresHook(postgres_conn_id=conn_id)
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""ALTER SEQUENCE IF EXISTS transactions_transaction_id_seq RESTART WITH 1""")
            conn.commit()
            logger.info(f"Successfully executed: ALTER SEQUENCE")
        except Exception as e:
            conn.rollback()
            logger.error(f"Error executing ALTER SEQUENCE: {e}")
            raise
        finally:
            cursor.close()
            conn.close()
    except Exception as e:
        logger.error(f"Error in alter_sequence_22: {e}")
        raise
    finally:
        logger.info("Completed alter_sequence_22")

def sql_fn_insert_transactions_23_0a7257(**kwargs):
    """Execute INSERT command 23"""
    logger = logging.getLogger(__name__)
    logger.info("Starting insert_transactions_23")
    try:
        conn_id = get_postgres_connection_id()
        logger.info(f"Using PostgreSQL connection ID: {conn_id}")
        
        hook = PostgresHook(postgres_conn_id=conn_id)
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""INSERT INTO transactions (customer_id, amount, transaction_type, description)
SELECT
    (SELECT customer_id FROM customers WHERE email = 'ivan.ivanov@example.com'),
    129999.99,
    'purchase',
    'Покупка ноутбука Dell XPS 13'
WHERE EXISTS (SELECT 1 FROM customers WHERE email = 'ivan.ivanov@example.com')
UNION ALL
SELECT
    (SELECT customer_id FROM customers WHERE email = 'maria.petrova@example.com'),
    89999.99,
    'purchase',
    'Покупка iPhone 15'
WHERE EXISTS (SELECT 1 FROM customers WHERE email = 'maria.petrova@example.com')
UNION ALL
SELECT
    (SELECT customer_id FROM customers WHERE email = 'alexey.sidorov@example.com'),
    24999.99,
    'purchase',
    'Покупка кофеварки DeLonghi'
WHERE EXISTS (SELECT 1 FROM customers WHERE email = 'alexey.sidorov@example.com')
UNION ALL
SELECT
    (SELECT customer_id FROM customers WHERE email = 'ivan.ivanov@example.com'),
    1999.99,
    'purchase',
    'Покупка футболки'
WHERE EXISTS (SELECT 1 FROM customers WHERE email = 'ivan.ivanov@example.com')
UNION ALL
SELECT
    (SELECT customer_id FROM customers WHERE email = 'elena.kuznetsova@example.com'),
    2999.99,
    'purchase',
    'Покупка книги по Python'
WHERE EXISTS (SELECT 1 FROM customers WHERE email = 'elena.kuznetsova@example.com')
UNION ALL
SELECT
    (SELECT customer_id FROM customers WHERE email = 'maria.petrova@example.com'),
    34999.99,
    'purchase',
    'Покупка наушников Sony'
WHERE EXISTS (SELECT 1 FROM customers WHERE email = 'maria.petrova@example.com')
UNION ALL
SELECT
    (SELECT customer_id FROM customers WHERE email = 'dmitry.smirnov@example.com'),
    4999.99,
    'purchase',
    'Покупка электрического чайника'
WHERE EXISTS (SELECT 1 FROM customers WHERE email = 'dmitry.smirnov@example.com')
UNION ALL
SELECT
    (SELECT customer_id FROM customers WHERE email = 'alexey.sidorov@example.com'),
    1999.99,
    'refund',
    'Возврат футболки'
WHERE EXISTS (SELECT 1 FROM customers WHERE email = 'alexey.sidorov@example.com')
ON CONFLICT DO NOTHING""")
            conn.commit()
            logger.info(f"Successfully executed: INSERT")
        except Exception as e:
            conn.rollback()
            logger.error(f"Error executing INSERT: {e}")
            raise
        finally:
            cursor.close()
            conn.close()
    except Exception as e:
        logger.error(f"Error in insert_transactions_23: {e}")
        raise
    finally:
        logger.info("Completed insert_transactions_23")

def sql_fn_create_table_customer_analytics_24_9368bc(**kwargs):
    """Execute CREATE TABLE command 24"""
    logger = logging.getLogger(__name__)
    logger.info("Starting create_table_customer_analytics_24")
    try:
        conn_id = get_postgres_connection_id()
        logger.info(f"Using PostgreSQL connection ID: {conn_id}")
        
        hook = PostgresHook(postgres_conn_id=conn_id)
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""CREATE TABLE IF NOT EXISTS customer_analytics (
    customer_id INTEGER PRIMARY KEY REFERENCES customers(customer_id),
    total_transactions INTEGER DEFAULT 0,
    total_spent DECIMAL(15,2) DEFAULT 0,
    avg_transaction_amount DECIMAL(10,2) DEFAULT 0,
    first_transaction_date TIMESTAMP,
    last_transaction_date TIMESTAMP,
    favorite_category VARCHAR(100),
    days_since_last_activity INTEGER,
    customer_segment VARCHAR(50),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)""")
            conn.commit()
            logger.info(f"Successfully executed: CREATE TABLE")
        except Exception as e:
            conn.rollback()
            logger.error(f"Error executing CREATE TABLE: {e}")
            raise
        finally:
            cursor.close()
            conn.close()
    except Exception as e:
        logger.error(f"Error in create_table_customer_analytics_24: {e}")
        raise
    finally:
        logger.info("Completed create_table_customer_analytics_24")

def sql_fn_create_table_sales_by_category_25_1d423d(**kwargs):
    """Execute CREATE TABLE command 25"""
    logger = logging.getLogger(__name__)
    logger.info("Starting create_table_sales_by_category_25")
    try:
        conn_id = get_postgres_connection_id()
        logger.info(f"Using PostgreSQL connection ID: {conn_id}")
        
        hook = PostgresHook(postgres_conn_id=conn_id)
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""CREATE TABLE IF NOT EXISTS sales_by_category (
    category VARCHAR(100),
    period_date DATE,
    total_sales DECIMAL(15,2) DEFAULT 0,
    total_quantity INTEGER DEFAULT 0,
    unique_customers INTEGER DEFAULT 0,
    avg_sale_amount DECIMAL(10,2) DEFAULT 0,
    PRIMARY KEY (category, period_date)
)""")
            conn.commit()
            logger.info(f"Successfully executed: CREATE TABLE")
        except Exception as e:
            conn.rollback()
            logger.error(f"Error executing CREATE TABLE: {e}")
            raise
        finally:
            cursor.close()
            conn.close()
    except Exception as e:
        logger.error(f"Error in create_table_sales_by_category_25: {e}")
        raise
    finally:
        logger.info("Completed create_table_sales_by_category_25")

def sql_fn_create_table_demand_forecast_26_551200(**kwargs):
    """Execute CREATE TABLE command 26"""
    logger = logging.getLogger(__name__)
    logger.info("Starting create_table_demand_forecast_26")
    try:
        conn_id = get_postgres_connection_id()
        logger.info(f"Using PostgreSQL connection ID: {conn_id}")
        
        hook = PostgresHook(postgres_conn_id=conn_id)
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""CREATE TABLE IF NOT EXISTS demand_forecast (
    product_id INTEGER REFERENCES products(product_id),
    forecast_date DATE,
    predicted_demand INTEGER,
    confidence_interval_lower INTEGER,
    confidence_interval_upper INTEGER,
    model_version VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (product_id, forecast_date)
)""")
            conn.commit()
            logger.info(f"Successfully executed: CREATE TABLE")
        except Exception as e:
            conn.rollback()
            logger.error(f"Error executing CREATE TABLE: {e}")
            raise
        finally:
            cursor.close()
            conn.close()
    except Exception as e:
        logger.error(f"Error in create_table_demand_forecast_26: {e}")
        raise
    finally:
        logger.info("Completed create_table_demand_forecast_26")

def sql_fn_delete_customer_analytics_27_9c5e55(**kwargs):
    """Execute DELETE command 27"""
    logger = logging.getLogger(__name__)
    logger.info("Starting delete_customer_analytics_27")
    try:
        conn_id = get_postgres_connection_id()
        logger.info(f"Using PostgreSQL connection ID: {conn_id}")
        
        hook = PostgresHook(postgres_conn_id=conn_id)
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""DELETE FROM customer_analytics""")
            conn.commit()
            logger.info(f"Successfully executed: DELETE")
        except Exception as e:
            conn.rollback()
            logger.error(f"Error executing DELETE: {e}")
            raise
        finally:
            cursor.close()
            conn.close()
    except Exception as e:
        logger.error(f"Error in delete_customer_analytics_27: {e}")
        raise
    finally:
        logger.info("Completed delete_customer_analytics_27")

def sql_fn_cte_query_28_33fd92(**kwargs):
    """Execute CTE command 28"""
    logger = logging.getLogger(__name__)
    logger.info("Starting cte_query_28")
    try:
        conn_id = get_postgres_connection_id()
        logger.info(f"Using PostgreSQL connection ID: {conn_id}")
        
        hook = PostgresHook(postgres_conn_id=conn_id)
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""WITH customer_stats AS (
    SELECT
        c.customer_id,
        COUNT(t.transaction_id) as transaction_count,
        SUM(t.amount) as total_amount,
        AVG(t.amount) as avg_amount,
        MIN(t.transaction_date) as first_purchase,
        MAX(t.transaction_date) as last_purchase,
        MODE() WITHIN GROUP (ORDER BY p.category) as top_category
    FROM customers c
    LEFT JOIN transactions t ON c.customer_id = t.customer_id
    LEFT JOIN products p ON 1=1  
    WHERE t.status = 'completed'
    GROUP BY c.customer_id
)
INSERT INTO customer_analytics (
    customer_id,
    total_transactions,
    total_spent,
    avg_transaction_amount,
    first_transaction_date,
    last_transaction_date,
    favorite_category,
    days_since_last_activity,
    customer_segment
)
SELECT
    cs.customer_id,
    cs.transaction_count,
    cs.total_amount,
    cs.avg_amount,
    cs.first_purchase,
    cs.last_purchase,
    cs.top_category,
    (CURRENT_DATE - cs.last_purchase::DATE) as days_since_last,
    CASE
        WHEN cs.total_amount > 100000 THEN 'VIP'
        WHEN cs.total_amount > 50000 THEN 'Premium'
        WHEN cs.total_amount > 10000 THEN 'Regular'
        ELSE 'New'
    END as segment
FROM customer_stats cs
ON CONFLICT (customer_id) DO UPDATE SET
    total_transactions = EXCLUDED.total_transactions,
    total_spent = EXCLUDED.total_spent,
    avg_transaction_amount = EXCLUDED.avg_transaction_amount,
    last_transaction_date = EXCLUDED.last_transaction_date,
    favorite_category = EXCLUDED.favorite_category,
    days_since_last_activity = EXCLUDED.days_since_last_activity,
    customer_segment = EXCLUDED.customer_segment,
    updated_at = CURRENT_TIMESTAMP""")
            conn.commit()
            logger.info(f"Successfully executed: CTE")
        except Exception as e:
            conn.rollback()
            logger.error(f"Error executing CTE: {e}")
            raise
        finally:
            cursor.close()
            conn.close()
    except Exception as e:
        logger.error(f"Error in cte_query_28: {e}")
        raise
    finally:
        logger.info("Completed cte_query_28")

def sql_fn_delete_sales_by_category_29_3c3a3d(**kwargs):
    """Execute DELETE command 29"""
    logger = logging.getLogger(__name__)
    logger.info("Starting delete_sales_by_category_29")
    try:
        conn_id = get_postgres_connection_id()
        logger.info(f"Using PostgreSQL connection ID: {conn_id}")
        
        hook = PostgresHook(postgres_conn_id=conn_id)
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""DELETE FROM sales_by_category""")
            conn.commit()
            logger.info(f"Successfully executed: DELETE")
        except Exception as e:
            conn.rollback()
            logger.error(f"Error executing DELETE: {e}")
            raise
        finally:
            cursor.close()
            conn.close()
    except Exception as e:
        logger.error(f"Error in delete_sales_by_category_29: {e}")
        raise
    finally:
        logger.info("Completed delete_sales_by_category_29")

def sql_fn_cte_query_30_1ae068(**kwargs):
    """Execute CTE command 30"""
    logger = logging.getLogger(__name__)
    logger.info("Starting cte_query_30")
    try:
        conn_id = get_postgres_connection_id()
        logger.info(f"Using PostgreSQL connection ID: {conn_id}")
        
        hook = PostgresHook(postgres_conn_id=conn_id)
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""WITH daily_sales AS (
    SELECT
        p.category,
        DATE(t.transaction_date) as sale_date,
        SUM(t.amount) as daily_total,
        COUNT(t.transaction_id) as daily_count,
        COUNT(DISTINCT t.customer_id) as customer_count
    FROM transactions t
    JOIN products p ON 1=1  
    WHERE t.transaction_type = 'purchase'
        AND t.status = 'completed'
        AND DATE(t.transaction_date) = CURRENT_DATE
    GROUP BY p.category, DATE(t.transaction_date)
)
INSERT INTO sales_by_category (
    category,
    period_date,
    total_sales,
    total_quantity,
    unique_customers,
    avg_sale_amount
)
SELECT
    category,
    sale_date,
    daily_total,
    daily_count,
    customer_count,
    CASE
        WHEN daily_count > 0 THEN daily_total / daily_count
        ELSE 0
    END as avg_sale
FROM daily_sales
ON CONFLICT (category, period_date) DO UPDATE SET
    total_sales = EXCLUDED.total_sales,
    total_quantity = EXCLUDED.total_quantity,
    unique_customers = EXCLUDED.unique_customers,
    avg_sale_amount = EXCLUDED.avg_sale_amount""")
            conn.commit()
            logger.info(f"Successfully executed: CTE")
        except Exception as e:
            conn.rollback()
            logger.error(f"Error executing CTE: {e}")
            raise
        finally:
            cursor.close()
            conn.close()
    except Exception as e:
        logger.error(f"Error in cte_query_30: {e}")
        raise
    finally:
        logger.info("Completed cte_query_30")

with DAG(
    dag_id="generated_basic_setup",
    default_args=default_args,
    start_date=datetime(2026, 1, 25),
    schedule_interval="@hourly",
    catchup=False,
    tags=["analytics", "etl", "reporting"]
) as dag:

    # SQL задачи:
    drop_table_sales_by_category_1 = PythonOperator(
        task_id="drop_table_sales_by_category_1",
        python_callable=sql_fn_drop_table_sales_by_category_1_485d0f,
        doc_md="""Execute DROP TABLE command 1""",
    )
    drop_table_demand_forecast_2 = PythonOperator(
        task_id="drop_table_demand_forecast_2",
        python_callable=sql_fn_drop_table_demand_forecast_2_0e2deb,
        doc_md="""Execute DROP TABLE command 2""",
    )
    drop_table_customer_analytics_3 = PythonOperator(
        task_id="drop_table_customer_analytics_3",
        python_callable=sql_fn_drop_table_customer_analytics_3_22f585,
        doc_md="""Execute DROP TABLE command 3""",
    )
    drop_table_transactions_4 = PythonOperator(
        task_id="drop_table_transactions_4",
        python_callable=sql_fn_drop_table_transactions_4_b91229,
        doc_md="""Execute DROP TABLE command 4""",
    )
    drop_table_products_5 = PythonOperator(
        task_id="drop_table_products_5",
        python_callable=sql_fn_drop_table_products_5_503d97,
        doc_md="""Execute DROP TABLE command 5""",
    )
    drop_table_customers_6 = PythonOperator(
        task_id="drop_table_customers_6",
        python_callable=sql_fn_drop_table_customers_6_68d1ee,
        doc_md="""Execute DROP TABLE command 6""",
    )
    create_table_customers_7 = PythonOperator(
        task_id="create_table_customers_7",
        python_callable=sql_fn_create_table_customers_7_5db892,
        doc_md="""Execute CREATE TABLE command 7""",
    )
    create_table_products_8 = PythonOperator(
        task_id="create_table_products_8",
        python_callable=sql_fn_create_table_products_8_57fad7,
        doc_md="""Execute CREATE TABLE command 8""",
    )
    create_table_transactions_9 = PythonOperator(
        task_id="create_table_transactions_9",
        python_callable=sql_fn_create_table_transactions_9_245717,
        doc_md="""Execute CREATE TABLE command 9""",
    )
    create_index_idx_customers_email_10 = PythonOperator(
        task_id="create_index_idx_customers_email_10",
        python_callable=sql_fn_create_index_idx_customers_email_10_b9df35,
        doc_md="""Execute CREATE INDEX command 10""",
    )
    create_index_idx_customers_last_name_11 = PythonOperator(
        task_id="create_index_idx_customers_last_name_11",
        python_callable=sql_fn_create_index_idx_customers_last_name_11_cfef34,
        doc_md="""Execute CREATE INDEX command 11""",
    )
    create_index_idx_transactions_customer_id_12 = PythonOperator(
        task_id="create_index_idx_transactions_customer_id_12",
        python_callable=sql_fn_create_index_idx_transactions_customer_id_12_e088d3,
        doc_md="""Execute CREATE INDEX command 12""",
    )
    create_index_idx_transactions_date_13 = PythonOperator(
        task_id="create_index_idx_transactions_date_13",
        python_callable=sql_fn_create_index_idx_transactions_date_13_47eaab,
        doc_md="""Execute CREATE INDEX command 13""",
    )
    create_index_idx_transactions_type_status_14 = PythonOperator(
        task_id="create_index_idx_transactions_type_status_14",
        python_callable=sql_fn_create_index_idx_transactions_type_status_14_f8c78d,
        doc_md="""Execute CREATE INDEX command 14""",
    )
    create_index_idx_products_category_15 = PythonOperator(
        task_id="create_index_idx_products_category_15",
        python_callable=sql_fn_create_index_idx_products_category_15_3b4c37,
        doc_md="""Execute CREATE INDEX command 15""",
    )
    create_index_idx_products_supplier_16 = PythonOperator(
        task_id="create_index_idx_products_supplier_16",
        python_callable=sql_fn_create_index_idx_products_supplier_16_52e296,
        doc_md="""Execute CREATE INDEX command 16""",
    )
    truncate_17 = PythonOperator(
        task_id="truncate_17",
        python_callable=sql_fn_truncate_17_16e3e8,
        doc_md="""Execute TRUNCATE command 17""",
    )
    alter_sequence_18 = PythonOperator(
        task_id="alter_sequence_18",
        python_callable=sql_fn_alter_sequence_18_7dbdf8,
        doc_md="""Execute ALTER SEQUENCE command 18""",
    )
    insert_customers_19 = PythonOperator(
        task_id="insert_customers_19",
        python_callable=sql_fn_insert_customers_19_ae52cd,
        doc_md="""Execute INSERT command 19""",
    )
    alter_sequence_20 = PythonOperator(
        task_id="alter_sequence_20",
        python_callable=sql_fn_alter_sequence_20_72db1e,
        doc_md="""Execute ALTER SEQUENCE command 20""",
    )
    insert_products_21 = PythonOperator(
        task_id="insert_products_21",
        python_callable=sql_fn_insert_products_21_72be25,
        doc_md="""Execute INSERT command 21""",
    )
    alter_sequence_22 = PythonOperator(
        task_id="alter_sequence_22",
        python_callable=sql_fn_alter_sequence_22_ffa9b7,
        doc_md="""Execute ALTER SEQUENCE command 22""",
    )
    insert_transactions_23 = PythonOperator(
        task_id="insert_transactions_23",
        python_callable=sql_fn_insert_transactions_23_0a7257,
        doc_md="""Execute INSERT command 23""",
    )
    create_table_customer_analytics_24 = PythonOperator(
        task_id="create_table_customer_analytics_24",
        python_callable=sql_fn_create_table_customer_analytics_24_9368bc,
        doc_md="""Execute CREATE TABLE command 24""",
    )
    create_table_sales_by_category_25 = PythonOperator(
        task_id="create_table_sales_by_category_25",
        python_callable=sql_fn_create_table_sales_by_category_25_1d423d,
        doc_md="""Execute CREATE TABLE command 25""",
    )
    create_table_demand_forecast_26 = PythonOperator(
        task_id="create_table_demand_forecast_26",
        python_callable=sql_fn_create_table_demand_forecast_26_551200,
        doc_md="""Execute CREATE TABLE command 26""",
    )
    delete_customer_analytics_27 = PythonOperator(
        task_id="delete_customer_analytics_27",
        python_callable=sql_fn_delete_customer_analytics_27_9c5e55,
        doc_md="""Execute DELETE command 27""",
    )
    cte_query_28 = PythonOperator(
        task_id="cte_query_28",
        python_callable=sql_fn_cte_query_28_33fd92,
        doc_md="""Execute CTE command 28""",
    )
    delete_sales_by_category_29 = PythonOperator(
        task_id="delete_sales_by_category_29",
        python_callable=sql_fn_delete_sales_by_category_29_3c3a3d,
        doc_md="""Execute DELETE command 29""",
    )
    cte_query_30 = PythonOperator(
        task_id="cte_query_30",
        python_callable=sql_fn_cte_query_30_1ae068,
        doc_md="""Execute CTE command 30""",
    )

    # Линейные зависимости для последовательного выполнения
    drop_table_sales_by_category_1 >> drop_table_demand_forecast_2 >> drop_table_customer_analytics_3 >> drop_table_transactions_4 >> drop_table_products_5 >> drop_table_customers_6 >> create_table_customers_7 >> create_table_products_8 >> create_table_transactions_9 >> create_index_idx_customers_email_10 >> create_index_idx_customers_last_name_11 >> create_index_idx_transactions_customer_id_12 >> create_index_idx_transactions_date_13 >> create_index_idx_transactions_type_status_14 >> create_index_idx_products_category_15 >> create_index_idx_products_supplier_16 >> truncate_17 >> alter_sequence_18 >> insert_customers_19 >> alter_sequence_20 >> insert_products_21 >> alter_sequence_22 >> insert_transactions_23 >> create_table_customer_analytics_24 >> create_table_sales_by_category_25 >> create_table_demand_forecast_26 >> delete_customer_analytics_27 >> cte_query_28 >> delete_sales_by_category_29 >> cte_query_30