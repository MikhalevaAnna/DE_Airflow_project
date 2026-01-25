"""
Auto-generated DAG from setup_database.sql
Generated: 2026-01-25 18:03:57
Total tasks: 42
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable

# Default arguments for the DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(seconds=30),
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

def sql_fn_create_table_users_1_df0c8c(**kwargs):
    """Execute CREATE TABLE command 1"""
    logger = logging.getLogger(__name__)
    logger.info("Starting create_table_users_1")
    try:
        conn_id = get_postgres_connection_id()
        logger.info(f"Using PostgreSQL connection ID: {conn_id}")
        
        hook = PostgresHook(postgres_conn_id=conn_id)
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""CREATE TABLE IF NOT EXISTS users (
    id SERIAL PRIMARY KEY,
    username VARCHAR(50) NOT NULL UNIQUE,
    email VARCHAR(100) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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
        logger.error(f"Error in create_table_users_1: {e}")
        raise
    finally:
        logger.info("Completed create_table_users_1")

def sql_fn_create_table_orders_2_13d3c2(**kwargs):
    """Execute CREATE TABLE command 2"""
    logger = logging.getLogger(__name__)
    logger.info("Starting create_table_orders_2")
    try:
        conn_id = get_postgres_connection_id()
        logger.info(f"Using PostgreSQL connection ID: {conn_id}")
        
        hook = PostgresHook(postgres_conn_id=conn_id)
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""CREATE TABLE IF NOT EXISTS orders (
    order_id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES users(id),
    amount DECIMAL(10, 2) NOT NULL,
    status VARCHAR(20) DEFAULT 'pending',
    order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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
        logger.error(f"Error in create_table_orders_2: {e}")
        raise
    finally:
        logger.info("Completed create_table_orders_2")

def sql_fn_drop_index_idx_users_email_3_b628cc(**kwargs):
    """Execute DROP INDEX command 3"""
    logger = logging.getLogger(__name__)
    logger.info("Starting drop_index_idx_users_email_3")
    try:
        conn_id = get_postgres_connection_id()
        logger.info(f"Using PostgreSQL connection ID: {conn_id}")
        
        hook = PostgresHook(postgres_conn_id=conn_id)
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""DROP INDEX IF EXISTS idx_users_email""")
            conn.commit()
            logger.info(f"Successfully executed: DROP INDEX")
        except Exception as e:
            conn.rollback()
            logger.error(f"Error executing DROP INDEX: {e}")
            raise
        finally:
            cursor.close()
            conn.close()
    except Exception as e:
        logger.error(f"Error in drop_index_idx_users_email_3: {e}")
        raise
    finally:
        logger.info("Completed drop_index_idx_users_email_3")

def sql_fn_create_index_idx_users_email_4_b6cf3f(**kwargs):
    """Execute CREATE INDEX command 4"""
    logger = logging.getLogger(__name__)
    logger.info("Starting create_index_idx_users_email_4")
    try:
        conn_id = get_postgres_connection_id()
        logger.info(f"Using PostgreSQL connection ID: {conn_id}")
        
        hook = PostgresHook(postgres_conn_id=conn_id)
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""CREATE INDEX idx_users_email ON users(email)""")
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
        logger.error(f"Error in create_index_idx_users_email_4: {e}")
        raise
    finally:
        logger.info("Completed create_index_idx_users_email_4")

def sql_fn_drop_index_idx_orders_user_id_5_d26952(**kwargs):
    """Execute DROP INDEX command 5"""
    logger = logging.getLogger(__name__)
    logger.info("Starting drop_index_idx_orders_user_id_5")
    try:
        conn_id = get_postgres_connection_id()
        logger.info(f"Using PostgreSQL connection ID: {conn_id}")
        
        hook = PostgresHook(postgres_conn_id=conn_id)
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""DROP INDEX IF EXISTS idx_orders_user_id""")
            conn.commit()
            logger.info(f"Successfully executed: DROP INDEX")
        except Exception as e:
            conn.rollback()
            logger.error(f"Error executing DROP INDEX: {e}")
            raise
        finally:
            cursor.close()
            conn.close()
    except Exception as e:
        logger.error(f"Error in drop_index_idx_orders_user_id_5: {e}")
        raise
    finally:
        logger.info("Completed drop_index_idx_orders_user_id_5")

def sql_fn_create_index_idx_orders_user_id_6_1a7e1d(**kwargs):
    """Execute CREATE INDEX command 6"""
    logger = logging.getLogger(__name__)
    logger.info("Starting create_index_idx_orders_user_id_6")
    try:
        conn_id = get_postgres_connection_id()
        logger.info(f"Using PostgreSQL connection ID: {conn_id}")
        
        hook = PostgresHook(postgres_conn_id=conn_id)
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""CREATE INDEX idx_orders_user_id ON orders(user_id)""")
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
        logger.error(f"Error in create_index_idx_orders_user_id_6: {e}")
        raise
    finally:
        logger.info("Completed create_index_idx_orders_user_id_6")

def sql_fn_delete_orders_7_c05a65(**kwargs):
    """Execute DELETE command 7"""
    logger = logging.getLogger(__name__)
    logger.info("Starting delete_orders_7")
    try:
        conn_id = get_postgres_connection_id()
        logger.info(f"Using PostgreSQL connection ID: {conn_id}")
        
        hook = PostgresHook(postgres_conn_id=conn_id)
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""DELETE FROM orders""")
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
        logger.error(f"Error in delete_orders_7: {e}")
        raise
    finally:
        logger.info("Completed delete_orders_7")

def sql_fn_delete_users_8_9195b2(**kwargs):
    """Execute DELETE command 8"""
    logger = logging.getLogger(__name__)
    logger.info("Starting delete_users_8")
    try:
        conn_id = get_postgres_connection_id()
        logger.info(f"Using PostgreSQL connection ID: {conn_id}")
        
        hook = PostgresHook(postgres_conn_id=conn_id)
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""DELETE FROM users""")
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
        logger.error(f"Error in delete_users_8: {e}")
        raise
    finally:
        logger.info("Completed delete_users_8")

def sql_fn_insert_users_9_e2cc4a(**kwargs):
    """Execute INSERT command 9"""
    logger = logging.getLogger(__name__)
    logger.info("Starting insert_users_9")
    try:
        conn_id = get_postgres_connection_id()
        logger.info(f"Using PostgreSQL connection ID: {conn_id}")
        
        hook = PostgresHook(postgres_conn_id=conn_id)
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""INSERT INTO users (username, email) VALUES
('john_doe', 'john@example.com'),
('jane_smith', 'jane@example.com')""")
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
        logger.error(f"Error in insert_users_9: {e}")
        raise
    finally:
        logger.info("Completed insert_users_9")

def sql_fn_update_users_10_8cf6cb(**kwargs):
    """Execute UPDATE command 10"""
    logger = logging.getLogger(__name__)
    logger.info("Starting update_users_10")
    try:
        conn_id = get_postgres_connection_id()
        logger.info(f"Using PostgreSQL connection ID: {conn_id}")
        
        hook = PostgresHook(postgres_conn_id=conn_id)
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""UPDATE users SET email = 'john.doe@example.com' WHERE username = 'john_doe'""")
            conn.commit()
            logger.info(f"Successfully executed: UPDATE")
        except Exception as e:
            conn.rollback()
            logger.error(f"Error executing UPDATE: {e}")
            raise
        finally:
            cursor.close()
            conn.close()
    except Exception as e:
        logger.error(f"Error in update_users_10: {e}")
        raise
    finally:
        logger.info("Completed update_users_10")

def sql_fn_select_users_11_14f104(**kwargs):
    """Execute SELECT command 11"""
    logger = logging.getLogger(__name__)
    logger.info("Starting select_users_11")
    try:
        conn_id = get_postgres_connection_id()
        logger.info(f"Using PostgreSQL connection ID: {conn_id}")
        
        hook = PostgresHook(postgres_conn_id=conn_id)
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""SELECT email FROM users AS u INNER JOIN orders AS o
ON u.id = o.user_id
WHERE u.email = 'john.doe@example.com'""")
            conn.commit()
            logger.info(f"Successfully executed: SELECT")
        except Exception as e:
            conn.rollback()
            logger.error(f"Error executing SELECT: {e}")
            raise
        finally:
            cursor.close()
            conn.close()
    except Exception as e:
        logger.error(f"Error in select_users_11: {e}")
        raise
    finally:
        logger.info("Completed select_users_11")

def sql_fn_select_orders_12_b082a5(**kwargs):
    """Execute SELECT command 12"""
    logger = logging.getLogger(__name__)
    logger.info("Starting select_orders_12")
    try:
        conn_id = get_postgres_connection_id()
        logger.info(f"Using PostgreSQL connection ID: {conn_id}")
        
        hook = PostgresHook(postgres_conn_id=conn_id)
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""SELECT user_id FROM  orders
WHERE amount = (SELECT MAX(amount) FROM orders)""")
            conn.commit()
            logger.info(f"Successfully executed: SELECT")
        except Exception as e:
            conn.rollback()
            logger.error(f"Error executing SELECT: {e}")
            raise
        finally:
            cursor.close()
            conn.close()
    except Exception as e:
        logger.error(f"Error in select_orders_12: {e}")
        raise
    finally:
        logger.info("Completed select_orders_12")

def sql_fn_cte_query_13_75369b(**kwargs):
    """Execute CTE command 13"""
    logger = logging.getLogger(__name__)
    logger.info("Starting cte_query_13")
    try:
        conn_id = get_postgres_connection_id()
        logger.info(f"Using PostgreSQL connection ID: {conn_id}")
        
        hook = PostgresHook(postgres_conn_id=conn_id)
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""WITH SubTotal AS ( SELECT user_id, SUM(amount) AS total FROM orders GROUP BY user_id )
SELECT user_id, total FROM SubTotal""")
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
        logger.error(f"Error in cte_query_13: {e}")
        raise
    finally:
        logger.info("Completed cte_query_13")

def sql_fn_create_table_users_14_df0c8c(**kwargs):
    """Execute CREATE TABLE command 14"""
    logger = logging.getLogger(__name__)
    logger.info("Starting create_table_users_14")
    try:
        conn_id = get_postgres_connection_id()
        logger.info(f"Using PostgreSQL connection ID: {conn_id}")
        
        hook = PostgresHook(postgres_conn_id=conn_id)
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""CREATE TABLE IF NOT EXISTS users (
    id SERIAL PRIMARY KEY,
    username VARCHAR(50) NOT NULL UNIQUE,
    email VARCHAR(100) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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
        logger.error(f"Error in create_table_users_14: {e}")
        raise
    finally:
        logger.info("Completed create_table_users_14")

def sql_fn_create_table_orders_15_4a550f(**kwargs):
    """Execute CREATE TABLE command 15"""
    logger = logging.getLogger(__name__)
    logger.info("Starting create_table_orders_15")
    try:
        conn_id = get_postgres_connection_id()
        logger.info(f"Using PostgreSQL connection ID: {conn_id}")
        
        hook = PostgresHook(postgres_conn_id=conn_id)
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""CREATE TABLE IF NOT EXISTS orders (
    order_id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    amount DECIMAL(10, 2) NOT NULL CHECK (amount > 0),
    status VARCHAR(20) DEFAULT 'pending',
    order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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
        logger.error(f"Error in create_table_orders_15: {e}")
        raise
    finally:
        logger.info("Completed create_table_orders_15")

def sql_fn_create_table_employees_16_8a06ed(**kwargs):
    """Execute CREATE TABLE command 16"""
    logger = logging.getLogger(__name__)
    logger.info("Starting create_table_employees_16")
    try:
        conn_id = get_postgres_connection_id()
        logger.info(f"Using PostgreSQL connection ID: {conn_id}")
        
        hook = PostgresHook(postgres_conn_id=conn_id)
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""CREATE TABLE IF NOT EXISTS employees (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    position VARCHAR(50),
    manager_id INTEGER REFERENCES employees(id) ON DELETE SET NULL,
    department VARCHAR(50),
    salary DECIMAL(10, 2),
    hired_date DATE DEFAULT CURRENT_DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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
        logger.error(f"Error in create_table_employees_16: {e}")
        raise
    finally:
        logger.info("Completed create_table_employees_16")

def sql_fn_drop_index_idx_orders_user_id_17_d26952(**kwargs):
    """Execute DROP INDEX command 17"""
    logger = logging.getLogger(__name__)
    logger.info("Starting drop_index_idx_orders_user_id_17")
    try:
        conn_id = get_postgres_connection_id()
        logger.info(f"Using PostgreSQL connection ID: {conn_id}")
        
        hook = PostgresHook(postgres_conn_id=conn_id)
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""DROP INDEX IF EXISTS idx_orders_user_id""")
            conn.commit()
            logger.info(f"Successfully executed: DROP INDEX")
        except Exception as e:
            conn.rollback()
            logger.error(f"Error executing DROP INDEX: {e}")
            raise
        finally:
            cursor.close()
            conn.close()
    except Exception as e:
        logger.error(f"Error in drop_index_idx_orders_user_id_17: {e}")
        raise
    finally:
        logger.info("Completed drop_index_idx_orders_user_id_17")

def sql_fn_create_index_idx_orders_user_id_18_e791ec(**kwargs):
    """Execute CREATE INDEX command 18"""
    logger = logging.getLogger(__name__)
    logger.info("Starting create_index_idx_orders_user_id_18")
    try:
        conn_id = get_postgres_connection_id()
        logger.info(f"Using PostgreSQL connection ID: {conn_id}")
        
        hook = PostgresHook(postgres_conn_id=conn_id)
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""CREATE INDEX IF NOT EXISTS idx_orders_user_id ON orders(user_id)""")
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
        logger.error(f"Error in create_index_idx_orders_user_id_18: {e}")
        raise
    finally:
        logger.info("Completed create_index_idx_orders_user_id_18")

def sql_fn_drop_index_idx_orders_order_date_19_acc162(**kwargs):
    """Execute DROP INDEX command 19"""
    logger = logging.getLogger(__name__)
    logger.info("Starting drop_index_idx_orders_order_date_19")
    try:
        conn_id = get_postgres_connection_id()
        logger.info(f"Using PostgreSQL connection ID: {conn_id}")
        
        hook = PostgresHook(postgres_conn_id=conn_id)
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""DROP INDEX IF EXISTS idx_orders_order_date""")
            conn.commit()
            logger.info(f"Successfully executed: DROP INDEX")
        except Exception as e:
            conn.rollback()
            logger.error(f"Error executing DROP INDEX: {e}")
            raise
        finally:
            cursor.close()
            conn.close()
    except Exception as e:
        logger.error(f"Error in drop_index_idx_orders_order_date_19: {e}")
        raise
    finally:
        logger.info("Completed drop_index_idx_orders_order_date_19")

def sql_fn_create_index_idx_orders_order_date_20_66b9a9(**kwargs):
    """Execute CREATE INDEX command 20"""
    logger = logging.getLogger(__name__)
    logger.info("Starting create_index_idx_orders_order_date_20")
    try:
        conn_id = get_postgres_connection_id()
        logger.info(f"Using PostgreSQL connection ID: {conn_id}")
        
        hook = PostgresHook(postgres_conn_id=conn_id)
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""CREATE INDEX IF NOT EXISTS idx_orders_order_date ON orders(order_date)""")
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
        logger.error(f"Error in create_index_idx_orders_order_date_20: {e}")
        raise
    finally:
        logger.info("Completed create_index_idx_orders_order_date_20")

def sql_fn_drop_index_idx_employees_manager_id_21_26b802(**kwargs):
    """Execute DROP INDEX command 21"""
    logger = logging.getLogger(__name__)
    logger.info("Starting drop_index_idx_employees_manager_id_21")
    try:
        conn_id = get_postgres_connection_id()
        logger.info(f"Using PostgreSQL connection ID: {conn_id}")
        
        hook = PostgresHook(postgres_conn_id=conn_id)
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""DROP INDEX IF EXISTS idx_employees_manager_id""")
            conn.commit()
            logger.info(f"Successfully executed: DROP INDEX")
        except Exception as e:
            conn.rollback()
            logger.error(f"Error executing DROP INDEX: {e}")
            raise
        finally:
            cursor.close()
            conn.close()
    except Exception as e:
        logger.error(f"Error in drop_index_idx_employees_manager_id_21: {e}")
        raise
    finally:
        logger.info("Completed drop_index_idx_employees_manager_id_21")

def sql_fn_create_index_idx_employees_manager_id_22_5f8ca4(**kwargs):
    """Execute CREATE INDEX command 22"""
    logger = logging.getLogger(__name__)
    logger.info("Starting create_index_idx_employees_manager_id_22")
    try:
        conn_id = get_postgres_connection_id()
        logger.info(f"Using PostgreSQL connection ID: {conn_id}")
        
        hook = PostgresHook(postgres_conn_id=conn_id)
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""CREATE INDEX IF NOT EXISTS idx_employees_manager_id ON employees(manager_id)""")
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
        logger.error(f"Error in create_index_idx_employees_manager_id_22: {e}")
        raise
    finally:
        logger.info("Completed create_index_idx_employees_manager_id_22")

def sql_fn_drop_index_idx_users_email_23_b628cc(**kwargs):
    """Execute DROP INDEX command 23"""
    logger = logging.getLogger(__name__)
    logger.info("Starting drop_index_idx_users_email_23")
    try:
        conn_id = get_postgres_connection_id()
        logger.info(f"Using PostgreSQL connection ID: {conn_id}")
        
        hook = PostgresHook(postgres_conn_id=conn_id)
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""DROP INDEX IF EXISTS idx_users_email""")
            conn.commit()
            logger.info(f"Successfully executed: DROP INDEX")
        except Exception as e:
            conn.rollback()
            logger.error(f"Error executing DROP INDEX: {e}")
            raise
        finally:
            cursor.close()
            conn.close()
    except Exception as e:
        logger.error(f"Error in drop_index_idx_users_email_23: {e}")
        raise
    finally:
        logger.info("Completed drop_index_idx_users_email_23")

def sql_fn_create_index_idx_users_email_24_7b1524(**kwargs):
    """Execute CREATE INDEX command 24"""
    logger = logging.getLogger(__name__)
    logger.info("Starting create_index_idx_users_email_24")
    try:
        conn_id = get_postgres_connection_id()
        logger.info(f"Using PostgreSQL connection ID: {conn_id}")
        
        hook = PostgresHook(postgres_conn_id=conn_id)
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""CREATE INDEX IF NOT EXISTS idx_users_email ON users(email)""")
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
        logger.error(f"Error in create_index_idx_users_email_24: {e}")
        raise
    finally:
        logger.info("Completed create_index_idx_users_email_24")

def sql_fn_insert_users_25_a5af0d(**kwargs):
    """Execute INSERT command 25"""
    logger = logging.getLogger(__name__)
    logger.info("Starting insert_users_25")
    try:
        conn_id = get_postgres_connection_id()
        logger.info(f"Using PostgreSQL connection ID: {conn_id}")
        
        hook = PostgresHook(postgres_conn_id=conn_id)
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""INSERT INTO users (username, email)
SELECT 'john_doe', 'john@example.com'
WHERE NOT EXISTS (SELECT 1 FROM users WHERE username = 'john_doe')""")
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
        logger.error(f"Error in insert_users_25: {e}")
        raise
    finally:
        logger.info("Completed insert_users_25")

def sql_fn_insert_users_26_90f0ea(**kwargs):
    """Execute INSERT command 26"""
    logger = logging.getLogger(__name__)
    logger.info("Starting insert_users_26")
    try:
        conn_id = get_postgres_connection_id()
        logger.info(f"Using PostgreSQL connection ID: {conn_id}")
        
        hook = PostgresHook(postgres_conn_id=conn_id)
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""INSERT INTO users (username, email)
SELECT 'jane_smith', 'jane@example.com'
WHERE NOT EXISTS (SELECT 1 FROM users WHERE username = 'jane_smith')""")
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
        logger.error(f"Error in insert_users_26: {e}")
        raise
    finally:
        logger.info("Completed insert_users_26")

def sql_fn_insert_users_27_8a576d(**kwargs):
    """Execute INSERT command 27"""
    logger = logging.getLogger(__name__)
    logger.info("Starting insert_users_27")
    try:
        conn_id = get_postgres_connection_id()
        logger.info(f"Using PostgreSQL connection ID: {conn_id}")
        
        hook = PostgresHook(postgres_conn_id=conn_id)
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""INSERT INTO users (username, email)
SELECT 'mike_jones', 'mike@example.com'
WHERE NOT EXISTS (SELECT 1 FROM users WHERE username = 'mike_jones')""")
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
        logger.error(f"Error in insert_users_27: {e}")
        raise
    finally:
        logger.info("Completed insert_users_27")

def sql_fn_insert_orders_28_07229b(**kwargs):
    """Execute INSERT command 28"""
    logger = logging.getLogger(__name__)
    logger.info("Starting insert_orders_28")
    try:
        conn_id = get_postgres_connection_id()
        logger.info(f"Using PostgreSQL connection ID: {conn_id}")
        
        hook = PostgresHook(postgres_conn_id=conn_id)
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""INSERT INTO orders (user_id, amount, status)
SELECT
    (SELECT id FROM users WHERE username = 'john_doe'),
    100.50,
    'completed'
WHERE NOT EXISTS (SELECT 1 FROM orders WHERE user_id = (SELECT id FROM users WHERE username = 'john_doe') AND amount = 100.50)""")
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
        logger.error(f"Error in insert_orders_28: {e}")
        raise
    finally:
        logger.info("Completed insert_orders_28")

def sql_fn_insert_orders_29_f8de50(**kwargs):
    """Execute INSERT command 29"""
    logger = logging.getLogger(__name__)
    logger.info("Starting insert_orders_29")
    try:
        conn_id = get_postgres_connection_id()
        logger.info(f"Using PostgreSQL connection ID: {conn_id}")
        
        hook = PostgresHook(postgres_conn_id=conn_id)
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""INSERT INTO orders (user_id, amount, status)
SELECT
    (SELECT id FROM users WHERE username = 'john_doe'),
    75.25,
    'completed'
WHERE NOT EXISTS (SELECT 1 FROM orders WHERE user_id = (SELECT id FROM users WHERE username = 'john_doe') AND amount = 75.25)""")
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
        logger.error(f"Error in insert_orders_29: {e}")
        raise
    finally:
        logger.info("Completed insert_orders_29")

def sql_fn_insert_orders_30_105ca9(**kwargs):
    """Execute INSERT command 30"""
    logger = logging.getLogger(__name__)
    logger.info("Starting insert_orders_30")
    try:
        conn_id = get_postgres_connection_id()
        logger.info(f"Using PostgreSQL connection ID: {conn_id}")
        
        hook = PostgresHook(postgres_conn_id=conn_id)
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""INSERT INTO orders (user_id, amount, status)
SELECT
    (SELECT id FROM users WHERE username = 'jane_smith'),
    200.00,
    'completed'
WHERE NOT EXISTS (SELECT 1 FROM orders WHERE user_id = (SELECT id FROM users WHERE username = 'jane_smith') AND amount = 200.00)""")
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
        logger.error(f"Error in insert_orders_30: {e}")
        raise
    finally:
        logger.info("Completed insert_orders_30")

def sql_fn_insert_orders_31_88828c(**kwargs):
    """Execute INSERT command 31"""
    logger = logging.getLogger(__name__)
    logger.info("Starting insert_orders_31")
    try:
        conn_id = get_postgres_connection_id()
        logger.info(f"Using PostgreSQL connection ID: {conn_id}")
        
        hook = PostgresHook(postgres_conn_id=conn_id)
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""INSERT INTO orders (user_id, amount, status)
SELECT
    (SELECT id FROM users WHERE username = 'mike_jones'),
    50.00,
    'pending'
WHERE NOT EXISTS (SELECT 1 FROM orders WHERE user_id = (SELECT id FROM users WHERE username = 'mike_jones') AND amount = 50.00)""")
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
        logger.error(f"Error in insert_orders_31: {e}")
        raise
    finally:
        logger.info("Completed insert_orders_31")

def sql_fn_insert_employees_32_8be146(**kwargs):
    """Execute INSERT command 32"""
    logger = logging.getLogger(__name__)
    logger.info("Starting insert_employees_32")
    try:
        conn_id = get_postgres_connection_id()
        logger.info(f"Using PostgreSQL connection ID: {conn_id}")
        
        hook = PostgresHook(postgres_conn_id=conn_id)
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""INSERT INTO employees (name, position, manager_id, department, salary)
SELECT 'Alice Johnson', 'CEO', NULL, 'Executive', 150000.00
WHERE NOT EXISTS (SELECT 1 FROM employees WHERE name = 'Alice Johnson')""")
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
        logger.error(f"Error in insert_employees_32: {e}")
        raise
    finally:
        logger.info("Completed insert_employees_32")

def sql_fn_insert_employees_33_a1fdc4(**kwargs):
    """Execute INSERT command 33"""
    logger = logging.getLogger(__name__)
    logger.info("Starting insert_employees_33")
    try:
        conn_id = get_postgres_connection_id()
        logger.info(f"Using PostgreSQL connection ID: {conn_id}")
        
        hook = PostgresHook(postgres_conn_id=conn_id)
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""INSERT INTO employees (name, position, manager_id, department, salary)
SELECT
    'Bob Smith',
    'CTO',
    (SELECT id FROM employees WHERE name = 'Alice Johnson'),
    'Executive',
    120000.00
WHERE NOT EXISTS (SELECT 1 FROM employees WHERE name = 'Bob Smith')""")
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
        logger.error(f"Error in insert_employees_33: {e}")
        raise
    finally:
        logger.info("Completed insert_employees_33")

def sql_fn_insert_employees_34_121632(**kwargs):
    """Execute INSERT command 34"""
    logger = logging.getLogger(__name__)
    logger.info("Starting insert_employees_34")
    try:
        conn_id = get_postgres_connection_id()
        logger.info(f"Using PostgreSQL connection ID: {conn_id}")
        
        hook = PostgresHook(postgres_conn_id=conn_id)
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""INSERT INTO employees (name, position, manager_id, department, salary)
SELECT
    'Carol Davis',
    'CFO',
    (SELECT id FROM employees WHERE name = 'Alice Johnson'),
    'Executive',
    110000.00
WHERE NOT EXISTS (SELECT 1 FROM employees WHERE name = 'Carol Davis')""")
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
        logger.error(f"Error in insert_employees_34: {e}")
        raise
    finally:
        logger.info("Completed insert_employees_34")

def sql_fn_insert_employees_35_ecdd81(**kwargs):
    """Execute INSERT command 35"""
    logger = logging.getLogger(__name__)
    logger.info("Starting insert_employees_35")
    try:
        conn_id = get_postgres_connection_id()
        logger.info(f"Using PostgreSQL connection ID: {conn_id}")
        
        hook = PostgresHook(postgres_conn_id=conn_id)
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""INSERT INTO employees (name, position, manager_id, department, salary)
SELECT
    'David Wilson',
    'Engineering Manager',
    (SELECT id FROM employees WHERE name = 'Bob Smith'),
    'Engineering',
    90000.00
WHERE NOT EXISTS (SELECT 1 FROM employees WHERE name = 'David Wilson')""")
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
        logger.error(f"Error in insert_employees_35: {e}")
        raise
    finally:
        logger.info("Completed insert_employees_35")

def sql_fn_insert_employees_36_b0285c(**kwargs):
    """Execute INSERT command 36"""
    logger = logging.getLogger(__name__)
    logger.info("Starting insert_employees_36")
    try:
        conn_id = get_postgres_connection_id()
        logger.info(f"Using PostgreSQL connection ID: {conn_id}")
        
        hook = PostgresHook(postgres_conn_id=conn_id)
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""INSERT INTO employees (name, position, manager_id, department, salary)
SELECT
    'Eva Brown',
    'Senior Developer',
    (SELECT id FROM employees WHERE name = 'David Wilson'),
    'Engineering',
    80000.00
WHERE NOT EXISTS (SELECT 1 FROM employees WHERE name = 'Eva Brown')""")
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
        logger.error(f"Error in insert_employees_36: {e}")
        raise
    finally:
        logger.info("Completed insert_employees_36")

def sql_fn_insert_employees_37_7e7b9e(**kwargs):
    """Execute INSERT command 37"""
    logger = logging.getLogger(__name__)
    logger.info("Starting insert_employees_37")
    try:
        conn_id = get_postgres_connection_id()
        logger.info(f"Using PostgreSQL connection ID: {conn_id}")
        
        hook = PostgresHook(postgres_conn_id=conn_id)
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""INSERT INTO employees (name, position, manager_id, department, salary)
SELECT
    'Frank Miller',
    'Junior Developer',
    (SELECT id FROM employees WHERE name = 'David Wilson'),
    'Engineering',
    60000.00
WHERE NOT EXISTS (SELECT 1 FROM employees WHERE name = 'Frank Miller')""")
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
        logger.error(f"Error in insert_employees_37: {e}")
        raise
    finally:
        logger.info("Completed insert_employees_37")

def sql_fn_cte_query_38_f3d193(**kwargs):
    """Execute CTE command 38"""
    logger = logging.getLogger(__name__)
    logger.info("Starting cte_query_38")
    try:
        conn_id = get_postgres_connection_id()
        logger.info(f"Using PostgreSQL connection ID: {conn_id}")
        
        hook = PostgresHook(postgres_conn_id=conn_id)
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""WITH user_stats AS (
    SELECT
        user_id,
        COUNT(*) as order_count,
        SUM(amount) as total_amount,
        AVG(amount) as avg_order_amount,
        MIN(order_date) as first_order_date,
        MAX(order_date) as last_order_date
    FROM orders
    WHERE status = 'completed'
    GROUP BY user_id
)
SELECT
    u.username,
    u.email,
    us.order_count,
    us.total_amount,
    us.avg_order_amount,
    us.first_order_date,
    us.last_order_date
FROM users u
LEFT JOIN user_stats us ON u.id = us.user_id
ORDER BY us.total_amount DESC NULLS LAST""")
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
        logger.error(f"Error in cte_query_38: {e}")
        raise
    finally:
        logger.info("Completed cte_query_38")

def sql_fn_cte_query_39_0650ff(**kwargs):
    """Execute CTE command 39"""
    logger = logging.getLogger(__name__)
    logger.info("Starting cte_query_39")
    try:
        conn_id = get_postgres_connection_id()
        logger.info(f"Using PostgreSQL connection ID: {conn_id}")
        
        hook = PostgresHook(postgres_conn_id=conn_id)
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""WITH RECURSIVE employee_hierarchy AS (
    
    SELECT
        id,
        name,
        position,
        manager_id,
        department,
        salary,
        1 as level,
        ARRAY[id] as path,
        name::TEXT as hierarchy_path
    FROM employees
    WHERE manager_id IS NULL

    UNION ALL

    
    SELECT
        e.id,
        e.name,
        e.position,
        e.manager_id,
        e.department,
        e.salary,
        eh.level + 1 as level,
        eh.path || e.id as path,
        eh.hierarchy_path || ' -> ' || e.name as hierarchy_path
    FROM employees e
    JOIN employee_hierarchy eh ON e.manager_id = eh.id
)
SELECT
    level,
    name,
    position,
    department,
    salary,
    hierarchy_path
FROM employee_hierarchy
ORDER BY path""")
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
        logger.error(f"Error in cte_query_39: {e}")
        raise
    finally:
        logger.info("Completed cte_query_39")

def sql_fn_cte_query_40_8769fd(**kwargs):
    """Execute CTE command 40"""
    logger = logging.getLogger(__name__)
    logger.info("Starting cte_query_40")
    try:
        conn_id = get_postgres_connection_id()
        logger.info(f"Using PostgreSQL connection ID: {conn_id}")
        
        hook = PostgresHook(postgres_conn_id=conn_id)
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""WITH new_users AS (
    SELECT
        'alice_wonder' as username,
        'alice.wonder@example.com' as email
    UNION ALL
    SELECT
        'bob_marley' as username,
        'bob.marley@example.com' as email
    UNION ALL
    SELECT
        'charlie_chaplin' as username,
        'charlie.chaplin@example.com' as email
)
INSERT INTO users (username, email)
SELECT username, email
FROM new_users nu
WHERE NOT EXISTS (
    SELECT 1 FROM users u WHERE u.username = nu.username OR u.email = nu.email
)
RETURNING id, username, email, created_at""")
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
        logger.error(f"Error in cte_query_40: {e}")
        raise
    finally:
        logger.info("Completed cte_query_40")

def sql_fn_cte_query_41_7efc05(**kwargs):
    """Execute CTE command 41"""
    logger = logging.getLogger(__name__)
    logger.info("Starting cte_query_41")
    try:
        conn_id = get_postgres_connection_id()
        logger.info(f"Using PostgreSQL connection ID: {conn_id}")
        
        hook = PostgresHook(postgres_conn_id=conn_id)
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""WITH department_stats AS (
    SELECT
        department,
        COUNT(*) as employee_count,
        ROUND(AVG(salary), 2) as avg_salary,
        MIN(salary) as min_salary,
        MAX(salary) as max_salary,
        SUM(salary) as total_salary_budget
    FROM employees
    WHERE department IS NOT NULL
    GROUP BY department
)
SELECT
    ds.department,
    ds.employee_count,
    ds.avg_salary,
    ds.min_salary,
    ds.max_salary,
    ds.total_salary_budget,
    ROUND(ds.total_salary_budget / SUM(ds.total_salary_budget) OVER () * 100, 2) as budget_percentage
FROM department_stats ds
ORDER BY ds.total_salary_budget DESC""")
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
        logger.error(f"Error in cte_query_41: {e}")
        raise
    finally:
        logger.info("Completed cte_query_41")

def sql_fn_cte_query_42_b73c13(**kwargs):
    """Execute CTE command 42"""
    logger = logging.getLogger(__name__)
    logger.info("Starting cte_query_42")
    try:
        conn_id = get_postgres_connection_id()
        logger.info(f"Using PostgreSQL connection ID: {conn_id}")
        
        hook = PostgresHook(postgres_conn_id=conn_id)
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""WITH user_orders_summary AS (
    SELECT
        u.id as user_id,
        u.username,
        COUNT(o.order_id) as total_orders,
        COALESCE(SUM(o.amount), 0) as lifetime_value,
        MAX(o.order_date) as last_purchase_date
    FROM users u
    LEFT JOIN orders o ON u.id = o.user_id AND o.status = 'completed'
    GROUP BY u.id, u.username
)
UPDATE users u
SET
    email = CASE
        WHEN uos.lifetime_value > 1000 THEN REPLACE(u.email, '@example.com', '@vip.example.com')
        ELSE u.email
    END
FROM user_orders_summary uos
WHERE u.id = uos.user_id
AND uos.lifetime_value > 1000
RETURNING u.id, u.username, u.email, uos.lifetime_value""")
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
        logger.error(f"Error in cte_query_42: {e}")
        raise
    finally:
        logger.info("Completed cte_query_42")

with DAG(
    dag_id="generated_setup_database",
    default_args=default_args,
    start_date=datetime(2026, 1, 25),
    schedule_interval="@daily",
    catchup=False,
    tags=["cte", "analytics"]
) as dag:

    # SQL задачи:
    create_table_users_1 = PythonOperator(
        task_id="create_table_users_1",
        python_callable=sql_fn_create_table_users_1_df0c8c,
        doc_md="""Execute CREATE TABLE command 1""",
    )
    create_table_orders_2 = PythonOperator(
        task_id="create_table_orders_2",
        python_callable=sql_fn_create_table_orders_2_13d3c2,
        doc_md="""Execute CREATE TABLE command 2""",
    )
    drop_index_idx_users_email_3 = PythonOperator(
        task_id="drop_index_idx_users_email_3",
        python_callable=sql_fn_drop_index_idx_users_email_3_b628cc,
        doc_md="""Execute DROP INDEX command 3""",
    )
    create_index_idx_users_email_4 = PythonOperator(
        task_id="create_index_idx_users_email_4",
        python_callable=sql_fn_create_index_idx_users_email_4_b6cf3f,
        doc_md="""Execute CREATE INDEX command 4""",
    )
    drop_index_idx_orders_user_id_5 = PythonOperator(
        task_id="drop_index_idx_orders_user_id_5",
        python_callable=sql_fn_drop_index_idx_orders_user_id_5_d26952,
        doc_md="""Execute DROP INDEX command 5""",
    )
    create_index_idx_orders_user_id_6 = PythonOperator(
        task_id="create_index_idx_orders_user_id_6",
        python_callable=sql_fn_create_index_idx_orders_user_id_6_1a7e1d,
        doc_md="""Execute CREATE INDEX command 6""",
    )
    delete_orders_7 = PythonOperator(
        task_id="delete_orders_7",
        python_callable=sql_fn_delete_orders_7_c05a65,
        doc_md="""Execute DELETE command 7""",
    )
    delete_users_8 = PythonOperator(
        task_id="delete_users_8",
        python_callable=sql_fn_delete_users_8_9195b2,
        doc_md="""Execute DELETE command 8""",
    )
    insert_users_9 = PythonOperator(
        task_id="insert_users_9",
        python_callable=sql_fn_insert_users_9_e2cc4a,
        doc_md="""Execute INSERT command 9""",
    )
    update_users_10 = PythonOperator(
        task_id="update_users_10",
        python_callable=sql_fn_update_users_10_8cf6cb,
        doc_md="""Execute UPDATE command 10""",
    )
    select_users_11 = PythonOperator(
        task_id="select_users_11",
        python_callable=sql_fn_select_users_11_14f104,
        doc_md="""Execute SELECT command 11""",
    )
    select_orders_12 = PythonOperator(
        task_id="select_orders_12",
        python_callable=sql_fn_select_orders_12_b082a5,
        doc_md="""Execute SELECT command 12""",
    )
    cte_query_13 = PythonOperator(
        task_id="cte_query_13",
        python_callable=sql_fn_cte_query_13_75369b,
        doc_md="""Execute CTE command 13""",
    )
    create_table_users_14 = PythonOperator(
        task_id="create_table_users_14",
        python_callable=sql_fn_create_table_users_14_df0c8c,
        doc_md="""Execute CREATE TABLE command 14""",
    )
    create_table_orders_15 = PythonOperator(
        task_id="create_table_orders_15",
        python_callable=sql_fn_create_table_orders_15_4a550f,
        doc_md="""Execute CREATE TABLE command 15""",
    )
    create_table_employees_16 = PythonOperator(
        task_id="create_table_employees_16",
        python_callable=sql_fn_create_table_employees_16_8a06ed,
        doc_md="""Execute CREATE TABLE command 16""",
    )
    drop_index_idx_orders_user_id_17 = PythonOperator(
        task_id="drop_index_idx_orders_user_id_17",
        python_callable=sql_fn_drop_index_idx_orders_user_id_17_d26952,
        doc_md="""Execute DROP INDEX command 17""",
    )
    create_index_idx_orders_user_id_18 = PythonOperator(
        task_id="create_index_idx_orders_user_id_18",
        python_callable=sql_fn_create_index_idx_orders_user_id_18_e791ec,
        doc_md="""Execute CREATE INDEX command 18""",
    )
    drop_index_idx_orders_order_date_19 = PythonOperator(
        task_id="drop_index_idx_orders_order_date_19",
        python_callable=sql_fn_drop_index_idx_orders_order_date_19_acc162,
        doc_md="""Execute DROP INDEX command 19""",
    )
    create_index_idx_orders_order_date_20 = PythonOperator(
        task_id="create_index_idx_orders_order_date_20",
        python_callable=sql_fn_create_index_idx_orders_order_date_20_66b9a9,
        doc_md="""Execute CREATE INDEX command 20""",
    )
    drop_index_idx_employees_manager_id_21 = PythonOperator(
        task_id="drop_index_idx_employees_manager_id_21",
        python_callable=sql_fn_drop_index_idx_employees_manager_id_21_26b802,
        doc_md="""Execute DROP INDEX command 21""",
    )
    create_index_idx_employees_manager_id_22 = PythonOperator(
        task_id="create_index_idx_employees_manager_id_22",
        python_callable=sql_fn_create_index_idx_employees_manager_id_22_5f8ca4,
        doc_md="""Execute CREATE INDEX command 22""",
    )
    drop_index_idx_users_email_23 = PythonOperator(
        task_id="drop_index_idx_users_email_23",
        python_callable=sql_fn_drop_index_idx_users_email_23_b628cc,
        doc_md="""Execute DROP INDEX command 23""",
    )
    create_index_idx_users_email_24 = PythonOperator(
        task_id="create_index_idx_users_email_24",
        python_callable=sql_fn_create_index_idx_users_email_24_7b1524,
        doc_md="""Execute CREATE INDEX command 24""",
    )
    insert_users_25 = PythonOperator(
        task_id="insert_users_25",
        python_callable=sql_fn_insert_users_25_a5af0d,
        doc_md="""Execute INSERT command 25""",
    )
    insert_users_26 = PythonOperator(
        task_id="insert_users_26",
        python_callable=sql_fn_insert_users_26_90f0ea,
        doc_md="""Execute INSERT command 26""",
    )
    insert_users_27 = PythonOperator(
        task_id="insert_users_27",
        python_callable=sql_fn_insert_users_27_8a576d,
        doc_md="""Execute INSERT command 27""",
    )
    insert_orders_28 = PythonOperator(
        task_id="insert_orders_28",
        python_callable=sql_fn_insert_orders_28_07229b,
        doc_md="""Execute INSERT command 28""",
    )
    insert_orders_29 = PythonOperator(
        task_id="insert_orders_29",
        python_callable=sql_fn_insert_orders_29_f8de50,
        doc_md="""Execute INSERT command 29""",
    )
    insert_orders_30 = PythonOperator(
        task_id="insert_orders_30",
        python_callable=sql_fn_insert_orders_30_105ca9,
        doc_md="""Execute INSERT command 30""",
    )
    insert_orders_31 = PythonOperator(
        task_id="insert_orders_31",
        python_callable=sql_fn_insert_orders_31_88828c,
        doc_md="""Execute INSERT command 31""",
    )
    insert_employees_32 = PythonOperator(
        task_id="insert_employees_32",
        python_callable=sql_fn_insert_employees_32_8be146,
        doc_md="""Execute INSERT command 32""",
    )
    insert_employees_33 = PythonOperator(
        task_id="insert_employees_33",
        python_callable=sql_fn_insert_employees_33_a1fdc4,
        doc_md="""Execute INSERT command 33""",
    )
    insert_employees_34 = PythonOperator(
        task_id="insert_employees_34",
        python_callable=sql_fn_insert_employees_34_121632,
        doc_md="""Execute INSERT command 34""",
    )
    insert_employees_35 = PythonOperator(
        task_id="insert_employees_35",
        python_callable=sql_fn_insert_employees_35_ecdd81,
        doc_md="""Execute INSERT command 35""",
    )
    insert_employees_36 = PythonOperator(
        task_id="insert_employees_36",
        python_callable=sql_fn_insert_employees_36_b0285c,
        doc_md="""Execute INSERT command 36""",
    )
    insert_employees_37 = PythonOperator(
        task_id="insert_employees_37",
        python_callable=sql_fn_insert_employees_37_7e7b9e,
        doc_md="""Execute INSERT command 37""",
    )
    cte_query_38 = PythonOperator(
        task_id="cte_query_38",
        python_callable=sql_fn_cte_query_38_f3d193,
        doc_md="""Execute CTE command 38""",
    )
    cte_query_39 = PythonOperator(
        task_id="cte_query_39",
        python_callable=sql_fn_cte_query_39_0650ff,
        doc_md="""Execute CTE command 39""",
    )
    cte_query_40 = PythonOperator(
        task_id="cte_query_40",
        python_callable=sql_fn_cte_query_40_8769fd,
        doc_md="""Execute CTE command 40""",
    )
    cte_query_41 = PythonOperator(
        task_id="cte_query_41",
        python_callable=sql_fn_cte_query_41_7efc05,
        doc_md="""Execute CTE command 41""",
    )
    cte_query_42 = PythonOperator(
        task_id="cte_query_42",
        python_callable=sql_fn_cte_query_42_b73c13,
        doc_md="""Execute CTE command 42""",
    )

    # Линейные зависимости для последовательного выполнения
    create_table_users_1 >> create_table_orders_2 >> drop_index_idx_users_email_3 >> create_index_idx_users_email_4 >> drop_index_idx_orders_user_id_5 >> create_index_idx_orders_user_id_6 >> delete_orders_7 >> delete_users_8 >> insert_users_9 >> update_users_10 >> select_users_11 >> select_orders_12 >> cte_query_13 >> create_table_users_14 >> create_table_orders_15 >> create_table_employees_16 >> drop_index_idx_orders_user_id_17 >> create_index_idx_orders_user_id_18 >> drop_index_idx_orders_order_date_19 >> create_index_idx_orders_order_date_20 >> drop_index_idx_employees_manager_id_21 >> create_index_idx_employees_manager_id_22 >> drop_index_idx_users_email_23 >> create_index_idx_users_email_24 >> insert_users_25 >> insert_users_26 >> insert_users_27 >> insert_orders_28 >> insert_orders_29 >> insert_orders_30 >> insert_orders_31 >> insert_employees_32 >> insert_employees_33 >> insert_employees_34 >> insert_employees_35 >> insert_employees_36 >> insert_employees_37 >> cte_query_38 >> cte_query_39 >> cte_query_40 >> cte_query_41 >> cte_query_42