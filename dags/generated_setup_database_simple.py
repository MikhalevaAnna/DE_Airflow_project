"""
Auto-generated DAG from setup_database_simple.sql
Generated: 2026-01-25 18:03:57
Total tasks: 7
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable

# Default arguments for the DAG
default_args = {
    "owner": "dba",
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

def sql_fn_drop_table_users_main_1_d23f73(**kwargs):
    """Execute DROP TABLE command 1"""
    logger = logging.getLogger(__name__)
    logger.info("Starting drop_table_users_main_1")
    try:
        conn_id = get_postgres_connection_id()
        logger.info(f"Using PostgreSQL connection ID: {conn_id}")
        
        hook = PostgresHook(postgres_conn_id=conn_id)
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""DROP TABLE IF EXISTS users_main""")
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
        logger.error(f"Error in drop_table_users_main_1: {e}")
        raise
    finally:
        logger.info("Completed drop_table_users_main_1")

def sql_fn_create_table_users_main_2_a55630(**kwargs):
    """Execute CREATE TABLE command 2"""
    logger = logging.getLogger(__name__)
    logger.info("Starting create_table_users_main_2")
    try:
        conn_id = get_postgres_connection_id()
        logger.info(f"Using PostgreSQL connection ID: {conn_id}")
        
        hook = PostgresHook(postgres_conn_id=conn_id)
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""CREATE TABLE IF NOT EXISTS users_main (
    id SERIAL PRIMARY KEY,
    username VARCHAR(50) UNIQUE NOT NULL,
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
        logger.error(f"Error in create_table_users_main_2: {e}")
        raise
    finally:
        logger.info("Completed create_table_users_main_2")

def sql_fn_drop_table_orders_main_3_c00163(**kwargs):
    """Execute DROP TABLE command 3"""
    logger = logging.getLogger(__name__)
    logger.info("Starting drop_table_orders_main_3")
    try:
        conn_id = get_postgres_connection_id()
        logger.info(f"Using PostgreSQL connection ID: {conn_id}")
        
        hook = PostgresHook(postgres_conn_id=conn_id)
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""DROP TABLE IF EXISTS orders_main""")
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
        logger.error(f"Error in drop_table_orders_main_3: {e}")
        raise
    finally:
        logger.info("Completed drop_table_orders_main_3")

def sql_fn_create_table_orders_main_4_6b82ea(**kwargs):
    """Execute CREATE TABLE command 4"""
    logger = logging.getLogger(__name__)
    logger.info("Starting create_table_orders_main_4")
    try:
        conn_id = get_postgres_connection_id()
        logger.info(f"Using PostgreSQL connection ID: {conn_id}")
        
        hook = PostgresHook(postgres_conn_id=conn_id)
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""CREATE TABLE IF NOT EXISTS orders_main (
    id SERIAL PRIMARY KEY,
    user_id INT REFERENCES users(id),
    amount DECIMAL(10,2),
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
        logger.error(f"Error in create_table_orders_main_4: {e}")
        raise
    finally:
        logger.info("Completed create_table_orders_main_4")

def sql_fn_create_index_idx_orders_main_user_id_5_b20dfd(**kwargs):
    """Execute CREATE INDEX command 5"""
    logger = logging.getLogger(__name__)
    logger.info("Starting create_index_idx_orders_main_user_id_5")
    try:
        conn_id = get_postgres_connection_id()
        logger.info(f"Using PostgreSQL connection ID: {conn_id}")
        
        hook = PostgresHook(postgres_conn_id=conn_id)
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""CREATE INDEX idx_orders_main_user_id ON orders_main(user_id)""")
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
        logger.error(f"Error in create_index_idx_orders_main_user_id_5: {e}")
        raise
    finally:
        logger.info("Completed create_index_idx_orders_main_user_id_5")

def sql_fn_create_index_idx_orders_main_created_at_6_18ce90(**kwargs):
    """Execute CREATE INDEX command 6"""
    logger = logging.getLogger(__name__)
    logger.info("Starting create_index_idx_orders_main_created_at_6")
    try:
        conn_id = get_postgres_connection_id()
        logger.info(f"Using PostgreSQL connection ID: {conn_id}")
        
        hook = PostgresHook(postgres_conn_id=conn_id)
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""CREATE INDEX idx_orders_main_created_at ON orders_main(created_at)""")
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
        logger.error(f"Error in create_index_idx_orders_main_created_at_6: {e}")
        raise
    finally:
        logger.info("Completed create_index_idx_orders_main_created_at_6")

def sql_fn_insert_users_main_7_79d85f(**kwargs):
    """Execute INSERT command 7"""
    logger = logging.getLogger(__name__)
    logger.info("Starting insert_users_main_7")
    try:
        conn_id = get_postgres_connection_id()
        logger.info(f"Using PostgreSQL connection ID: {conn_id}")
        
        hook = PostgresHook(postgres_conn_id=conn_id)
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""INSERT INTO users_main (username) VALUES ('admin'), ('user1')""")
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
        logger.error(f"Error in insert_users_main_7: {e}")
        raise
    finally:
        logger.info("Completed insert_users_main_7")

with DAG(
    dag_id="generated_setup_database_simple",
    default_args=default_args,
    start_date=datetime(2026, 1, 25),
    schedule_interval="@once",
    catchup=False,
    tags=["setup", "database"],
    description="Настройка базы данных"
) as dag:

    # SQL задачи:
    drop_table_users_main_1 = PythonOperator(
        task_id="drop_table_users_main_1",
        python_callable=sql_fn_drop_table_users_main_1_d23f73,
        doc_md="""Execute DROP TABLE command 1""",
    )
    create_table_users_main_2 = PythonOperator(
        task_id="create_table_users_main_2",
        python_callable=sql_fn_create_table_users_main_2_a55630,
        doc_md="""Execute CREATE TABLE command 2""",
    )
    drop_table_orders_main_3 = PythonOperator(
        task_id="drop_table_orders_main_3",
        python_callable=sql_fn_drop_table_orders_main_3_c00163,
        doc_md="""Execute DROP TABLE command 3""",
    )
    create_table_orders_main_4 = PythonOperator(
        task_id="create_table_orders_main_4",
        python_callable=sql_fn_create_table_orders_main_4_6b82ea,
        doc_md="""Execute CREATE TABLE command 4""",
    )
    create_index_idx_orders_main_user_id_5 = PythonOperator(
        task_id="create_index_idx_orders_main_user_id_5",
        python_callable=sql_fn_create_index_idx_orders_main_user_id_5_b20dfd,
        doc_md="""Execute CREATE INDEX command 5""",
    )
    create_index_idx_orders_main_created_at_6 = PythonOperator(
        task_id="create_index_idx_orders_main_created_at_6",
        python_callable=sql_fn_create_index_idx_orders_main_created_at_6_18ce90,
        doc_md="""Execute CREATE INDEX command 6""",
    )
    insert_users_main_7 = PythonOperator(
        task_id="insert_users_main_7",
        python_callable=sql_fn_insert_users_main_7_79d85f,
        doc_md="""Execute INSERT command 7""",
    )

    # Линейные зависимости для последовательного выполнения
    drop_table_users_main_1 >> create_table_users_main_2 >> drop_table_orders_main_3 >> create_table_orders_main_4 >> create_index_idx_orders_main_user_id_5 >> create_index_idx_orders_main_created_at_6 >> insert_users_main_7