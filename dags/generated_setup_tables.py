"""
Auto-generated DAG from setup_tables.sql
Generated: 2026-01-25 18:03:57
Total tasks: 5
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable

# Default arguments for the DAG
default_args = {
    "owner": "admin",
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

def sql_fn_drop_table_users_my_1_a6ce42(**kwargs):
    """Execute DROP TABLE command 1"""
    logger = logging.getLogger(__name__)
    logger.info("Starting drop_table_users_my_1")
    try:
        conn_id = get_postgres_connection_id()
        logger.info(f"Using PostgreSQL connection ID: {conn_id}")
        
        hook = PostgresHook(postgres_conn_id=conn_id)
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""DROP TABLE IF EXISTS users_my""")
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
        logger.error(f"Error in drop_table_users_my_1: {e}")
        raise
    finally:
        logger.info("Completed drop_table_users_my_1")

def sql_fn_create_table_users_my_2_f713bb(**kwargs):
    """Execute CREATE TABLE command 2"""
    logger = logging.getLogger(__name__)
    logger.info("Starting create_table_users_my_2")
    try:
        conn_id = get_postgres_connection_id()
        logger.info(f"Using PostgreSQL connection ID: {conn_id}")
        
        hook = PostgresHook(postgres_conn_id=conn_id)
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""CREATE TABLE users_my (
    id SERIAL PRIMARY KEY,
    username VARCHAR(50) UNIQUE NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
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
        logger.error(f"Error in create_table_users_my_2: {e}")
        raise
    finally:
        logger.info("Completed create_table_users_my_2")

def sql_fn_drop_index_idx_users_my_email_3_405645(**kwargs):
    """Execute DROP INDEX command 3"""
    logger = logging.getLogger(__name__)
    logger.info("Starting drop_index_idx_users_my_email_3")
    try:
        conn_id = get_postgres_connection_id()
        logger.info(f"Using PostgreSQL connection ID: {conn_id}")
        
        hook = PostgresHook(postgres_conn_id=conn_id)
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""DROP INDEX IF EXISTS  idx_users_my_email""")
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
        logger.error(f"Error in drop_index_idx_users_my_email_3: {e}")
        raise
    finally:
        logger.info("Completed drop_index_idx_users_my_email_3")

def sql_fn_create_index_idx_users_my_email_4_de449e(**kwargs):
    """Execute CREATE INDEX command 4"""
    logger = logging.getLogger(__name__)
    logger.info("Starting create_index_idx_users_my_email_4")
    try:
        conn_id = get_postgres_connection_id()
        logger.info(f"Using PostgreSQL connection ID: {conn_id}")
        
        hook = PostgresHook(postgres_conn_id=conn_id)
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""CREATE INDEX idx_users_my_email ON users_my(email)""")
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
        logger.error(f"Error in create_index_idx_users_my_email_4: {e}")
        raise
    finally:
        logger.info("Completed create_index_idx_users_my_email_4")

def sql_fn_insert_users_my_5_d37acd(**kwargs):
    """Execute INSERT command 5"""
    logger = logging.getLogger(__name__)
    logger.info("Starting insert_users_my_5")
    try:
        conn_id = get_postgres_connection_id()
        logger.info(f"Using PostgreSQL connection ID: {conn_id}")
        
        hook = PostgresHook(postgres_conn_id=conn_id)
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""INSERT INTO users_my (username, email)
VALUES ('admin', 'admin@example.com')""")
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
        logger.error(f"Error in insert_users_my_5: {e}")
        raise
    finally:
        logger.info("Completed insert_users_my_5")

with DAG(
    dag_id="generated_setup_tables",
    default_args=default_args,
    start_date=datetime(2026, 1, 25),
    schedule_interval="@once",
    catchup=False,
    tags=["setup", "database"],
    description="Initial database setup"
) as dag:

    # SQL задачи:
    drop_table_users_my_1 = PythonOperator(
        task_id="drop_table_users_my_1",
        python_callable=sql_fn_drop_table_users_my_1_a6ce42,
        doc_md="""Execute DROP TABLE command 1""",
    )
    create_table_users_my_2 = PythonOperator(
        task_id="create_table_users_my_2",
        python_callable=sql_fn_create_table_users_my_2_f713bb,
        doc_md="""Execute CREATE TABLE command 2""",
    )
    drop_index_idx_users_my_email_3 = PythonOperator(
        task_id="drop_index_idx_users_my_email_3",
        python_callable=sql_fn_drop_index_idx_users_my_email_3_405645,
        doc_md="""Execute DROP INDEX command 3""",
    )
    create_index_idx_users_my_email_4 = PythonOperator(
        task_id="create_index_idx_users_my_email_4",
        python_callable=sql_fn_create_index_idx_users_my_email_4_de449e,
        doc_md="""Execute CREATE INDEX command 4""",
    )
    insert_users_my_5 = PythonOperator(
        task_id="insert_users_my_5",
        python_callable=sql_fn_insert_users_my_5_d37acd,
        doc_md="""Execute INSERT command 5""",
    )

    # Линейные зависимости для последовательного выполнения
    drop_table_users_my_1 >> create_table_users_my_2 >> drop_index_idx_users_my_email_3 >> create_index_idx_users_my_email_4 >> insert_users_my_5