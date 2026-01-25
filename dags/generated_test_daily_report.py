"""
Auto-generated DAG from test_daily_report.sql
Generated: 2026-01-25 18:03:57
Total tasks: 11
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable

# Default arguments for the DAG
default_args = {
    "owner": "analytics",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(seconds=300),
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

def sql_fn_drop_table_orders_info_1_08f484(**kwargs):
    """Execute DROP TABLE command 1"""
    logger = logging.getLogger(__name__)
    logger.info("Starting drop_table_orders_info_1")
    try:
        conn_id = get_postgres_connection_id()
        logger.info(f"Using PostgreSQL connection ID: {conn_id}")
        
        hook = PostgresHook(postgres_conn_id=conn_id)
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""DROP TABLE IF EXISTS orders_info""")
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
        logger.error(f"Error in drop_table_orders_info_1: {e}")
        raise
    finally:
        logger.info("Completed drop_table_orders_info_1")

def sql_fn_create_table_orders_info_2_fac6a3(**kwargs):
    """Execute CREATE TABLE command 2"""
    logger = logging.getLogger(__name__)
    logger.info("Starting create_table_orders_info_2")
    try:
        conn_id = get_postgres_connection_id()
        logger.info(f"Using PostgreSQL connection ID: {conn_id}")
        
        hook = PostgresHook(postgres_conn_id=conn_id)
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""CREATE TABLE IF NOT EXISTS orders_info (
    id SERIAL PRIMARY KEY,
    user_id INT NOT NULL,
    amount DECIMAL(10,2) NOT NULL,
    status VARCHAR(20) DEFAULT 'completed',
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
        logger.error(f"Error in create_table_orders_info_2: {e}")
        raise
    finally:
        logger.info("Completed create_table_orders_info_2")

def sql_fn_drop_table_daily_reports_3_885735(**kwargs):
    """Execute DROP TABLE command 3"""
    logger = logging.getLogger(__name__)
    logger.info("Starting drop_table_daily_reports_3")
    try:
        conn_id = get_postgres_connection_id()
        logger.info(f"Using PostgreSQL connection ID: {conn_id}")
        
        hook = PostgresHook(postgres_conn_id=conn_id)
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""DROP TABLE IF EXISTS daily_reports""")
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
        logger.error(f"Error in drop_table_daily_reports_3: {e}")
        raise
    finally:
        logger.info("Completed drop_table_daily_reports_3")

def sql_fn_create_table_daily_reports_4_4f5851(**kwargs):
    """Execute CREATE TABLE command 4"""
    logger = logging.getLogger(__name__)
    logger.info("Starting create_table_daily_reports_4")
    try:
        conn_id = get_postgres_connection_id()
        logger.info(f"Using PostgreSQL connection ID: {conn_id}")
        
        hook = PostgresHook(postgres_conn_id=conn_id)
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""CREATE TABLE IF NOT EXISTS daily_reports (
    id SERIAL PRIMARY KEY,
    report_date DATE NOT NULL UNIQUE,
    total_orders INT NOT NULL,
    total_revenue DECIMAL(12,2) NOT NULL,
    average_order_value DECIMAL(10,2),
    generated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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
        logger.error(f"Error in create_table_daily_reports_4: {e}")
        raise
    finally:
        logger.info("Completed create_table_daily_reports_4")

def sql_fn_insert_orders_info_5_ad8852(**kwargs):
    """Execute INSERT command 5"""
    logger = logging.getLogger(__name__)
    logger.info("Starting insert_orders_info_5")
    try:
        conn_id = get_postgres_connection_id()
        logger.info(f"Using PostgreSQL connection ID: {conn_id}")
        
        hook = PostgresHook(postgres_conn_id=conn_id)
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""INSERT INTO orders_info (user_id, amount, created_at) VALUES
(1, 100.50, CURRENT_DATE - INTERVAL '1 day' + INTERVAL '10:30'),
(2, 75.25, CURRENT_DATE - INTERVAL '1 day' + INTERVAL '14:15'),
(3, 200.00, CURRENT_DATE - INTERVAL '1 day' + INTERVAL '16:45'),
(1, 50.00, CURRENT_DATE - INTERVAL '1 day' + INTERVAL '20:10'),
(4, 300.75, CURRENT_DATE - INTERVAL '1 day' + INTERVAL '22:30')""")
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
        logger.error(f"Error in insert_orders_info_5: {e}")
        raise
    finally:
        logger.info("Completed insert_orders_info_5")

def sql_fn_insert_orders_info_6_4cd4d5(**kwargs):
    """Execute INSERT command 6"""
    logger = logging.getLogger(__name__)
    logger.info("Starting insert_orders_info_6")
    try:
        conn_id = get_postgres_connection_id()
        logger.info(f"Using PostgreSQL connection ID: {conn_id}")
        
        hook = PostgresHook(postgres_conn_id=conn_id)
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""INSERT INTO orders_info (user_id, amount, created_at) VALUES
(5, 125.00, CURRENT_DATE - INTERVAL '2 days' + INTERVAL '09:00'),
(2, 80.50, CURRENT_DATE - INTERVAL '2 days' + INTERVAL '15:20')""")
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
        logger.error(f"Error in insert_orders_info_6: {e}")
        raise
    finally:
        logger.info("Completed insert_orders_info_6")

def sql_fn_insert_orders_info_7_c21d11(**kwargs):
    """Execute INSERT command 7"""
    logger = logging.getLogger(__name__)
    logger.info("Starting insert_orders_info_7")
    try:
        conn_id = get_postgres_connection_id()
        logger.info(f"Using PostgreSQL connection ID: {conn_id}")
        
        hook = PostgresHook(postgres_conn_id=conn_id)
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""INSERT INTO orders_info (user_id, amount, created_at) VALUES
(3, 150.00, CURRENT_DATE + INTERVAL '8 hours'),
(6, 95.30, CURRENT_DATE + INTERVAL '12 hours')""")
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
        logger.error(f"Error in insert_orders_info_7: {e}")
        raise
    finally:
        logger.info("Completed insert_orders_info_7")

def sql_fn_create_index_idx_orders_info_created_at_8_9f21af(**kwargs):
    """Execute CREATE INDEX command 8"""
    logger = logging.getLogger(__name__)
    logger.info("Starting create_index_idx_orders_info_created_at_8")
    try:
        conn_id = get_postgres_connection_id()
        logger.info(f"Using PostgreSQL connection ID: {conn_id}")
        
        hook = PostgresHook(postgres_conn_id=conn_id)
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""CREATE INDEX IF NOT EXISTS idx_orders_info_created_at ON orders_info(created_at)""")
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
        logger.error(f"Error in create_index_idx_orders_info_created_at_8: {e}")
        raise
    finally:
        logger.info("Completed create_index_idx_orders_info_created_at_8")

def sql_fn_create_index_idx_orders_info_user_created_9_540241(**kwargs):
    """Execute CREATE INDEX command 9"""
    logger = logging.getLogger(__name__)
    logger.info("Starting create_index_idx_orders_info_user_created_9")
    try:
        conn_id = get_postgres_connection_id()
        logger.info(f"Using PostgreSQL connection ID: {conn_id}")
        
        hook = PostgresHook(postgres_conn_id=conn_id)
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""CREATE INDEX IF NOT EXISTS idx_orders_info_user_created ON orders_info(user_id, created_at)""")
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
        logger.error(f"Error in create_index_idx_orders_info_user_created_9: {e}")
        raise
    finally:
        logger.info("Completed create_index_idx_orders_info_user_created_9")

def sql_fn_cte_query_10_f86f84(**kwargs):
    """Execute CTE command 10"""
    logger = logging.getLogger(__name__)
    logger.info("Starting cte_query_10")
    try:
        conn_id = get_postgres_connection_id()
        logger.info(f"Using PostgreSQL connection ID: {conn_id}")
        
        hook = PostgresHook(postgres_conn_id=conn_id)
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""WITH daily_stats AS (
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
    generated_at = CURRENT_TIMESTAMP""")
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
        logger.error(f"Error in cte_query_10: {e}")
        raise
    finally:
        logger.info("Completed cte_query_10")

def sql_fn_update_daily_reports_11_4b34a0(**kwargs):
    """Execute UPDATE command 11"""
    logger = logging.getLogger(__name__)
    logger.info("Starting update_daily_reports_11")
    try:
        conn_id = get_postgres_connection_id()
        logger.info(f"Using PostgreSQL connection ID: {conn_id}")
        
        hook = PostgresHook(postgres_conn_id=conn_id)
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""UPDATE daily_reports
SET average_order_value = total_revenue / NULLIF(total_orders, 0)
WHERE report_date = CURRENT_DATE - INTERVAL '1 day'""")
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
        logger.error(f"Error in update_daily_reports_11: {e}")
        raise
    finally:
        logger.info("Completed update_daily_reports_11")

with DAG(
    dag_id="generated_test_daily_report",
    default_args=default_args,
    start_date=datetime(2026, 1, 25),
    schedule_interval="0 1 * * *",
    catchup=False,
    tags=["reporting", "analytics", "sales"],
    description="Ежедневный отчет по продажам"
) as dag:

    # SQL задачи:
    drop_table_orders_info_1 = PythonOperator(
        task_id="drop_table_orders_info_1",
        python_callable=sql_fn_drop_table_orders_info_1_08f484,
        doc_md="""Execute DROP TABLE command 1""",
    )
    create_table_orders_info_2 = PythonOperator(
        task_id="create_table_orders_info_2",
        python_callable=sql_fn_create_table_orders_info_2_fac6a3,
        doc_md="""Execute CREATE TABLE command 2""",
    )
    drop_table_daily_reports_3 = PythonOperator(
        task_id="drop_table_daily_reports_3",
        python_callable=sql_fn_drop_table_daily_reports_3_885735,
        doc_md="""Execute DROP TABLE command 3""",
    )
    create_table_daily_reports_4 = PythonOperator(
        task_id="create_table_daily_reports_4",
        python_callable=sql_fn_create_table_daily_reports_4_4f5851,
        doc_md="""Execute CREATE TABLE command 4""",
    )
    insert_orders_info_5 = PythonOperator(
        task_id="insert_orders_info_5",
        python_callable=sql_fn_insert_orders_info_5_ad8852,
        doc_md="""Execute INSERT command 5""",
    )
    insert_orders_info_6 = PythonOperator(
        task_id="insert_orders_info_6",
        python_callable=sql_fn_insert_orders_info_6_4cd4d5,
        doc_md="""Execute INSERT command 6""",
    )
    insert_orders_info_7 = PythonOperator(
        task_id="insert_orders_info_7",
        python_callable=sql_fn_insert_orders_info_7_c21d11,
        doc_md="""Execute INSERT command 7""",
    )
    create_index_idx_orders_info_created_at_8 = PythonOperator(
        task_id="create_index_idx_orders_info_created_at_8",
        python_callable=sql_fn_create_index_idx_orders_info_created_at_8_9f21af,
        doc_md="""Execute CREATE INDEX command 8""",
    )
    create_index_idx_orders_info_user_created_9 = PythonOperator(
        task_id="create_index_idx_orders_info_user_created_9",
        python_callable=sql_fn_create_index_idx_orders_info_user_created_9_540241,
        doc_md="""Execute CREATE INDEX command 9""",
    )
    cte_query_10 = PythonOperator(
        task_id="cte_query_10",
        python_callable=sql_fn_cte_query_10_f86f84,
        doc_md="""Execute CTE command 10""",
    )
    update_daily_reports_11 = PythonOperator(
        task_id="update_daily_reports_11",
        python_callable=sql_fn_update_daily_reports_11_4b34a0,
        doc_md="""Execute UPDATE command 11""",
    )

    # Линейные зависимости для последовательного выполнения
    drop_table_orders_info_1 >> create_table_orders_info_2 >> drop_table_daily_reports_3 >> create_table_daily_reports_4 >> insert_orders_info_5 >> insert_orders_info_6 >> insert_orders_info_7 >> create_index_idx_orders_info_created_at_8 >> create_index_idx_orders_info_user_created_9 >> cte_query_10 >> update_daily_reports_11