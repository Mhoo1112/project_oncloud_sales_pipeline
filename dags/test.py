from datetime import datetime
from airflow import DAG
from airflow.providers.mysql.operators.mysql import MySqlOperator

with DAG(
    dag_id="test_cloudsql_conn",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["test","cloudsql"],
) as dag:

    ping = MySqlOperator(
        task_id="ping",
        mysql_conn_id="mysql_cloudsql",   # ใช้ Admin > Connections ที่เราตั้งไว้
        sql="SELECT 1;"
    )

    join_count = MySqlOperator(
        task_id="join_count",
        mysql_conn_id="mysql_cloudsql",
        sql="""
        SELECT COUNT(*) AS rows_joined
        FROM customers c
        INNER JOIN products p ON c.customer_id = p.customer_id;
        """,
        do_xcom_push=True,
    )

    ping >> join_count