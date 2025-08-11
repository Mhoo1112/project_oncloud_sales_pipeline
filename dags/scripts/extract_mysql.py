import os
import pandas
import sqlalchemy
from airflow.models import Variable
from airflow.hooks.base import BaseHook
from google.cloud import storage
import datetime
from scripts.log_start_log_end import log_start
from scripts.log_start_log_end import log_end


def extract_mysql():
    """
    อ่านข้อมูลจาก MySQL ชื่อ Data Base: project_sales_pipeline/
    """
    log_start()

    # ค่าพารามิเตอร์อื่นๆ
    tmp_path = f"/tmp"
    output_path = f"output/"

    # ดึงข้อมูลจาก Variable
    bucket_name = Variable.get("GCP_BUCKET_NAME")

    # host = Variable.get("MYSQL_HOST")
    # port = Variable.get("MYSQL_PORT")
    # database = Variable.get("MYSQL_DATABASE")
    # username = Variable.get("MYSQL_USER")
    # password = Variable.get("MYSQL_PASSWORD")

    # ตั้งค่า Google client
    client = storage.Client()
    bucket = client.get_bucket(bucket_name)

    # สร้าง connection string
    # engine = sqlalchemy.create_engine(
    #     f"mysql+pymysql://{username}:{password}@{host}:{port}/{database}")

    # สร้าง connection string
    # เพิ่ม connect_timeout และ read_timeout เข้าไปใน Connection string
    # ดึงค่าจาก Admin > Connections (conn_id = mysql_cloudsql)
    
    conn = BaseHook.get_connection("mysql_cloudsql")
    url = f"mysql+pymysql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}"
    engine = sqlalchemy.create_engine(
        url + "?connect_timeout=10&read_timeout=30",
        pool_pre_ping=True,
        pool_recycle=3600,
    )

    query_df = pandas.read_sql("""
    SELECT
        CONCAT('C', LPAD(c.customer_id, 3, '0')) AS customer_id,   -- แปลงเป็น C001
        c.customer_name,
        c.customer_email,
        p.product_id,
        p.product_name,
        p.price
        FROM customers c
        JOIN products p ON c.customer_id = p.customer_id;
        """, engine)

    query_df["extracted_date"] = pandas.Timestamp.now().strftime("%Y-%m-%d")
    print("\n▶️ DataFrame from MySQL --------------------")
    print(query_df.head())
    print("\n▶️ Describe from MySQL")
    print(query_df.describe())
    print("\n▶️ Dtypes from MySQL")
    print(query_df.dtypes)

    try:
        # บันทึกข้อมูลลงเป็นไฟล์ CSV และ parquet
        today = datetime.date.today().strftime("%Y-%m-%d")
        out_csv_local = os.path.join(tmp_path, f"mysql_customers_products_{today}.csv")
        out_parq_local = os.path.join(tmp_path, f"mysql_customers_products_{today}.parquet")

        query_df.to_csv(out_csv_local, index=False)
        query_df.to_parquet(out_parq_local)

        print(f"💾 Saved: {out_csv_local}")
        print(f"💾 Saved: {out_parq_local}")

        # อัพโหลดไปไฟล์ปลายทาง ชื่อ mysql_customers_ ... จาก /tpm ชื่อ mysql_customers_ ... 
        bucket.blob(os.path.join(
            output_path, f"mysql_customers_products_{today}.csv")).upload_from_filename(out_csv_local)
        bucket.blob(os.path.join(
            output_path, f"mysql_customers_products_{today}.parquet")).upload_from_filename(out_parq_local)

    except Exception as e:
        print(e)

    engine.dispose()
    log_end()
    return query_df
