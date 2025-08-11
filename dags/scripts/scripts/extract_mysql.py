import os
import pandas
import sqlalchemy
# import dotenv
from airflow.models import Variable
import datetime
from scripts.log_start_log_end import log_start
from scripts.log_start_log_end import log_end


def extract_mysql():
    """
    อ่านข้อมูลจาก MySQL ชื่อ Data Base: project_sales_pipeline/
    """
    log_start()
    # dotenv_path = os.path.join(os.path.dirname(__file__), "..", "config", ".env")
    # dotenv.load_dotenv(dotenv_path)

    # เชื่อมต่อเข้ากับ MySQL
    # host = os.getenv("MYSQL_HOST")
    # port = os.getenv("MYSQL_PORT")
    # database = os.getenv("MYSQL_DATABASE")
    # username = os.getenv("MYSQL_USER")
    # password = os.getenv("MYSQL_PASSWORD")

    host = Variable.get("MYSQL_HOST")
    port = Variable.get("MYSQL_PORT")
    database = Variable.get("MYSQL_DATABASE")
    username = Variable.get("MYSQL_USER")
    password = Variable.get("MYSQL_PASSWORD")

    # สร้าง connection string
    engine = sqlalchemy.create_engine(
        f"mysql+pymysql://{username}:{password}@{host}:{port}/{database}"
    )

    query_df = pandas.read_sql("""
    select 
    	c.customer_id,
        c.customer_name,
        c.customer_email,
        p.product_id,
        p.product_name,
        p.price
    from customers c
    inner join products p on c.customer_id = p.customer_id
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
        output_path = os.path.join(os.path.dirname(__file__), "..", "output", )
        os.makedirs(output_path, exist_ok=True)

        today = datetime.date.today().strftime("%Y-%m-%d")
        output_file_parquet = os.path.join(output_path, f"mysql_customers_products_{today}.parquet")
        output_file_csv = os.path.join(output_path, f"mysql_customers_products_{today}.csv")

        query_df.to_parquet(output_file_parquet, index=False)
        print(f"\n✅ Saved data from MySQL parquet file to {output_file_parquet}")
        query_df.to_csv(output_file_csv, index=False)
        print(f"\n✅ Saved data from MySQL csv file to {output_file_csv}")
    except Exception as e:
        print(e)

    log_end()
    return query_df