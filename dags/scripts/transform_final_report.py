import os
import pandas
import datetime
from airflow.models import Variable
from google.cloud import storage
from scripts.log_start_log_end import log_start
from scripts.log_start_log_end import log_end

def transform_final_report():
    """
        รวมข้อมูลจาก CSV, MySQL และ AIP
        """
    log_start()
    # ค่าพารามิเตอร์
    today = datetime.date.today().strftime("%Y-%m-%d")
    output_path = f"output/"
    tmp_path = f"/tmp"

    # ดึงข้อมูลจาก Variable
    bucket_name = Variable.get("GCP_BUCKET_NAME")
    # เชื่อมต่อกับ Google
    client = storage.Client()
    bucket = client.get_bucket(bucket_name)
    # ดึงข้อมูล CSV จาก GCS
    path_blob_csv = os.path.join(output_path, f"cleaned_sales_{today}.csv")
    local_csv = os.path.join(tmp_path, f"tmp_cleaned_sales_{today}.csv")
    bucket.blob(path_blob_csv).download_to_filename(local_csv)

    # ดึงข้อมูล MySQL จาก GCS
    path_blob_mysql = os.path.join(output_path, f"mysql_customers_products_{today}.csv")
    local_mysql = os.path.join(tmp_path, f"mysql_customers_products_{today}.csv")
    bucket.blob(path_blob_mysql).download_to_filename(local_mysql)

    # ดึงข้อมูล Ex_rate จาก GCS
    path_blob_exchange = os.path.join(output_path, f"fetch_exchange_rate_{today}.csv")
    local_exchange = os.path.join(tmp_path, f"tmp_fetch_exchange_rate_{today}.csv")
    bucket.blob(path_blob_exchange).download_to_filename(local_exchange)

    # อ่านไฟล์ด้วย pandas
    csv_df = pandas.read_csv(local_csv)
    mysql_df = pandas.read_csv(local_mysql)
    exchange_df = pandas.read_csv(local_exchange)
    exchange_rate = exchange_df.loc[exchange_df["currency"] == "THB", "rate"].values[0]



    # เปลี่ยนชื่อ column จาก CSV
    csv_df = csv_df.rename(columns={"amount_usd": "order_usd"})

    # เปลี่ยนชื่อ column จาก MySQL
    mysql_df = mysql_df.rename(columns={"price": "total_usd", "customer_name": "name"})

    # รวมข้อมูลด้วย customer_id
    merged_df = csv_df.merge(mysql_df, on="customer_id", how="left")

    # คำนวณยอดเป็นเงิน
    merged_df["order_thb"] = merged_df["order_usd"] * exchange_rate
    merged_df["total_thb"] = merged_df["total_usd"] * exchange_rate
    merged_df["exchange_rate"] = exchange_rate
    merged_df = merged_df[
        ["order_id", "order_date", "customer_id", "name", "order_usd", "order_thb", "total_usd", "total_thb",
         "exchange_rate"]]

    print("▶️ Transform final report --------------------")
    print(merged_df)

    out_csv_local = os.path.join(tmp_path,  f"final_report_{today}.csv")
    out_parq_local = os.path.join(tmp_path, f"final_report_{today}.parquet")

    merged_df.to_csv(out_csv_local, index=False)
    merged_df.to_parquet(out_parq_local)

    print(f"💾 Saved: {out_csv_local}")
    print(f"💾 Saved: {out_parq_local}")

    bucket.blob(os.path.join(output_path, f"final_report_{today}.csv")).upload_from_filename(out_csv_local)
    bucket.blob(os.path.join(output_path, f"final_report_{today}.parquet")).upload_from_filename(out_parq_local)


    print(f"\n✅ Export Final Report → gs://{bucket_name}/{output_path}final_report_{today}.csv")


    log_end()
    return merged_df
