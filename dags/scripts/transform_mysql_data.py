import os
import pandas
import datetime
from airflow.models import Variable
from google.cloud import storage
from scripts.log_start_log_end import log_start
from scripts.log_start_log_end import log_end

def transform_mysql_data():
    """
    รวมข้อมูลจาก MySQL โดยจัดกลุ่มจาก Customer ID และ Customer Name
    """
    log_start()
    today = datetime.date.today().strftime("%Y-%m-%d")
    # ค่าพารามิเตอร์
    tmp_path = f"/tmp"
    output_path = f"output/"
    local_tmp = None

    # ดึงข้อมูลจาก Variable
    bucket_name = Variable.get("GCP_BUCKET_NAME")
    # เชื่อมต่อกับ Google
    client = storage.Client()
    bucket = client.get_bucket(bucket_name)    
    blobs = client.list_blobs(bucket, prefix=output_path) # bucket_name/output/

    for blob in blobs: # blobs = client.list_blobs(bucket, prefix=output_path)
        blob_name = blob.name.split("/")[-1]
        if blob_name.startswith(f"mysql_customers_products_") and blob_name.endswith(f"{today}.parquet"):
            # สร้างพื้นที่ชั่วคราว จะถูกลบเมื่อรันเสร็จ
            local_tmp = os.path.join(tmp_path, blob_name)
            # ดาวน์โหลดไฟล์จาก GCS ไว้ที่ local tmp ก่อนอ่านด้วย pandas
            blob.download_to_filename(local_tmp)
            break  # โหลดแค่ไฟล์แรกที่เจอ
    if local_tmp is None:
        raise FileNotFoundError(f"❌ ไม่พบไฟล์ mysql_customers_products_{today}.parquet ใน GCS")


    # สรุปข้อมูล
    df = pandas.read_parquet(local_tmp)
    summary_df_01 = df.groupby(["customer_id",
                                "customer_name"]).agg({"price": "sum"}).reset_index()
    print("\n📊 Summary by Customer --------------------")
    print(summary_df_01)

    # เตรียม path สำหรับ export
    output_csv_tmp = os.path.join(tmp_path, f"tmp_mysql_report_{today}.csv")
    output_parq_tmp = os.path.join(tmp_path, f"tmp_mysql_report_{today}.parquet")

    summary_df_01.to_csv(output_csv_tmp, index=False)
    summary_df_01.to_parquet(output_parq_tmp)

    print(f"💾 Saved: {output_csv_tmp}")
    print(f"💾 Saved: {output_parq_tmp}")

    bucket.blob(os.path.join(output_path,f"mysql_report_{today}.csv")).upload_from_filename(output_csv_tmp)
    bucket.blob(os.path.join(output_path,f"mysql_report_{today}.parquet")).upload_from_filename(output_parq_tmp)
    log_end()
    return summary_df_01
