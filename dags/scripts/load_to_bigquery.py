import os
import datetime
from airflow.models import Variable
from google.cloud import storage
from scripts.log_start_log_end import log_start, log_end
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

def load_to_bigquery(task_id: str, conn_id: str = "google_cloud_default"):
    
    log_start()

    # ไฟล์ที่จะอัพโหลด
    path_file_output = os.path.join(os.path.dirname(__file__), '..', 'output')

    # ค่าพารามิเตอร์
    today = datetime.date.today().strftime("%Y-%m-%d")
    tmp_path = "/tmp"
    output_path = "output/"
    upload_path = "uploads"

    # ดึงข้อมูลจาก Variable
    bucket_name =Variable.get("GCP_BUCKET_NAME")

    # ตั้งค่า google client
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    path_blob_csv = client.list_blobs(bucket, prefix=output_path) # bucket_name/output/

    files = []
    for blob in path_blob_csv: # = client.list_blobs(bucket, prefix=output_path) # os.listdir(path_file_output):
        blob_name = blob.name.split("/")[-1]
        if blob_name.endswith(f"{today}.csv") and blob_name.startswith("final_report_"):
            local_csv = os.path.join(tmp_path, blob_name)
            blob.download_to_filename(local_csv)
            files.append(local_csv)

    if not files:
        print("⚠️ ไม่พบไฟล์ final_report*.csv วันนี้ แต่ยังสร้าง Operator ได้")
        return GCSToBigQueryOperator(
            task_id=task_id,
            bucket=bucket_name,
            source_objects=[],
            destination_project_dataset_table=Variable.get("BIGQUERY_TABLE"),
            source_format="CSV",
            skip_leading_rows=1,
            write_disposition="WRITE_TRUNCATE",
            autodetect=True,
            gcp_conn_id=conn_id,
        )
    

    last_file = sorted(files)[-1]
    gcs_object_path = os.path.join(upload_path, os.path.basename(last_file))
    
    # อัปโหลดไฟล์ไปยัง uploads/
    upload_blob = bucket.blob(gcs_object_path)
    upload_blob.upload_from_filename(last_file)
    print(f"✅ อัปโหลดไฟล์ {last_file} ไปยัง gs://{bucket.name}/{gcs_object_path}")


    log_end()

    return GCSToBigQueryOperator(
        task_id=task_id,
        # bucket=os.getenv("GCP_BUCKET_NAME"),  # GCS bucket ที่เก็บไฟล์
        bucket=Variable.get("GCP_BUCKET_NAME"),
        source_objects=[gcs_object_path],  # path ของไฟล์บน GCS
        # destination_project_dataset_table=os.getenv("BIGQUERY_TABLE"),  # BigQuery ปลายทาง
        destination_project_dataset_table=Variable.get("BIGQUERY_TABLE"),
        source_format="CSV",  # ประเภทไฟล์: CSV
        skip_leading_rows=1,  # ข้าม header row ถ้ามี
        write_disposition="WRITE_TRUNCATE",  # ล้างข้อมูลเก่าทิ้งก่อนเขียนใหม่
        autodetect=True,  # ให้ BQ ตรวจ schema อัตโนมัติ (แนะนำเปิด)
        gcp_conn_id=conn_id,  # ตั้งค่าจาก Airflow connection
    )