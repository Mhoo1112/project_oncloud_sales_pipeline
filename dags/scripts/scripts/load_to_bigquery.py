import os
# from dotenv import load_dotenv
from airflow.models import Variable
from scripts.log_start_log_end import log_start, log_end
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

def load_to_bigquery(task_id: str, conn_id: str = "google_cloud_default"):
    log_start()

    # เชื่อมต่อกับโฟลเดอร์ .env
    # path_dotenv = os.path.join(os.path.dirname(__file__), "..", "config", ".env")
    # โหลดข้อมูลจาก .env
    # load_dotenv(path_dotenv)

    # ไฟล์ที่จะอัพโหลด
    path_file_output = os.path.join(os.path.dirname(__file__), '..', 'output')
    files = []
    for i in os.listdir(path_file_output):
        if i.endswith(".csv") and i.startswith("final_report"):
            files.append(i)
    if not files:
        raise FileNotFoundError("❌ ไม่พบไฟล์ที่ขึ้นต้นด้วย final_report และลงท้าย .csv")

    file_name = sorted(files)[-1]
    gcs_object_path = f"uploads/{file_name}"
    local_file_path = os.path.join(path_file_output, file_name)

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