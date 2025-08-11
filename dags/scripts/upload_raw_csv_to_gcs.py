import os
# from dotenv import load_dotenv
from airflow.models import Variable
from email.message import EmailMessage
from google.cloud import storage
from scripts.log_start_log_end import log_start
from scripts.log_start_log_end import log_end

def upload_raw_csv_to_gcs():
    log_start()

    # โหลดไฟล์ .env
    # path_dotenv = os.path.join(os.path.dirname(__file__), '..', 'config', '.env')
    # load_dotenv(path_dotenv)

    # bucket_name = os.getenv("GCP_BUCKET_NAME")
    bucket_name = Variable.get("GCP_BUCKET_NAME")
    # path_credentials = os.path.join(os.path.dirname(__file__), '..', os.getenv("GOOGLE_APPLICATION_CREDENTIALS"))
    path_credentials = os.path.join(os.path.dirname(__file__), '..', Variable.get("GOOGLE_APPLICATION_CREDENTIALS"))


    if not bucket_name or not path_credentials:
        raise ValueError("❌ GCP_BUCKET_NAME หรือ GOOGLE_APPLICATION_CREDENTIALS ยังไม่ได้ตั้งค่า")

    # ไฟล์ local ที่ต้องการอัปโหลด
    path_file_output = os.path.join(
        os.path.dirname(__file__), '..', 'data', 'sales_files')

    files = []
    for i in os.listdir(path_file_output):
        if i.endswith(".csv") and i.startswith("2024_sales_"):
            local_file = os.path.join(path_file_output, i)
            files.append(local_file)
    if not files:
        raise FileNotFoundError("❌ ไม่พบไฟล์ที่ขึ้นต้นด้วย 2024_sales_ และลงท้าย .csv")

    # สร้าง client จาก credentials
    storage_client = storage.Client.from_service_account_json(path_credentials)
    bucket = storage_client.get_bucket(bucket_name)
    # path ที่จะใช้ใน GCS

    for i in files:
        gcs_blob_path = f"raw_data/{os.path.basename(i)}"
        blob = bucket.blob(gcs_blob_path)
        # ตรวจสอบว่ามีอยู่รึเปล่า
        if blob.exists():
            print(f"⚠️ ไฟล์นี้มีอยู่แล้วบน GCS: {gcs_blob_path} (ข้ามอัปโหลด)")
            continue
        # อัปโหลด
        blob.upload_from_filename(i)

        print(f"\n✅ Upload สำเร็จ: gs://{bucket_name}/{gcs_blob_path}")

    log_end()

if __name__ == "__main__":
    upload_raw_csv_to_gcs()