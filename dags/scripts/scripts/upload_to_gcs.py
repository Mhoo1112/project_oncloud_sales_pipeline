import os
# from dotenv import load_dotenv
from airflow.models import Variable
from google.cloud import storage
from scripts.log_start_log_end import log_start
from scripts.log_start_log_end import log_end

def upload_to_gcs():
    log_start()

    # โหลดไฟล์ .env
    # path_dotenv = os.path.join(
    #     os.path.dirname(__file__), '..', 'config', '.env')
    # load_dotenv(path_dotenv)

    # bucket_name = os.getenv("GCP_BUCKET_NAME")
    # path_credentials = os.path.join(os.path.dirname(__file__), '..', os.getenv("GOOGLE_APPLICATION_CREDENTIALS"))
    bucket_name = Variable.get("GCP_BUCKET_NAME")
    path_credentials = os.path.join(os.path.dirname(__file__), '..', Variable.get("GOOGLE_APPLICATION_CREDENTIALS"))


    if not bucket_name or not path_credentials:
        raise ValueError("❌ GCP_BUCKET_NAME หรือ GOOGLE_APPLICATION_CREDENTIALS ยังไม่ได้ตั้งค่า")

    # ไฟล์ local ที่ต้องการอัปโหลด
    path_file_output = os.path.join(
        os.path.dirname(__file__), '..', 'output'
    )

    files = []
    for i in os.listdir(path_file_output):
        if i.endswith(".csv") and i.startswith("final_report"):
            files.append(i)
    if not files:
        raise FileNotFoundError("❌ ไม่พบไฟล์ที่ขึ้นต้นด้วย final_report และลงท้าย .csv")

    file_name = sorted(files)[-1]
    local_file_path = os.path.join(path_file_output, file_name)

    # สร้าง client จาก credentials
    storage_client = storage.Client.from_service_account_json(path_credentials)
    bucket = storage_client.get_bucket(bucket_name)
    # path ที่จะใช้ใน GCS
    gcs_blob_path = f"uploads/{file_name}"
    blob = bucket.blob(gcs_blob_path)

    # อัปโหลด
    blob.upload_from_filename(local_file_path)

    print(f"\n✅ Upload สำเร็จ: gs://{bucket_name}/{gcs_blob_path}")

    log_end()