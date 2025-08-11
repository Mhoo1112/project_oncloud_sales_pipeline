import os
import smtplib
import datetime
from email.message import EmailMessage
from airflow.models import Variable
from google.cloud import storage

def send_email_report():
    
    # ค่าพารามิเตอร์
    path_tmp = f"/tmp"
    output_path = f"output/"
    today = datetime.date.today().strftime("%Y-%m-%d")


    # ดึงข้อมูลจาก VaVariable
    bucket_name = Variable.get("GCP_BUCKET_NAME")
    EMAIL_USER = Variable.get("EMAIL_USER")
    EMAIL_PASSWORD = Variable.get("EMAIL_PASSWORD")

    # ตั้งค่า Google client
    client = storage.Client()
    bucket = client.get_bucket(bucket_name)
    path_blob_csv = client.list_blobs(bucket, prefix=output_path)

    # ตั้งค่าอีเมล
    email_user = EMAIL_USER
    email_password = EMAIL_PASSWORD
    email_to = email_user  # ส่งให้ตัวเอง

    # เตรียมอีเมล
    msg = EmailMessage()
    msg["Subject"] = "Sales Report แนบไฟล์"  # เรื่องที่ส่ง
    msg["From"] = email_user
    msg["To"] = email_to
    msg.set_content("แนบไฟล์รายงานยอดขายด้วยแล้วครับ")  # เนื้อหาข้างใน

    # แนบไฟล์
    # เลือกไฟล์มาอ่าน
    files = []
    for blob in path_blob_csv: # = client.list_blobs(bucket, prefix=output_path)
        blob_name = blob.name.split("/")[-1]
        if blob_name.endswith(".csv") and blob_name.startswith("final_report"):
            local_csv = os.path.join(path_tmp, blob_name)
            blob.download_to_filename(local_csv)
            files.append(local_csv)

    if not files:
        raise FileNotFoundError("❌ ไม่พบไฟล์รายงาน final_report*.csv")

    last_file = sorted(files)[-1]
    # last_file = sorted(files, reverse=True)[0]

    with open(last_file, "rb") as f:
        file_content = f.read()
        msg.add_attachment(file_content,
                           maintype="application",
                           subtype="octet-stream",
                           filename=os.path.basename(last_file))

    # ส่งอีเมล
    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp:
        smtp.login(email_user, email_password)
        smtp.send_message(msg)
    
    print(f"✅ ส่งอีเมลแนบไฟล์ {os.path.basename(last_file)} เรียบร้อยแล้ว")
