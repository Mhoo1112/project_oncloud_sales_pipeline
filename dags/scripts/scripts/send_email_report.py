import os
import smtplib
# from dotenv import load_dotenv
from email.message import EmailMessage
from airflow.models import Variable

def send_email_report():
    # โหลด .env
    # dotenv_path = os.path.join(os.path.dirname(__file__), "..", "config", ".env")
    # load_dotenv(dotenv_path)


    # อ่านค่าจาก .env
    # email_user = os.getenv('EMAIL_USER')
    # email_password = os.getenv('EMAIL_PASSWORD')
    email_user = Variable.get("EMAIL_USER")
    email_password = Variable.get("EMAIL_PASSWORD")
    email_to = email_user  # ส่งให้ตัวเอง

    # เตรียมอีเมล
    msg = EmailMessage()
    msg["Subject"] = "Sales Report แนบไฟล์"  # เรื่องที่ส่ง
    msg["From"] = email_user
    msg["To"] = email_to
    msg.set_content("แนบไฟล์รายงานยอดขายด้วยแล้วครับ")  # เนื้อหาข้างใน

    # แนบไฟล์
    file_path = os.path.join(os.path.dirname(__file__), "..", "output")
    # เลือกไฟล์มาอ่าน
    files = []
    for i in os.listdir(file_path):
        if i.endswith(".csv") and i.startswith("final_report"):
            files.append(i)

    if not files:
        raise FileNotFoundError("❌ ไม่พบไฟล์รายงาน final_report*.csv")

    last_file = sorted(files)[-1]
    latest_file_path  = os.path.join(file_path, last_file)
    with open(latest_file_path, "rb") as f:
        file_content = f.read()
        msg.add_attachment(file_content,
                           maintype="application",
                           subtype="octet-stream",
                           filename=last_file)
    # files = []
    # for i in os.listdir(file_path):
    #     if i.endswith(".csv") and i.startswith("final_report"):
    #         latest_file = sorted(files)[-1]  # เอาไฟล์ล่าสุด
    #         file = os.path.join(file_path, i)
    #
    #         with open(file, "rb") as f:
    #             file_content = f.read()
    #             msg.add_attachment(file_content,
    #                                maintype="application",
    #                                subtype="octet-stream",
    #                                filename=i)

    # ส่งอีเมล
    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp:
        smtp.login(email_user, email_password)
        smtp.send_message(msg)

    print(f"✅ ส่งอีเมลแนบไฟล์ {last_file} เรียบร้อยแล้ว")