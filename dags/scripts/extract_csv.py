import os
import pandas
import datetime
from airflow.models import Variable
from google.cloud import storage
from scripts.log_start_log_end import log_start
from scripts.log_start_log_end import log_end

def extract_sales_csv():
    """
        อ่านไฟล์ CSV ทั้งหมดจากโฟลเดอร์ sales_files/
        - รวมเป็น DataFrame เดียว
        - ปรับชื่อคอลัมน์ให้เป็นมาตรฐาน
        - คืนค่า DataFrame
        """
    log_start()
    # ดึงข้อมูลจาก Variable
    bucket_name = Variable.get("GCP_BUCKET_NAME")
    # ค่าพารามิเตอร์อื่นๆ
    today = datetime.date.today().strftime("%Y-%m-%d")
    prefix = "raw_data/" # โฟลเดอร์ที่เก็บ raw data
    tmp_path = "/tmp" # ที่เก็บไฟล์ชั่วคราวบนเครื่องรัน
    output_path = "output/"

    # ตั้งค่า Google client
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blobs = client.list_blobs(bucket, prefix=prefix) # bucket_name/raw_data/

    # สร้างลิสต์ไว้รวมทุก DataFrame
    all_files = []

    # for filename in os.listdir(folder_path):
    for blob in blobs: # client.list_blobs(bucket, prefix=prefix):
        blob_name = blob.name.split("/")[-1]
        # ค้นหาไฟล์ CSV
        if blob_name.startswith("2024_sales") and blob_name.endswith(".csv"):
            # สร้างพื้นที่ชั่วคราว จะถูกลบเมื่อรันเสร็จ
            local_tmp = os.path.join(tmp_path, blob_name) # /tmp/2024_sales_01.csv
            # ดาวน์โหลดไฟล์จาก GCS ไว้ที่ local tmp ก่อนอ่านด้วย pandas
            blob.download_to_filename(local_tmp)
            print(f"⬇️ Downloaded: gs://{bucket_name}/{blob.name} -> {local_tmp}")
            
            try:
                # อ่าน CSV
                data_frame = pandas.read_csv(local_tmp)
                print("\n▶️ local_tmp")
                print(local_tmp)

                # แปลงชื่อคอลัมน์
                new_columns = []
                for col in data_frame.columns:
                    col = col.strip()
                    col = col.replace(' ', '_')
                    col = col.lower()
                    new_columns.append(col)
                # print("\n▶️new_columns")
                # print(new_columns)
                data_frame.columns = new_columns
                # แปลงชื่อ column
                data_frame = data_frame.rename(
                    columns={
                        "orderid": "order_id",
                        "orderdate": "order_date",

                    }
                )
                # เรียง column
                # data_frame = data_frame[sorted(data_frame.columns)] เรียงจากน้อยไปมาก
                required_cols = ['order_id', 'order_date', 'customer_id', 'product_id', 'amount_usd']
                # ตรวจสอบคอลัมน์ที่ขาด
                ordered_cols = []
                # วนลูปเอาคอลัมน์ตามลำดับที่ต้องการ
                for col in required_cols:
                    if col in data_frame.columns:
                        ordered_cols.append(col)
                # วนลูปเพิ่มคอลัมน์อื่น ๆ ที่เหลือ
                for col in data_frame.columns:
                    if col not in required_cols:
                        ordered_cols.append(col)
                # เรียง column ตาม ordered_cols
                data_frame = data_frame[ordered_cols]

                # เปลี่ยนประเภทข้อมูลของคอลัมน์
                data_frame["order_date"] = pandas.to_datetime(data_frame["order_date"], errors="coerce")
                data_frame["order_date"] = pandas.to_datetime(data_frame["order_date"].dt.strftime("%Y-%m-%d"),
                                                              errors="coerce")
                data_frame["amount_usd"] = pandas.to_numeric(data_frame["amount_usd"], errors="coerce")
                # data_frame["amount_usd"] = data_frame["amount_usd"].fillna(0)

                print("\n▶️ DataFrame of each file")
                print(data_frame)
                print(f"✅ Loaded: {blob_name} | {len(data_frame)} rows x {len(data_frame.columns)} cols")

                if not data_frame.empty:
                    all_files.append(data_frame)
                else:
                    print(f"! Empty file: {blob_name}")
            except Exception as e:
                print(f"❌ Error loading file {blob_name}: {e}")
                continue
    # รวมทุก DataFrame
    if all_files:
        combined_df = pandas.concat(all_files, ignore_index=True)
        combined_df = combined_df.drop_duplicates().reset_index(drop=True)
        # combined_df = combined_df.dropna(subset=["customer_id"])
        combined_df = combined_df.dropna().reset_index(drop=True)
        print("\n ️✅ dtypes")
        print(combined_df.dtypes)
        print("\n ️✅ count")
        print(combined_df.count())
        print("\n ️✅ DataFrame Combined --------------------")
        print(combined_df)

        # เซฟไฟล์ผลลัพธ์ไว้ใน /tmp
        out_csv_local = os.path.join(tmp_path, "cleaned_sales.csv")
        out_parq_local = os.path.join(tmp_path, "cleaned_sales.parquet")


        combined_df.to_csv(out_csv_local, index=False)
        combined_df.to_parquet(out_parq_local)


        print(f"💾 Saved: {out_csv_local}")
        print(f"💾 Saved: {out_parq_local}")

        bucket.blob(os.path.join(output_path, f"cleaned_sales_{today}.csv")).upload_from_filename(out_csv_local)
        bucket.blob(os.path.join(output_path, f"cleaned_sales_{today}.parquet")).upload_from_filename(out_parq_local)

    else:
        combined_df = pandas.DataFrame()  # ถ้าไม่มีไฟล์เลย
        print("\n✅ DataFrame --------------------")
        print(combined_df)

    log_end()
    return combined_df