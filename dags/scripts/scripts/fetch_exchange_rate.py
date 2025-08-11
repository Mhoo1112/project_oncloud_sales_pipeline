import os
import requests
import pandas
# from dotenv import load_dotenv
from airflow.models import Variable
from scripts.log_start_log_end import log_start
from scripts.log_start_log_end import log_end

def fetch_usd_to_thb():
    """
        อ่านข้อมูล Convertion Rate จาก API/
    """
    log_start()

    # โหลด .env ที่อยู่ในโฟลเดอร์ config
    # dotenv_path = os.path.join(os.path.dirname(__file__), '..', 'config', '.env')
    # load_dotenv(dotenv_path)

    # URL สำหรับเรียกดูอัตราแลกเปลี่ยน
    # url = os.getenv("EXCHANGE_RATE_API")
    url = Variable.get("EXCHANGE_RATE_API")
    print("✅ DEBUG URL:", url)

    # โหลดข้อมูลจาก API
    response = requests.get(url)
    # แปลง ข้อมูล JSON ที่ได้จาก API ให้กลายเป็น dictionary
    exchange_rate_data = response.json()

    # thb_rate = exchange_rate_data["conversion_rates"]["THB"]
    if "conversion_rates" not in exchange_rate_data:
        raise KeyError("❌ ไม่พบ key 'conversion_rates' ใน response")
    # เมื่อถูก raise โปรแกรมจะหยุด

    exchange_df = pandas.DataFrame.from_dict(
        exchange_rate_data["conversion_rates"],  # ดึง dict ของ conversion_rates
        orient="index",  # ให้ key เป็น index แต่ละ key ใน dictionary จะกลายเป็น index (แถว) แต่ละ value จะกลายเป็น ค่าของแถว
        columns=["rate"]  # ตั้งชื่อ column value เป็น rate
    ).reset_index().rename(columns={"index": "currency"})
    # exchange_df = pandas.DataFrame(
    #     exchange_rate_data["conversion_rates"].items(),  # → List of tuples
    #     columns=["currency", "rate"]
    # )

    from datetime import datetime

    #  "time_last_update_utc":"Wed, 06 Aug 2025 00:00:01 +0000",
    time_str = exchange_rate_data["time_last_update_utc"]
    formats = [
        "%a, %d %b %Y %H:%M:%S %z",
        "%d %b %Y %H:%M:%S",
        "%d %m %Y %H:%M:%S",
        "%Y %m %d %H:%M:%S",
        "%Y %m %d",
        "%d %m %Y"
    ]
    for fmt in formats:
        try:
            time_obj = datetime.strptime(time_str, fmt)
            break
        except ValueError:
            continue
    else:
        raise ValueError(f"❌ ไม่สามารถแปลงรูปแบบของวันที่ได้: {time_str}")

    exchange_df["date"] = time_obj.strftime("%Y-%m-%d")

    # from datetime import datetime
    # exchange_df["date"] = datetime.now()
    # exchange_df["date"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")


    print("\n✅ rate:")
    print(exchange_df)

    if "THB" not in exchange_rate_data["conversion_rates"]:
        raise KeyError("❌ ไม่พบค่า THB ใน conversion_rates")

    # Filter ค่า THB จาก DataFrame
    thb_df = exchange_df[exchange_df["currency"] == "THB"].reset_index(drop=True)
    thb_value = thb_df["rate"].values[0]
    print("\n อัตรา THB จาก DataFrame:")
    print(thb_df)

    try:
        # output_path = r'C:/Users/Buath/projects/project_sales_pipeline/output'
        output_path = os.path.join(os.path.dirname(__file__), '..', 'output')
        os.makedirs(output_path, exist_ok=True)
        output_file_parquet = os.path.join(output_path, "fetch_exchange_rate.parquet")
        output_file_CSV = os.path.join(output_path, "fetch_exchange_rate.csv")

        exchange_df.to_parquet(output_file_parquet,index=False)
        print(f"\n✅ Saved parquet file to {output_file_parquet}")
        exchange_df.to_csv(output_file_CSV, index=False)
        print(f"✅ Saved csv file to {output_file_CSV}")
    except Exception as e:
        print("❌ เกิดข้อผิดพลาด:", e)

    log_end()
    return thb_value