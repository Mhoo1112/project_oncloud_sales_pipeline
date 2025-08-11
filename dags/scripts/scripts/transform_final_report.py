import os
import pandas
import datetime
from scripts.log_start_log_end import log_start
from scripts.log_start_log_end import log_end

def transform_final_report(csv_df, mysql_df, exchange_rate):
    """
        รวมข้อมูลจาก CSV, MySQL และ AIP
        """
    log_start()

    # เปลี่ยนชื่อ column จาก MySQL
    mysql_df = mysql_df.rename(columns={"price": "total_usd", "customer_name": "name"})

    # เปลี่ยนชื่อ column จาก CSV
    csv_df = csv_df.rename(columns={"amount_usd": "order_usd"})

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

    # เตรียม Path สำหรับ export
    output_path = os.path.join(os.path.dirname(__file__), "..", "output")
    os.makedirs(output_path, exist_ok=True)

    today = datetime.date.today().strftime("%Y-%m-%d")
    output_file_parquet = os.path.join(output_path, f"final_report_{today}.parquet")
    output_file_csv = os.path.join(output_path, f"final_report_{today}.csv")

    # Export
    merged_df.to_parquet(output_file_parquet, index=False)
    merged_df.to_csv(output_file_csv, index=False)

    print(f"\n✅ Export Final Report → {output_file_csv}")

    log_end()
    return merged_df
