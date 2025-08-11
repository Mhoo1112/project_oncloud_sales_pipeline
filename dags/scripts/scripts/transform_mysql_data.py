import os
import pandas
import datetime
from scripts.log_start_log_end import log_start
from scripts.log_start_log_end import log_end

def transform_mysql_data(df):
    """
    รวมข้อมูลจาก MySQL โดยจัดกลุ่มจาก Customer ID และ Customer Name
    """
    log_start()
    summary_df_01 = df.groupby(["customer_id",
                                "customer_name"]).agg({"price": "sum"}).reset_index()
    print("\n📊 Summary by Customer --------------------")
    print(summary_df_01)

    # เตรียม path สำหรับ export
    output_path = os.path.join(os.path.dirname(__file__),"..","output")
    os.makedirs(output_path, exist_ok=True)

    today = datetime.date.today().strftime("%Y-%m-%d")
    output_file_parquet = os.path.join(output_path, f"mysql_report_{today}.parquet")
    output_file_csv = os.path.join(output_path, f"mysql_report_{today}.csv")

    # Export
    summary_df_01.to_parquet(output_file_parquet, index=False)
    summary_df_01.to_csv(output_file_csv, index=False)

    # สรุปหลายฟังก์ชัน
    # summary_df_02 = df.groupby("customer_name")["price"].agg(["sum","min","max","mean"]).reset_index()
    # print("\n📊 Summary by Customer")
    # print(summary_df_02)
    log_end()
    return summary_df_01
