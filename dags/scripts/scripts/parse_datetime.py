import datetime
# Format ยอดนิยมที่ใช้ใน strftime()
# Format        ความหมาย            ตัวอย่าง
# %Y	        ปี 4 หลัก	            2025
# %y	        ปี 2 หลัก	            25
# %m	        เดือน (01–12)	    07
# %d	        วัน (01–31) 	        27
# %H	        ชั่วโมง (00–23)	    13
# %M	        นาที (00–59)	        45
# %S        	วินาที (00–59)	    08
# %A	        ชื่อวันเต็ม (ภาษาอังกฤษ)	Sunday
# %a        	ชื่อวันย่อ	            Sun
# %B    	    ชื่อเดือนเต็ม	        July
# %b	        ชื่อเดือนย่อ	        Jul
def parse_datetime(time_str):
    formats = [
        "%d %m %Y", "%d-%m-%Y", "%d/%m/%Y",
        "%d %m %Y %H:%M:%S", "%d-%m-%Y %H:%M:%S", "%d/%m/%Y %H:%M:%S",
        "%d %m %Y %z", "%d-%m-%Y %z", "%d/%m/%Y %z",
        "%d %m %Y %H:%M:%S %z", "%d-%m-%Y %H:%M:%S %z", "%d/%m/%Y %H:%M:%S %z",
        "%a, %d %m %Y %H:%M:%S %z"
    ]
    for i in formats:
        try:
            return datetime.datetime.strptime(time_str, i)
            # from datetime import datetime
            # datetime.strptime(time_str, format)
            break
        except ValueError:
            continue
    raise ValueError(f"❌ ไม่สามารถแปลงรูปแบบของวันที่ได้: {time_str}")