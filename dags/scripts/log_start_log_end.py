import datetime

def log_start():
    to_start = datetime.datetime.now()
    print("\n" + "*" * 100)
    print("start")
    print(to_start)

def log_end():
    to_end = datetime.datetime.now()
    print("\nend")
    print(to_end)
    print("*" * 100)