import time
import random
from datetime import datetime

while True:
    data = [{
        "device_id": f"D{random.randint(1,100)}",
        "patient_id": f"P{random.randint(1,10000)}",
        "heart_rate": random.randint(60,120),
        "oxygen_level": random.randint(85,100),
        "temperature": round(random.uniform(36, 39), 2),
        "event_time": str(datetime.now())
    }]
    
    df = spark.createDataFrame(data)
    df.write.mode("append").json("/bronze/icu_stream/")
    
    time.sleep(1)