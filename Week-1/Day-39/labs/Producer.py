from azure.eventhub import EventHubProducerClient, EventData
import json, time, random
from datetime import datetime

conn_str ="Endpoint=sb://iot-stream-ns.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=yLxSotdl1bPrhE1bQmy46D1DF9Rvr+WGd+AEhAgNkNM="
eventhub_name = "iot-data"

producer = EventHubProducerClient.from_connection_string(
    conn_str=conn_str,
    eventhub_name=eventhub_name
)

regions = ["north", "south", "east", "west"]

while True:
    data = {
        "deviceId": f"device-{random.randint(1,5)}",
        "region": random.choice(regions),
        "temperature": round(random.uniform(20,40),2),
        "humidity": random.randint(30,90),
        "pressure": random.randint(990,1025),
        "eventTime": datetime.utcnow().isoformat()
    }

    event = EventData(json.dumps(data))
    producer.send_batch([event])

    print("Sent:", data)
    time.sleep(1)