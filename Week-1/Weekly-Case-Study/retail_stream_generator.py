import json
import random
import time
from datetime import datetime
from kafka import KafkaProducer

# =========================
# CONFIGURATION (UPDATE THESE)
# =========================
BOOTSTRAP_SERVERS = "your-namespace.servicebus.windows.net:9093"
TOPIC_NAME = "retail-stream"
CONNECTION_STRING = "Endpoint=sb://<your-namespace>.servicebus.windows.net/;SharedAccessKeyName=<keyname>;SharedAccessKey=<key>"

# =========================
# PRODUCER SETUP
# =========================
producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    security_protocol="SASL_SSL",
    sasl_mechanism="PLAIN",
    sasl_plain_username="$ConnectionString",
    sasl_plain_password=CONNECTION_STRING,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# =========================
# SAMPLE DATA
# =========================
products = ["Laptop", "Mobile", "Tablet", "Headphones", "TV"]
cities = ["Bangalore", "Delhi", "Mumbai", "Chennai", "Hyderabad"]
payment_methods = ["UPI", "Credit Card", "Debit Card"]

# =========================
# GENERATE STREAMING DATA
# =========================
def generate_event(i):
    price = random.randint(1000, 50000)
    quantity = random.randint(1, 3)

    return {
        "order_id": f"O{i}",
        "product": random.choice(products),
        "price": price,
        "quantity": quantity,
        "total_amount": price * quantity,
        "city": random.choice(cities),
        "payment_method": random.choice(payment_methods),
        "event_time": datetime.utcnow().isoformat()
    }

# =========================
# STREAM LOOP
# =========================
def stream_data():
    print("Streaming started... Press Ctrl+C to stop")

    i = 1
    while True:
        event = generate_event(i)
        producer.send(TOPIC_NAME, event)
        print(f"Sent: {event}")

        i += 1
        time.sleep(1)  # 1 event per second

# =========================
# MAIN
# =========================
if __name__ == "__main__":
    stream_data()
