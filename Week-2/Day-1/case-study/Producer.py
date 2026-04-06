import json
import time
import random
from faker import Faker
from azure.eventhub import EventHubProducerClient, EventData

fake = Faker()

# Replace with your Event Hub connection string
CONNECTION_STR = "YOUR_EVENT_HUB_CONNECTION_STRING"
EVENT_HUB_NAME = "telecom-stream"

producer = EventHubProducerClient.from_connection_string(
    conn_str=CONNECTION_STR,
    eventhub_name=EVENT_HUB_NAME
)

def generate_cdr():
    return {
        "event_type": "CDR",
        "call_id": f"C{random.randint(1000,9999)}",
        "customer_id": random.randint(1, 1000),
        "call_type": random.choice(["Local", "STD", "ISD"]),
        "duration_sec": random.randint(10, 1000),
        "tower_id": f"T{random.randint(1,50)}",
        "timestamp": str(fake.date_time_this_year())
    }

def generate_data_usage():
    return {
        "event_type": "DATA",
        "usage_id": f"U{random.randint(1000,9999)}",
        "customer_id": random.randint(1, 1000),
        "data_mb": round(random.uniform(1, 500), 2),
        "app_type": random.choice(["YouTube","Netflix","WhatsApp"]),
        "timestamp": str(fake.date_time_this_year())
    }

def generate_fraud():
    return {
        "event_type": "FRAUD",
        "fraud_id": f"F{random.randint(1000,9999)}",
        "customer_id": random.randint(1, 1000),
        "fraud_type": random.choice(["SIM Cloning","High ISD Usage"]),
        "risk_score": round(random.uniform(0.7,1.0), 2),
        "timestamp": str(fake.date_time_this_year())
    }

def send_events():
    while True:
        batch = producer.create_batch()

        for _ in range(10):
            event_choice = random.choice(["CDR", "DATA", "FRAUD"])

            if event_choice == "CDR":
                data = generate_cdr()
            elif event_choice == "DATA":
                data = generate_data_usage()
            else:
                data = generate_fraud()

            batch.add(EventData(json.dumps(data)))

        producer.send_batch(batch)
        print("Sent batch of events")

        time.sleep(2)  # simulate streaming delay

if __name__ == "__main__":
    send_events()