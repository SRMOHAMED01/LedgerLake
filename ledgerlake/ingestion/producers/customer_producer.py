"""
customer_producer.py — Simule des événements clients (Palier A.1)

À IMPLÉMENTER : événements de type "création de compte" / "mise à jour profil"
  {
    "customer_id": str,
    "event_type": "ACCOUNT_CREATED" | "PROFILE_UPDATED",
    "age": int,
    "country": str,
    "segment": str | null,
    "timestamp": ISO 8601
  }
Publié sur KAFKA_TOPIC_CUSTOMERS.
"""
import json
import time
import random
import uuid
from datetime import datetime
from faker import Faker
from confluent_kafka import Producer

# Configuration
KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC_CUSTOMERS = "customers-events"

fake = Faker()

def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

def generate_customer_event():
    event_type = random.choice(["ACCOUNT_CREATED", "PROFILE_UPDATED"])
    
    event = {
        "customer_id": str(uuid.uuid4()) if event_type == "ACCOUNT_CREATED" else fake.uuid4(),
        "event_type": event_type,
        "age": random.randint(18, 90),
        "country": random.choice(["MA", "Morocco", "Maroc", "FR", "France", "US", "USA"]),
        "segment": random.choice(["RETAIL", "PREMIUM", "BUSINESS", None]),
        "timestamp": datetime.utcnow().isoformat() + "Z"
    }
    return event

if __name__ == "__main__":
    producer = Producer({'bootstrap.servers': KAFKA_BROKER})
    
    print(f"Starting customer producer... Publishing to {KAFKA_TOPIC_CUSTOMERS}")
    try:
        while True:
            event = generate_customer_event()
            producer.produce(
                KAFKA_TOPIC_CUSTOMERS,
                key=event["customer_id"].encode('utf-8'),
                value=json.dumps(event).encode('utf-8'),
                callback=delivery_report
            )
            producer.poll(0)
            time.sleep(random.uniform(1.0, 5.0)) # Moins fréquent que les transactions
    except KeyboardInterrupt:
        print("\nStopping customer producer.")
    finally:
        producer.flush()
