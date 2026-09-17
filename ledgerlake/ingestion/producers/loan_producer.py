"""
loan_producer.py — Simule des événements de crédit (Palier A.1)

À IMPLÉMENTER :
  {
    "loan_id": str,
    "customer_id": str,
    "amount": float,
    "duration_months": int,
    "status": "REQUESTED" | "APPROVED" | "REJECTED" | "REPAID" | "DEFAULTED",
    "timestamp": ISO 8601
  }
Publié sur KAFKA_TOPIC_LOANS.

(Palier H) pour fermer la boucle plateforme de données -> moteur de décision.
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
KAFKA_TOPIC_LOANS = "loan-events"

fake = Faker()

def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

def generate_loan_event():
    event = {
        "loan_id": str(uuid.uuid4()),
        "customer_id": fake.uuid4(),
        "amount": round(random.uniform(5000.0, 500000.0), 2),
        "duration_months": random.choice([12, 24, 36, 48, 60, 120, 240, 300]),
        "status": random.choice(["REQUESTED", "APPROVED", "REJECTED", "REPAID", "DEFAULTED"]),
        "timestamp": datetime.utcnow().isoformat() + "Z"
    }
    return event

if __name__ == "__main__":
    producer = Producer({'bootstrap.servers': KAFKA_BROKER})
    
    print(f"Starting loan producer... Publishing to {KAFKA_TOPIC_LOANS}")
    try:
        while True:
            event = generate_loan_event()
            producer.produce(
                KAFKA_TOPIC_LOANS,
                key=event["loan_id"].encode('utf-8'),
                value=json.dumps(event).encode('utf-8'),
                callback=delivery_report
            )
            producer.poll(0)
            time.sleep(random.uniform(2.0, 10.0)) # Encore moins fréquent
    except KeyboardInterrupt:
        print("\nStopping loan producer.")
    finally:
        producer.flush()
