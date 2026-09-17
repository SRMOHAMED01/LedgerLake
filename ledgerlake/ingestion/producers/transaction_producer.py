"""
transaction_producer.py — Simule des transactions bancaires (Palier A.1)

À IMPLÉMENTER :
1. Utiliser `faker` pour générer des transactions réalistes :
   {
     "transaction_id": str (uuid),
     "customer_id": str,
     "amount": float,
     "currency": "MAD" | "EUR" | "USD",
     "type": "CARD_PAYMENT" | "TRANSFER" | "ATM_WITHDRAWAL" | "LOAN_REPAYMENT",
     "merchant": str,
     "country": str (varier volontairement le format : "MA", "Morocco", "Maroc"
                     pour avoir un vrai cas de standardisation à faire en Silver),
     "timestamp": ISO 8601
   }
2. Injecter volontairement quelques données invalides de temps en temps
   (amount négatif, customer_id vide) pour tester la Data Quality au Palier D
3. Producer confluent_kafka.Producer -> produce() sur KAFKA_TOPIC_TRANSACTIONS
4. Boucle avec un délai aléatoire entre chaque message pour simuler un flux réel
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
KAFKA_TOPIC_TRANSACTIONS = "transactions-events"

fake = Faker()

def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

def generate_transaction():
    """Génère une transaction aléatoire avec quelques erreurs intentionnelles."""
    # Simulation d'erreurs (10% de chance)
    is_invalid = random.random() < 0.1
    
    amount = round(random.uniform(10.0, 5000.0), 2)
    if is_invalid and random.random() < 0.5:
        amount = -amount # Invalid amount

    customer_id = fake.uuid4()
    if is_invalid and random.random() < 0.5:
        customer_id = None # Invalid customer

    # Variations dans le pays
    countries = ["MA", "Morocco", "Maroc", "FR", "France", "US", "USA"]
    
    transaction = {
        "transaction_id": str(uuid.uuid4()),
        "customer_id": customer_id,
        "amount": amount,
        "currency": random.choice(["MAD", "EUR", "USD"]),
        "type": random.choice(["CARD_PAYMENT", "TRANSFER", "ATM_WITHDRAWAL", "LOAN_REPAYMENT"]),
        "merchant": fake.company(),
        "country": random.choice(countries),
        "timestamp": datetime.utcnow().isoformat() + "Z"
    }
    return transaction

if __name__ == "__main__":
    producer = Producer({'bootstrap.servers': KAFKA_BROKER})
    
    print(f"Starting transaction producer... Publishing to {KAFKA_TOPIC_TRANSACTIONS}")
    try:
        while True:
            transaction = generate_transaction()
            # Envoi du message (en asynchrone)
            producer.produce(
                KAFKA_TOPIC_TRANSACTIONS,
                key=transaction["transaction_id"].encode('utf-8'),
                value=json.dumps(transaction).encode('utf-8'),
                callback=delivery_report
            )
            # Servir les événements de callback
            producer.poll(0)
            
            # Délai aléatoire entre 0.1 et 1.5 secondes
            time.sleep(random.uniform(0.1, 1.5))
    except KeyboardInterrupt:
        print("\nStopping transaction producer.")
    finally:
        # Attendre que tous les messages en attente soient envoyés
        producer.flush()
