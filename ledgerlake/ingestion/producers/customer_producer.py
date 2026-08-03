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
