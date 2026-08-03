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

NOTE : ces données peuvent être les mêmes que celles utilisées par CreditPulse
(Palier H) pour fermer la boucle plateforme de données -> moteur de décision.
"""
