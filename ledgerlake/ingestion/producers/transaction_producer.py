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

if __name__ == "__main__":
    # boucle infinie de génération + envoi
    pass
"""
