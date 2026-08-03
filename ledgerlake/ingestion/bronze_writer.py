"""
bronze_writer.py — Consumer Kafka qui écrit brut dans Bronze (Palier A.2)

RÈGLE D'OR : ce script ne transforme JAMAIS les données. Il les écrit
exactement comme elles arrivent (raw), en JSON, partitionnées par date.

À IMPLÉMENTER :
1. Consumer confluent_kafka.Consumer abonné aux 3 topics
   (transactions-events, customers-events, loan-events)
2. Pour chaque message reçu, écrire dans MinIO (via boto3, API compatible S3)
   au chemin :
     bronze/<topic>/year=YYYY/month=MM/day=DD/<batch_id>.json
3. Batcher les écritures (ex: toutes les 100 messages ou toutes les 30s)
   plutôt qu'un fichier par message, pour éviter de créer des milliers de
   micro-fichiers (mauvais pour la performance de lecture ensuite)

if __name__ == "__main__":
    # run the consume + write loop
    pass
"""
