# ingestion/ — Producers Kafka (Palier A.1) + writer Bronze (Palier A.2)

Simule les événements financiers d'une banque et les publie dans Kafka.
Le bronze_writer.py consomme ces topics et écrit brut dans le data lake (bronze/).
