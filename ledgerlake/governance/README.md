# governance/ — OpenMetadata (Palier F)

À IMPLÉMENTER :
1. Déployer OpenMetadata via Docker (service à ajouter dans docker-compose.yml)
2. Connecter les "ingestion connectors" OpenMetadata à :
   - MinIO (pour cataloguer Bronze/Silver)
   - Ton entrepôt Gold (DuckDB/PostgreSQL)
   - Airflow (pour capturer le lineage des DAGs)
3. Documenter manuellement (ou via API) pour chaque table Gold :
   - owner (ex: "Risk Department" pour loan_portfolio_analysis)
   - description métier
   - tags de sensibilité des données (ex: "PII" sur customer_360)

Objectif final : pouvoir ouvrir l'UI OpenMetadata, chercher "customer_360"
et voir sa lignée complète Kafka -> Bronze -> Silver -> Gold, visible
graphiquement (data lineage).
