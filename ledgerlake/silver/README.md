# silver/ — Convention de stockage de la couche Silver (Palier B)

Documente la convention de partitionnement des données nettoyées, écrites par
les jobs Spark (spark_jobs/).

Structure attendue dans MinIO (bucket "silver"), format Parquet :

    silver/
      transactions/year=2026/month=08/day=03/part-*.parquet
      customers/year=2026/month=08/day=03/part-*.parquet

Contenu : données dédupliquées, typées, standardisées, enrichies
(customer_segment, country_region). Voir docs/GUIDE_COMPLET.md Palier B.
