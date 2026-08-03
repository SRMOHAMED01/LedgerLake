# bronze/ — Convention de stockage de la couche Bronze (Palier A.2)

Ce dossier ne contient PAS de code : il documente la convention de partitionnement
utilisée dans MinIO (bucket "bronze"), pour que tu t'y retrouves.

Structure attendue dans MinIO :

    bronze/
      transactions/
        year=2026/month=08/day=03/transactions_<batch_id>.json
      customers/
        year=2026/month=08/day=03/customers_<batch_id>.json
      loans/
        year=2026/month=08/day=03/loans_<batch_id>.json

RÈGLE D'OR : never modify raw data. Bronze est ton historique brut immuable —
si un bug apparaît en Silver/Gold, tu dois pouvoir rejouer depuis Bronze.

Format : JSON pour commencer (facile à débugger). Tu passeras en Parquet
uniquement à partir de la couche Silver (Palier B).
