# spark_jobs/ — Jobs Spark Bronze → Silver (Palier B)

Ordre de développement recommandé :
1. bronze_to_silver_transactions.py (le plus riche, à faire en premier)
2. bronze_to_silver_customers.py
3. bronze_to_silver_loans.py

## Transactions : stratégie de reconstruction

Le job transactions relit tout l'historique Bronze retenu et remplace le jeu
Silver complet. Un replay strictement identique d'un `transaction_id` est
dédupliqué. Deux contenus différents portant le même identifiant font échouer
le job avant l'écriture : aucune version arbitraire n'est choisie.

Cette stratégie rend deux exécutions successives réussies idempotentes et
répare les doublons historiques créés par l'ancien mode append. Elle suppose :

- que Bronze conserve l'historique complet ;
- qu'un seul writer Silver s'exécute à la fois ;
- qu'un échec pendant l'overwrite est suivi d'une nouvelle exécution.

L'overwrite Parquet n'est pas une transaction atomique. Pour une plateforme
multi-writer ou de production, la prochaine évolution sera une table ACID
(Iceberg, Delta ou Hudi) avec `MERGE` et snapshots.

## Qualité Silver transactions

Le job transactions classe maintenant chaque version unique dans l'un des deux
jeux de données :

- `s3a://silver/transactions/` : événements acceptés, montant en
  `DecimalType(18,2)`, pays normalisé et partitions UTC ;
- `s3a://silver-rejected/transactions/` : événement brut, raison principale et
  date de rejet.

Les replays strictement identiques sont comptés puis supprimés. Toutes les
versions distinctes partageant un `transaction_id` sont mises en quarantaine
avec `DUPLICATE_CONFLICT`. Chaque exécution impose l'égalité suivante :

```text
input = accepted + rejected + exact_duplicates
```

Une exécution sans aucune ligne acceptée met à jour la quarantaine mais conserve
le Silver accepté précédent. Les erreurs de lecture, stockage ou réconciliation
font échouer le job.

## Tests locaux

Les régressions Spark couvrent le replay, le rerun, les doublons historiques,
les arrivées tardives, les conflits d'identifiants, les totaux et les erreurs :

```bash
python -B -m unittest discover -s ledgerlake/spark_jobs/tests -v
```
