"""
financial_daily_pipeline.py — DAG Airflow principal (Palier E.1)

À IMPLÉMENTER : DAG avec les tâches suivantes, dans l'ordre (dépendances
explicites entre tâches via >>) :

1. extract_and_validate_bronze
   -> vérifie que les nouveaux fichiers Bronze du jour sont bien arrivés

2. quality_check_bronze
   -> exécute le checkpoint Great Expectations "bronze_to_silver"
      (data_quality/great_expectations/)

3. spark_bronze_to_silver_transactions
   spark_bronze_to_silver_customers
   spark_bronze_to_silver_loans
   -> déclenche les 3 jobs Spark (spark_jobs/), en parallèle si possible

4. quality_check_silver
   -> checkpoint Great Expectations "silver_to_gold"

5. dbt_run_gold
   -> BashOperator qui lance `dbt run` (dbt_project/)

6. dbt_test_gold
   -> BashOperator qui lance `dbt test`

7. notify_completion
   -> log ou notification de succès/échec

schedule_interval : '@daily' en production, mais mets un trigger manuel
pendant le développement pour itérer plus vite.
"""
