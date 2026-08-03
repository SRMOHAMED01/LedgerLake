"""
bronze_to_silver_transactions.py — Nettoyage des transactions (Palier B.1)

À IMPLÉMENTER :
1. spark.read.json("s3a://bronze/transactions/...") — lire une plage de dates
2. Nettoyage :
   - dropDuplicates(["transaction_id"])
   - filter(col("amount") > 0) et filter(col("customer_id").isNotNull())
3. Typage :
   - withColumn("amount", col("amount").cast(DoubleType()))
   - withColumn("timestamp", col("timestamp").cast(TimestampType()))
4. Standardisation du pays :
   - mapping {"MA": "Morocco", "Maroc": "Morocco", "Morocco": "Morocco"}
     via une UDF ou un `when/otherwise` chain
5. Enrichissement :
   - country_region (mapping pays -> région, ex: dict simple)
6. Écriture :
   df.write.partitionBy("year", "month", "day").parquet("s3a://silver/transactions/")

if __name__ == "__main__":
    # spark = SparkSession.builder.appName("bronze_to_silver_transactions")...
    pass
"""
