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
"""
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, year, month, dayofmonth
from pyspark.sql.types import DoubleType, TimestampType
from dotenv import load_dotenv

load_dotenv()

def get_spark_session():
    # Nécessite les packages hadoop-aws pour lire/écrire sur s3a://
    return SparkSession.builder \
        .appName("bronze_to_silver_transactions") \
        .master("local[*]") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262") \
        .config("spark.hadoop.fs.s3a.endpoint", os.getenv("MINIO_ENDPOINT_HOST", "http://localhost:9000")) \
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ROOT_USER", "ledgerlake")) \
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_ROOT_PASSWORD", "changeme")) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

def process_transactions():
    spark = get_spark_session()
    
    # Paramètres (pourraient être passés en arguments pour un traitement temporel précis)
    bronze_path = "s3a://bronze/transactions/*/*/*/*.json"
    silver_path = "s3a://silver/transactions/"
    
    print(f"Reading from {bronze_path}")
    
    # 1. Lecture
    try:
        df = spark.read.json(bronze_path)
    except Exception as e:
        print(f"Error reading path (maybe empty?): {e}")
        return
        
    if df.isEmpty():
        print("No new data to process.")
        return

    # 2. Nettoyage
    # Supprimer les doublons
    df_clean = df.dropDuplicates(["transaction_id"])
    # Filtrer valeurs invalides
    df_clean = df_clean.filter(col("amount") > 0).filter(col("customer_id").isNotNull())
    
    # 3. Typage
    df_typed = df_clean \
        .withColumn("amount", col("amount").cast(DoubleType())) \
        .withColumn("timestamp", col("timestamp").cast(TimestampType()))

    # 4. Standardisation du pays
    df_std = df_typed.withColumn(
        "country",
        when(col("country").isin("MA", "Maroc"), "Morocco")
        .when(col("country").isin("FR"), "France")
        .when(col("country").isin("US"), "USA")
        .otherwise(col("country"))
    )
    
    # 5. Enrichissement (country_region & extract date for partitions)
    df_enriched = df_std.withColumn(
        "country_region",
        when(col("country") == "Morocco", "Africa")
        .when(col("country") == "France", "Europe")
        .when(col("country") == "USA", "North America")
        .otherwise("Unknown")
    ).withColumn("year", year("timestamp")) \
     .withColumn("month", month("timestamp")) \
     .withColumn("day", dayofmonth("timestamp"))

    print(f"Writing to {silver_path}")
    # 6. Écriture (en mode append ou overwrite selon la strat. En batch quotidien on ferait un overwrite sur la partition)
    df_enriched.write.mode("append").partitionBy("year", "month", "day").parquet(silver_path)
    print("Done processing transactions to Silver layer.")

if __name__ == "__main__":
    process_transactions()
