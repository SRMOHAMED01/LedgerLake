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
"""
import os
import json
import uuid
from datetime import datetime
from dotenv import load_dotenv
from confluent_kafka import Consumer, KafkaError, KafkaException
import boto3
from botocore.exceptions import ClientError

# Charger les variables d'environnement (soit depuis .env, soit par défaut)
load_dotenv()

# Configuration Kafka (localhost depuis l'hôte, kafka depuis le réseau Docker)
KAFKA_BROKER = os.getenv("KAFKA_BOOTSTRAP_SERVERS_HOST", "localhost:9092")
TOPICS = [
    os.getenv("KAFKA_TOPIC_TRANSACTIONS", "transactions-events"),
    os.getenv("KAFKA_TOPIC_CUSTOMERS", "customers-events"),
    os.getenv("KAFKA_TOPIC_LOANS", "loan-events")
]

# Configuration MinIO
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT_HOST", "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "ledgerlake")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "changeme")
BRONZE_BUCKET = os.getenv("MINIO_BUCKET_BRONZE", "bronze")

# Initialiser le client S3 pour MinIO
s3_client = boto3.client(
    's3',
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY,
    region_name='us-east-1' # Obligatoire pour boto3, même avec MinIO local
)

def create_bucket_if_not_exists(bucket_name):
    try:
        s3_client.head_bucket(Bucket=bucket_name)
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == '404':
            s3_client.create_bucket(Bucket=bucket_name)
            print(f"Bucket {bucket_name} created.")
        else:
            print(f"Error checking/creating bucket: {e}")

def get_s3_key(topic, current_time):
    # topic expected format: xxx-events -> extract xxx
    entity = topic.replace('-events', '')
    year = current_time.strftime("%Y")
    month = current_time.strftime("%m")
    day = current_time.strftime("%d")
    batch_id = str(uuid.uuid4())
    return f"{entity}/year={year}/month={month}/day={day}/{batch_id}.json"

def write_batch_to_minio(topic, messages):
    if not messages:
        return
    
    current_time = datetime.utcnow()
    s3_key = get_s3_key(topic, current_time)
    
    # Combiner les messages (1 JSON par ligne)
    # ou créer un tableau JSON : json.dumps([json.loads(m) for m in messages])
    # Ici, nous formatons en JSON Lines pour une intégration facile avec Spark
    content = "\n".join(messages)
    
    try:
        s3_client.put_object(
            Bucket=BRONZE_BUCKET,
            Key=s3_key,
            Body=content.encode('utf-8')
        )
        print(f"Wrote batch of {len(messages)} messages to s3://{BRONZE_BUCKET}/{s3_key}")
    except Exception as e:
        print(f"Error writing to MinIO: {e}")

if __name__ == "__main__":
    create_bucket_if_not_exists(BRONZE_BUCKET)
    
    conf = {
        'bootstrap.servers': KAFKA_BROKER,
        'group.id': 'bronze_writer_group',
        'auto.offset.reset': 'earliest'
    }
    
    consumer = Consumer(conf)
    consumer.subscribe(TOPICS)
    
    print(f"Subscribed to topics: {TOPICS}")
    print("Waiting for messages...")
    
    # Simple batching mechanism
    BATCH_SIZE = 100
    BATCH_TIMEOUT_MS = 10000 # 10 seconds
    
    # Dictionary to hold batches for each topic
    batches = {topic: [] for topic in TOPICS}
    last_flush_time = time.time()
    
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            
            if msg is not None:
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        # Fin de partition, on ignore
                        pass
                    else:
                        raise KafkaException(msg.error())
                else:
                    topic = msg.topic()
                    payload = msg.value().decode('utf-8')
                    if topic not in batches:
                        batches[topic] = []
                    batches[topic].append(payload)
            
            # Check if we need to flush batches
            current_time = time.time()
            time_elapsed = current_time - last_flush_time
            
            for topic, messages in batches.items():
                if len(messages) >= BATCH_SIZE or (time_elapsed * 1000 >= BATCH_TIMEOUT_MS and len(messages) > 0):
                    write_batch_to_minio(topic, messages)
                    batches[topic] = [] # Clear the batch
            
            if time_elapsed * 1000 >= BATCH_TIMEOUT_MS:
                last_flush_time = current_time
                
    except KeyboardInterrupt:
        print("Stopping consumer.")
    finally:
        # Flush remaining messages
        for topic, messages in batches.items():
            if len(messages) > 0:
                write_batch_to_minio(topic, messages)
        consumer.close()
