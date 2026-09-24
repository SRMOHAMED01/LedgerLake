"""
bronze_writer.py — Consumer Kafka qui écrit brut dans Bronze (Palier A.2)

RÈGLE D'OR : ce script ne transforme JAMAIS les données. Il les écrit
exactement comme elles arrivent (raw), en JSON, partitionnées par date.

Fonctionnement :
1. Consumer confluent_kafka.Consumer abonné aux 3 topics
   (transactions-events, customers-events, loan-events)
2. Pour chaque message reçu, écrire dans MinIO (via boto3, API compatible S3)
   au chemin :
     bronze/<topic>/year=YYYY/month=MM/day=DD/<batch_id>.json
3. Batcher par partition (100 messages ou toutes les 10s)
   plutôt qu'un fichier par message, pour éviter de créer des milliers de
   micro-fichiers (mauvais pour la performance de lecture ensuite)
"""
import os
import time
import uuid
from datetime import datetime, timezone
from dotenv import load_dotenv
from confluent_kafka import Consumer, KafkaError, KafkaException, TopicPartition
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

def create_s3_client():
    return boto3.client(
        's3',
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        region_name='us-east-1',
    )


def create_bucket_if_not_exists(s3_client, bucket_name):
    try:
        s3_client.head_bucket(Bucket=bucket_name)
    except ClientError as error:
        if error.response['Error']['Code'] in ('404', 'NoSuchBucket'):
            s3_client.create_bucket(Bucket=bucket_name)
        else:
            raise


def get_s3_key(topic, current_time):
    # topic expected format: xxx-events -> extract xxx
    entity = topic.replace('-events', '')
    if entity == "loan":
        entity = "loans"
    year = current_time.strftime("%Y")
    month = current_time.strftime("%m")
    day = current_time.strftime("%d")
    batch_id = str(uuid.uuid4())
    return f"{entity}/year={year}/month={month}/day={day}/{batch_id}.json"

def write_batch_to_minio(s3_client, topic, messages):
    """Persist raw payloads; propagate failures so offsets remain uncommitted."""
    if not messages:
        return
    s3_key = get_s3_key(topic, datetime.now(timezone.utc))
    # Do not parse or transform the financial payloads.
    content = b"\n".join(message.value() for message in messages)
    s3_client.put_object(
        Bucket=BRONZE_BUCKET,
        Key=s3_key,
        Body=content,
        ContentType='application/x-ndjson',
    )
    print(f"Wrote {len(messages)} messages to s3://{BRONZE_BUCKET}/{s3_key}")


class BronzeWriter:
    """At-least-once writer: a partition is acknowledged only after storage."""

    def __init__(self, consumer, s3_client):
        self.consumer = consumer
        self.s3_client = s3_client
        self.batches = {}

    def add(self, message):
        # A tombstone cannot be represented as a raw JSON line. Fail without
        # acknowledging it; deciding how to store tombstones is a data contract.
        if message.value() is None:
            raise ValueError('Kafka tombstone cannot be stored as JSON Lines')
        key = (message.topic(), message.partition())
        self.batches.setdefault(key, []).append(message)
        return key

    def flush(self, key):
        messages = self.batches.get(key)
        if not messages:
            return
        topic, partition = key
        write_batch_to_minio(self.s3_client, topic, messages)
        # Commit only this persisted partition, never the consumer's global
        # position (other partitions may still have unwritten buffered data).
        results = self.consumer.commit(
            offsets=[TopicPartition(topic, partition, messages[-1].offset() + 1)],
            asynchronous=False,
        )
        for result in results:
            if result.error is not None:
                raise KafkaException(result.error)
        del self.batches[key]

    def flush_all(self):
        for key in list(self.batches):
            self.flush(key)

    def release_partitions(self, consumer, partitions):
        # Do not acknowledge data for partitions being reassigned. Kafka will
        # replay it from the committed offsets to the next owner.
        for partition in partitions:
            self.batches.pop((partition.topic, partition.partition), None)

    def run(self, batch_size=100, batch_timeout=10.0):
        try:
            self.consumer.subscribe(
                TOPICS,
                on_revoke=self.release_partitions,
                on_lost=self.release_partitions,
            )
            last_flush = time.monotonic()
            while True:
                message = self.consumer.poll(timeout=1.0)
                if message is not None:
                    if message.error():
                        if message.error().code() != KafkaError._PARTITION_EOF:
                            raise KafkaException(message.error())
                    else:
                        key = self.add(message)
                        if len(self.batches[key]) >= batch_size:
                            self.flush(key)
                now = time.monotonic()
                if now - last_flush >= batch_timeout:
                    self.flush_all()
                    last_flush = now
        except KeyboardInterrupt:
            print('Stopping consumer; persisting pending messages.')
            self.flush_all()
        finally:
            # Storage/commit failures propagate and terminate the process.
            # Do not try another flush on failure: restart replays uncommitted
            # data, including uncertain writes. Automatic commits are disabled.
            self.consumer.close()


def main():
    s3_client = create_s3_client()
    create_bucket_if_not_exists(s3_client, BRONZE_BUCKET)
    consumer = Consumer({
        'bootstrap.servers': KAFKA_BROKER,
        'group.id': 'bronze_writer_group',
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False,
        'enable.auto.offset.store': False,
    })
    BronzeWriter(consumer, s3_client).run()


if __name__ == '__main__':
    main()
