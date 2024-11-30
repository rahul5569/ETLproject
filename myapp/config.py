# myapp/config.py

import os
import time
import json
import logging
from minio import Minio
from minio.error import S3Error
from kafka import KafkaProducer
from kafka.errors import KafkaError

# Configure Logging
logging.basicConfig(
    level=logging.INFO,  # Set to DEBUG for more verbose output
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("myapp")

# Environment Variables
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "mybucket")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "test-topic")

# Initialize MinIO client
def init_minio():
    for attempt in range(5):
        try:
            client = Minio(
                MINIO_ENDPOINT,
                access_key=MINIO_ACCESS_KEY,
                secret_key=MINIO_SECRET_KEY,
                secure=False  # Set to True if using HTTPS
            )
            # Create bucket if it doesn't exist
            if not client.bucket_exists(MINIO_BUCKET):
                client.make_bucket(MINIO_BUCKET)
                logger.info(f"Bucket '{MINIO_BUCKET}' created.")
            else:
                logger.info(f"Bucket '{MINIO_BUCKET}' already exists.")
            return client
        except S3Error as e:
            logger.error(f"MinIO connection failed: {e}. Retrying in 5 seconds...")
            time.sleep(5)
    logger.critical("Failed to connect to MinIO after multiple attempts.")
    raise Exception("Failed to connect to MinIO after multiple attempts.")

minio_client = init_minio()

# Initialize Kafka producer
producer = None  # Global variable to hold the Kafka producer

def init_kafka_producer():
    global producer
    for attempt in range(5):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                api_version=(0, 11, 5),
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            logger.info("Connected to Kafka successfully.")
            break
        except KafkaError as e:
            logger.error(f"Kafka connection failed: {e}. Retrying in 5 seconds...")
            time.sleep(5)
    else:
        logger.critical("Failed to connect to Kafka after multiple attempts.")
        producer = None
