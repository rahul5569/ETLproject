# processor/processor.py

import os
import json
import io
import logging
from minio import Minio
from kafka import KafkaConsumer
from kafka.errors import KafkaError
from minio.error import S3Error

# Configure Logging
logging.basicConfig(
    level=logging.INFO,  # Set to DEBUG for more verbose output
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("processor")

# Environment Variables
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
SOURCE_BUCKET = os.getenv("SOURCE_BUCKET", "mybucket")
TARGET_BUCKET = os.getenv("TARGET_BUCKET", "processed-bucket")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "test-topic")
CHUNK_SIZE = int(os.getenv("CHUNK_SIZE", 1000))  # Number of lines per chunk

def init_minio_client():
    try:
        client = Minio(
            MINIO_ENDPOINT,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=False  # Set to True if using HTTPS
        )
        # Create target bucket if it doesn't exist
        if not client.bucket_exists(TARGET_BUCKET):
            client.make_bucket(TARGET_BUCKET)
            logger.info(f"Bucket '{TARGET_BUCKET}' created.")
        else:
            logger.info(f"Bucket '{TARGET_BUCKET}' already exists.")
        return client
    except Exception as e:
        logger.critical(f"Failed to initialize MinIO client: {e}")
        raise

def split_content(content, chunk_size=CHUNK_SIZE):
    """
    Splits the content into chunks of 'chunk_size' lines each.
    """
    lines = content.splitlines()
    for i in range(0, len(lines), chunk_size):
        yield "\n".join(lines[i:i + chunk_size])

def process_file(client, object_name):
    """
    Downloads a file from MinIO, splits its content, and uploads the chunks.
    """
    try:
        # Download the file from MinIO
        logger.info(f"Downloading '{object_name}' from bucket '{SOURCE_BUCKET}'.")
        response = client.get_object(SOURCE_BUCKET, object_name)
        file_content = response.read().decode('utf-8')
        response.close()
        response.release_conn()

        # Split the content into chunks
        chunks = list(split_content(file_content))
        logger.info(f"Splitting '{object_name}' into {len(chunks)} chunks.")

        # Upload each chunk to the target bucket
        for idx, chunk in enumerate(chunks, start=1):
            # Generate a unique name for each chunk
            # For example: chunks/content_the-great-gatsby.txt_chunk_1.txt
            sanitized_object_name = object_name.replace('/', '_')  # Replace '/' to avoid nested paths
            chunk_object_name = f"chunks/{sanitized_object_name}_chunk_{idx}.txt"
            client.put_object(
                bucket_name=TARGET_BUCKET,
                object_name=chunk_object_name,
                data=io.BytesIO(chunk.encode('utf-8')),
                length=len(chunk.encode('utf-8')),
                content_type="text/plain"
            )
            logger.info(f"Uploaded chunk '{chunk_object_name}' to bucket '{TARGET_BUCKET}'.")

    except S3Error as e:
        logger.error(f"MinIO S3Error while processing '{object_name}': {e}")
    except Exception as e:
        logger.error(f"Error processing file '{object_name}': {e}")

def main():
    # Initialize MinIO client
    client = init_minio_client()

    # Initialize Kafka consumer
    try:
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id='processor-group',
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))
        )
        logger.info(f"Connected to Kafka topic '{KAFKA_TOPIC}'.")
    except KafkaError as e:
        logger.critical(f"Failed to connect to Kafka: {e}")
        raise

    # Consume messages
    for message in consumer:
        try:
            data = message.value
            logger.info(f"Received message: {data}")

            # Extract object name
            object_name = data.get("object_name")
            if not object_name:
                logger.warning("Received message without 'object_name'. Skipping.")
                continue

            # Process the file
            process_file(client, object_name)

        except Exception as e:
            logger.error(f"Error consuming message: {e}")

if __name__ == "__main__":
    main()
