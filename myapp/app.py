# myapp/app.py

from fastapi import FastAPI, HTTPException, UploadFile, File, Form
from pydantic import BaseModel
from minio import Minio
from kafka import KafkaProducer
import json
import os
import io
import time
import logging
from minio.error import S3Error
from kafka.errors import KafkaError
from prometheus_fastapi_instrumentator import Instrumentator  # Added for Prometheus

# Configure Logging
logging.basicConfig(
    level=logging.INFO,  # Set to DEBUG for more verbose output
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("myapp")

app = FastAPI()

# Initialize Prometheus Instrumentator
instrumentator = Instrumentator()
instrumentator.instrument(app).expose(app)

# Environment Variables
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "mybucket")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "test-topic")  # Added for flexibility

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

# Initialize Kafka producer in startup event
@app.on_event("startup")
def startup_event():
    global producer
    for attempt in range(5):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                api_version=(0,11,5),
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

@app.post("/upload/")
async def upload_file(
    file: UploadFile = File(...),
    id: int = Form(...),
    name: str = Form(...),
    value: float = Form(...)
):
    if not producer:
        logger.error("Kafka producer not initialized.")
        raise HTTPException(status_code=500, detail="Kafka producer not initialized.")

    try:
        # Read file content
        file_content = await file.read()
        logger.debug(f"Received file '{file.filename}' with size {len(file_content)} bytes.")

        # Store file in MinIO
        object_name = f"content/{file.filename}"
        minio_client.put_object(
            bucket_name=MINIO_BUCKET,
            object_name=object_name,
            data=io.BytesIO(file_content),
            length=len(file_content),
            content_type=file.content_type
        )
        logger.info(f"Stored '{object_name}' in MinIO.")

        # Create the JSON payload for Kafka, including 'object_name'
        json_payload = {
            "id": id,
            "name": name,
            "value": value,
            "object_name": object_name  # Include object_name
        }

        # Send data to Kafka
        future = producer.send(KAFKA_TOPIC, json_payload)
        producer.flush()
        result = future.get(timeout=10)  # Blocks until a single message is sent (or timeout)
        logger.info(f"Sent data to Kafka topic '{KAFKA_TOPIC}' with offset {result.offset}.")

        return {"message": "File uploaded and data sent to Kafka successfully.", "object_name": object_name}

    except S3Error as e:
        logger.error(f"Failed to store file in MinIO: {e}")
        raise HTTPException(status_code=500, detail="Failed to store file in MinIO.")
    except KafkaError as e:
        logger.error(f"Failed to send data to Kafka: {e}")
        raise HTTPException(status_code=500, detail="Failed to send data to Kafka.")
    except Exception as e:
        logger.exception("An unexpected error occurred.")
        raise HTTPException(status_code=500, detail="An unexpected error occurred.")
