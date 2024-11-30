# myapp/routes.py

from fastapi import APIRouter, HTTPException, UploadFile, File
import io
from minio.error import S3Error
from kafka.errors import KafkaError
import config  # Import the config module instead of individual variables

router = APIRouter()

@router.post("/upload/")
async def upload_file(
    file: UploadFile = File(...)
):
    if not config.producer:
        config.logger.error("Kafka producer not initialized.")
        raise HTTPException(status_code=500, detail="Kafka producer not initialized.")

    try:
        # Read file content
        file_content = await file.read()
        config.logger.debug(f"Received file '{file.filename}' with size {len(file_content)} bytes.")

        # Store file in MinIO
        object_name = f"content/{file.filename}"
        config.minio_client.put_object(
            bucket_name=config.MINIO_BUCKET,
            object_name=object_name,
            data=io.BytesIO(file_content),
            length=len(file_content),
            content_type=file.content_type
        )
        config.logger.info(f"Stored '{object_name}' in MinIO.")

        # Create the JSON payload for Kafka, including 'object_name'
        json_payload = {
            "object_name": object_name  # Only include object_name
        }

        # Send data to Kafka
        future = config.producer.send(config.KAFKA_TOPIC, json_payload)
        config.producer.flush()
        result = future.get(timeout=10)  # Blocks until a single message is sent (or timeout)
        config.logger.info(f"Sent data to Kafka topic '{config.KAFKA_TOPIC}' with offset {result.offset}.")

        return {"message": "File uploaded and data sent to Kafka successfully.", "object_name": object_name}

    except S3Error as e:
        config.logger.error(f"Failed to store file in MinIO: {e}")
        raise HTTPException(status_code=500, detail="Failed to store file in MinIO.")
    except KafkaError as e:
        config.logger.error(f"Failed to send data to Kafka: {e}")
        raise HTTPException(status_code=500, detail="Failed to send data to Kafka.")
    except Exception as e:
        config.logger.exception("An unexpected error occurred.")
        raise HTTPException(status_code=500, detail="An unexpected error occurred.")
