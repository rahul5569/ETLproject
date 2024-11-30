1. myapp is the fastapi service 
2. promethues and grafana integration is remaining
3. we fetch the data from locust. Locust will use post request to the endpoint of our fastapi service at http://localhost:8000/
4. the data is stored in minio and the minio bucket details are sent to processor service to divide it in chunks.
5. the processor service divides the text into chunk and stored in the bucket.
6. minio can be accessed via link http://127.0.0.1:9001/browser with username and password - minioadmin