import json
import logging
from google.cloud import storage

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("BlingGCS")

class GCSHandler:
    def __init__(self, bucket_name):
        self.client = storage.Client()
        self.bucket = self.client.bucket(bucket_name)

    def read_json(self, blob_path):
        blob = self.bucket.blob(blob_path)
        if not blob.exists():
            return None
        return json.loads(blob.download_as_text())

    def salvar_json(self, blob_path, data):
        blob = self.bucket.blob(blob_path)
        blob.upload_from_string(
            json.dumps(data, indent=4, ensure_ascii=False),
            content_type='application/json'
        )
        logger.info(f"Salvo no GCS: gs://{self.bucket.name}/{blob_path}")