from app.gcs_handler import GCSHandler
from app.bling.auth import BlingAuth
from app.bling.extract import ExtratorBling

def executar_pipeline(bucket_name: str) -> int:
    gcs = GCSHandler(bucket_name)
    auth = BlingAuth(gcs)
    extractor = ExtratorBling(auth, gcs)
    return extractor.executar_pipeline_diario()
