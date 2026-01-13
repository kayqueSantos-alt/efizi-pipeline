from app.gcs_handler import GCSHandler
from app.anymarket.auth import AnymarketAuth
from app.anymarket.extract import ExtratorAnymarket

def executar_pipeline(bucket_name: str) -> int:
    gcs = GCSHandler(bucket_name)
    auth = AnymarketAuth()
    extractor = ExtratorAnymarket(auth, gcs)
    return extractor.executar_pipeline_diario()
