import os

class Config:
    # Segredos vêm do ambiente (Cloud Run)
    BUCKET_NAME = os.getenv("BUCKET_NAME")
    BLING_CLIENT_ID = os.getenv("BLING_CLIENT_ID")
    BLING_CLIENT_SECRET = os.getenv("BLING_CLIENT_SECRET")
    
    # Caminhos Fixos no Storage
    # O token fica na pasta de configuração
    TOKEN_PATH = "config/bling/tokens.json"
    
    # Os dados brutos ficam na pasta raw
    CAMINHO_BASE_RAW = "raw/bling"