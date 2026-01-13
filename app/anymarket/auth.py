# app/anymarket/auth.py
from typing import Dict
from app.config import Config
from app.gcs_handler import logger

class AnymarketAuth:
    def __init__(self,gcs_handler):
        self.token = Config.ANYMARKET_TOKEN

    def obter_cabecalhos(self) -> Dict[str, str]:
        """
        Retorna os headers para autenticação (GumgaToken).
        """
        if not self.token:
            logger.error("FATAL: ANYMARKET_TOKEN não encontrado.")
            raise Exception("Token do AnyMarket não configurado.")
            
        return {
            "gumgaToken": self.token,
            "Content-Type": "application/json"
        }