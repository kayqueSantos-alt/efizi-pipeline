import requests
import base64
import time
from app.config import Config, os
from app.gcs_handler import logger

class BlingAuth:
    def __init__(self, gcs_handler):
        self.gcs = gcs_handler
        self.base_url = "https://www.bling.com.br/Api/v3/oauth/token"

    def get_valid_token(self):
        tokens = self.gcs.read_json(Config.TOKEN_PATH)
        if not tokens:
            raise Exception("FATAL: tokens.json não encontrado no Bucket.")

        # Se criado há mais de 50 min, renova
        created_at = tokens.get('created_at', 0)
        if (time.time() - created_at) > 3000:
            logger.info("Token expirando. Renovando...")
            return self._refresh_token(tokens['refresh_token'])
        
        return tokens['access_token']

    def _refresh_token(self, refresh_token):
        credentials = f"{Config.BLING_CLIENT_ID}:{Config.BLING_CLIENT_SECRET}"
        encoded = base64.b64encode(credentials.encode()).decode()
        
        payload = {"grant_type": "refresh_token", "refresh_token": refresh_token}
        headers = {"Authorization": f"Basic {encoded}", "Content-Type": "application/x-www-form-urlencoded"}

        resp = requests.post(self.base_url, data=payload, headers=headers)
        if resp.status_code == 200:
            new_tokens = resp.json()
            new_tokens['created_at'] = time.time()
            self.gcs.write_json(Config.TOKEN_PATH, new_tokens)
            return new_tokens['access_token']
        else:
            raise Exception(f"Erro Auth: {resp.text}")