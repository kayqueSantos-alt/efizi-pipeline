import requests
import time
from datetime import datetime, timedelta
from app.config import Config
from app.gcs_handler import logger

class BlingExtractor:
    def __init__(self, auth_service, gcs_handler):
        self.auth = auth_service
        self.gcs = gcs_handler

    def run_daily_extraction(self):
        # Pega D-1 (Ontem)
        target_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        logger.info(f"Iniciando extração para: {target_date}")
        
        token = self.auth.get_valid_token()
        headers = {"Authorization": f"Bearer {token}"}
        
        page = 1
        data_buffer = []
        
        while True:
            resp = requests.get(
                "https://www.bling.com.br/Api/v3/pedidos/vendas",
                headers=headers,
                params={"pagina": page, "limite": 100, "dataInclusaoInicial": target_date, "dataInclusaoFinal": target_date}
            )
            
            if resp.status_code == 429:
                time.sleep(2)
                continue
                
            if resp.status_code != 200:
                logger.error(f"Erro API: {resp.text}")
                break
                
            items = resp.json().get('data', [])
            if not items: break
            
            data_buffer.extend(items)
            page += 1
            time.sleep(0.3)

        # Salva no caminho particionado: raw/bling/pedidos/data_ref=YYYY-MM-DD/data.json
        if data_buffer:
            path = f"{Config.RAW_BASE_PATH}/pedidos/data_ref={target_date}/data.json"
            self.gcs.write_json(path, data_buffer)
            return len(data_buffer)
        return 0