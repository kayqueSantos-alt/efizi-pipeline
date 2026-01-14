# app/anymarket/extractor.py
import requests
import time
from datetime import datetime, timedelta
from typing import Dict, Any, List

from app.config import Config
from app.gcs_handler import logger
from .auth import AnymarketAuth
import json

class ExtratorAnymarket:
    URL_BASE = "https://api.anymarket.com.br/v2"

    def __init__(self, servico_autenticacao, manipulador_gcs):
        self.auth = servico_autenticacao
        self.gcs = manipulador_gcs
        # Data D-1 (Ontem)
        self.data_alvo = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')

    def _buscar_todas_paginas(self, endpoint: str, parametros: Dict[str, Any]) -> List[Dict]:
        """
        MÉTODO MOTOR (Adaptado para OFFSET):
        Diferente do Bling (pagina 1, 2...), o AnyMarket usa offset (0, 100, 200...).
        """
        offset = 0
        limite = 100
        dados_coletados = []
        url = f"{self.URL_BASE}/{endpoint}"
        
        cabecalhos = self.auth.obter_cabecalhos()
        
        logger.info(f"Iniciando extração AnyMarket: {endpoint} | Offset Inicial: {offset}")

        while True:
            # Atualiza os parâmetros de paginação
            parametros["limit"] = limite
            parametros["offset"] = offset

            try:
                resposta = requests.get(url, headers=cabecalhos, params=parametros, timeout=30)
                
                if resposta.status_code != 200:
                    logger.error(f"Erro API AnyMarket ({resposta.status_code}): {resposta.text}")
                    break

                # AnyMarket retorna os dados dentro de 'content' ou 'data' dependendo do endpoint
                payload = resposta.json()
                # Tenta pegar 'content' (padrão v2), se não tiver tenta 'data'
                itens = payload.get("content") or payload.get("data", [])

                if not itens:
                    logger.info(f"Fim da paginação. Total processado: {len(dados_coletados)}")
                    break

                dados_coletados.extend(itens)
                logger.info(f"Offset {offset} baixado: {len(itens)} itens.")

                # Prepara para a próxima página
                offset += limite
                time.sleep(0.5) # Respeitar limites da API

            except Exception as e:
                logger.error(f"Erro crítico na extração: {e}")
                break
                
        return dados_coletados

    def _salvar_no_gcs(self, dados: List[Dict], pasta: str) -> None:
        """Persiste dados no GCS com particionamento por data em formato NDJSON."""
        if not dados:
            logger.info(f"Nenhum dado para salvar em {pasta}")
            return
        
        # Validação básica
        if not all(isinstance(d, dict) for d in dados):
            raise ValueError("Todos os itens devem ser dicionários")
        
        caminho = (
            f"{Config.CAMINHO_BASE_RAW_ANYMARKET}/{pasta}/"
            f"data_ref={self.data_alvo}/data.json"
        )
        
        try:
            # Mais eficiente em memória
            conteudo_ndjson = '\n'.join(
                json.dumps(registro, ensure_ascii=False) 
                for registro in dados
            )
            
            blob = self.bucket.blob(caminho)
            
            # Metadados opcionais
            blob.metadata = {'num_registros': str(len(dados))}
            
            blob.upload_from_string(
                conteudo_ndjson,
                content_type='application/x-ndjson; charset=utf-8'
            )
            
            logger.info(
                f"✓ Salvos {len(dados)} registros em gs://{self.BUCKET_NAME}/{caminho}"
            )
            
        except Exception as e:
            logger.error(f"Erro ao salvar no GCS ({caminho}): {e}")
            raise

    # ---------------------------------------------------------
    # MÉTODOS DE NEGÓCIO
    # ---------------------------------------------------------

    def extrair_pedidos(self):
        """Extrai pedidos criados ontem."""
        endpoint = "orders"
        
        # AnyMarket exige data completa ISO 8601 (ex: 2023-10-27T00:00:00Z)
        data_iso = f"{self.data_alvo}T00:00:00Z"
        
        parametros = {
            "createdAfter": data_iso,
            # Se quiser filtrar por status, adicione aqui: "status": "PAID_WAITING_SHIP"
        }
        
        dados = self._buscar_todas_paginas(endpoint, parametros)
        self._salvar_no_gcs(dados, "orders")
        return len(dados)

    def executar_pipeline_diario(self):
        logger.info(f"--- Pipeline AnyMarket ({self.data_alvo}) ---")
        qtd = self.extrair_pedidos()
        logger.info("--- Pipeline Finalizado ---")
        return qtd