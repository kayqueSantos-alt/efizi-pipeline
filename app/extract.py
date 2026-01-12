import requests
import time
from datetime import datetime, timedelta
from typing import Dict, Any, List
from app.config import Config
from app.gcs_handler import logger

class ExtratorBling:
    URL_BASE = "https://www.bling.com.br/Api/v3"

    def __init__(self, servico_autenticacao, manipulador_gcs):
        self.auth = servico_autenticacao
        self.gcs = manipulador_gcs
        # Define D-1 (Ontem) como padrão
        self.data_alvo = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')

    def _obter_cabecalhos(self) -> Dict[str, str]:
        """Gera os headers com token válido."""
        token = self.auth.obter_token_valido()
        return {
            "Authorization": f"Bearer {token}",
            "Accept": "application/json"
        }

    def _buscar_todas_paginas(self, endpoint: str, parametros: Dict[str, Any]) -> List[Dict]:
        """
        MÉTODO GENÉRICO (O MOTOR):
        Faz o loop, trata paginação, erro 429 e token expirado.
        Funciona para Vendas, Produtos, Contatos, etc.
        """
        pagina = 1
        dados_coletados = []
        url = f"{self.URL_BASE}/{endpoint}"
        
        # Garante headers atualizados no inicio do loop
        cabecalhos = self._obter_cabecalhos()

        logger.info(f"Iniciando extração: {endpoint} | Params: {parametros}")

        while True:
            parametros['pagina'] = pagina
            parametros['limite'] = 100 

            try:
                resposta = requests.get(url, headers=cabecalhos, params=parametros)

                # Tratamento de Rate Limit (429 - Muitas requisições)
                if resposta.status_code == 429:
                    logger.warning("Rate limit atingido. Aguardando 3s...")
                    time.sleep(3)
                    continue
                
                # Tratamento de Token Expirado (401)
                if resposta.status_code == 401:
                    logger.warning("Token expirou. Renovando...")
                    cabecalhos = self._obter_cabecalhos() # Renova e tenta de novo
                    continue

                if resposta.status_code != 200:
                    logger.error(f"Erro API {endpoint}: {resposta.status_code} - {resposta.text}")
                    break

                payload = resposta.json()
                itens = payload.get('data', [])

                if not itens:
                    break

                dados_coletados.extend(itens)
                logger.debug(f"Página {pagina} obtida: {len(itens)} itens.")
                
                pagina += 1
                time.sleep(0.3) # Respeita limites da API

            except Exception as e:
                logger.error(f"Erro crítico: {e}")
                break
        
        return dados_coletados

    def _salvar(self, dados: List[Dict], pasta: str):
        """Salva no GCS particionado por data."""
        if not dados:
            logger.info(f"Sem dados para salvar em {pasta}.")
            return

        caminho = f"{Config.CAMINHO_BASE_RAW}/{pasta}/data_ref={self.data_alvo}/data.json"
        self.gcs.salvar_json(caminho, dados)

    # ---------------------------------------------------------
    # CONFIGURAÇÕES DE CADA ENDPOINT (ADICIONE NOVOS AQUI)
    # ---------------------------------------------------------

    def extrair_vendas(self):
        """Configuração para Pedidos de Venda"""
        endpoint = "vendas"
        
        # CORREÇÃO DO ERRO: 
        # Na V3 usa-se 'dataInicial' e não 'dataInclusaoInicial'
        parametros = {
            "dataInicial": self.data_alvo,
            "dataFinal": self.data_alvo
        }
        
        dados = self._buscar_todas_paginas(endpoint, parametros)
        self._salvar(dados, "pedidos_vendas")

        return len(dados)

    def extrair_produtos(self):
        """Configuração para Produtos (Exemplo de escalabilidade)"""
        endpoint = "produtos"
        
        # Produtos geralmente pegamos ativos ou alterados recentemente
        # Aqui pegamos todos os ativos (criterio=1)
        parametros = {
            "criterio": 1, 
            "tipo": "P"
        }
        
        dados = self._buscar_todas_paginas(endpoint, parametros)
        # Nota: Produtos podem não ser salvos por data_ref, depende da sua estratégia,
        # mas aqui mantive o padrão.
        self._salvar(dados, "produtos")

        return len(dados)

    def executar_pipeline_diario(self):
        """Roda tudo que precisa ser extraído no dia"""
        logger.info(f"--- Pipeline Iniciado: {self.data_alvo} ---")
        
        self.extrair_vendas()
        # self.extrair_produtos() # Descomente para rodar produtos também
        
        logger.info("--- Pipeline Finalizado ---")
