import requests
import time
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional
from app.config import Config
from app.gcs_handler import logger


class ExtratorBling:
    """
    Extrator de dados da API Bling v3.
    Gerencia paginação, rate limiting e renovação de token automaticamente.
    """
    
    URL_BASE = "https://www.bling.com.br/Api/v3"
    LIMITE_POR_PAGINA = 100
    DELAY_ENTRE_REQUISICOES = 0.3
    DELAY_RATE_LIMIT = 3
    
    def __init__(self, servico_autenticacao, manipulador_gcs):
        self.auth = servico_autenticacao
        self.gcs = manipulador_gcs
        self.data_alvo = self._calcular_data_alvo()
    
    @staticmethod
    def _calcular_data_alvo() -> str:
        """Calcula D-1 (ontem) como data alvo padrão."""
        return (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    
    def _obter_cabecalhos(self) -> Dict[str, str]:
        """Gera headers HTTP com token de autenticação válido."""
        token = self.auth.obter_token_valido()
        return {
            "Authorization": f"Bearer {token}",
            "Accept": "application/json"
        }
    
    def _construir_url(self, endpoint: str) -> str:
        """Constrói URL completa do endpoint."""
        return f"{self.URL_BASE}/{endpoint}"
    
    def _tratar_resposta_erro(
        self, 
        resposta: requests.Response, 
        endpoint: str
    ) -> Optional[str]:
        """
        Trata códigos de erro HTTP.
        Retorna ação a ser tomada: 'retry', 'renovar_token', 'parar' ou None.
        """
        if resposta.status_code == 429:
            logger.warning(f"Rate limit atingido em {endpoint}. Aguardando {self.DELAY_RATE_LIMIT}s...")
            time.sleep(self.DELAY_RATE_LIMIT)
            return 'retry'
        
        if resposta.status_code == 401:
            logger.warning(f"Token expirado em {endpoint}. Renovando...")
            return 'renovar_token'
        
        if resposta.status_code != 200:
            logger.error(
                f"Erro na API {endpoint}: "
                f"Status {resposta.status_code} - {resposta.text}"
            )
            return 'parar'
        
        return None
    
    def _extrair_dados_resposta(self, resposta: requests.Response) -> List[Dict]:
        """Extrai dados do payload JSON da resposta."""
        payload = resposta.json()
        return payload.get('data', [])
    
    def _buscar_todas_paginas(
        self, 
        endpoint: str, 
        parametros: Dict[str, Any]
    ) -> List[Dict]:
        """
        Motor genérico de extração paginada.
        Gerencia paginação, rate limiting e renovação de token automaticamente.
        Funciona para qualquer endpoint da API Bling v3.
        """
        pagina = 1
        dados_coletados = []
        url = self._construir_url(endpoint)
        cabecalhos = self._obter_cabecalhos()
        
        logger.info(f"Iniciando extração: {endpoint} | Parâmetros: {parametros}")
        
        while True:
            parametros_requisicao = {
                **parametros,
                'pagina': pagina,
                'limite': self.LIMITE_POR_PAGINA
            }
            
            try:
                resposta = requests.get(
                    url, 
                    headers=cabecalhos, 
                    params=parametros_requisicao
                )
                
                acao = self._tratar_resposta_erro(resposta, endpoint)
                
                if acao == 'retry':
                    continue
                elif acao == 'renovar_token':
                    cabecalhos = self._obter_cabecalhos()
                    continue
                elif acao == 'parar':
                    break
                
                itens = self._extrair_dados_resposta(resposta)
                
                if not itens:
                    logger.info(f"Extração finalizada em {endpoint}. Total: {len(dados_coletados)} registros")
                    break
                
                dados_coletados.extend(itens)
                logger.debug(f"Página {pagina} de {endpoint}: {len(itens)} itens coletados")
                
                pagina += 1
                time.sleep(self.DELAY_ENTRE_REQUISICOES)
                
            except requests.RequestException as e:
                logger.error(f"Erro de rede em {endpoint}: {e}")
                break
            except Exception as e:
                logger.error(f"Erro crítico em {endpoint}: {e}")
                break
        
        return dados_coletados
    
    def _salvar(self, dados: List[Dict], pasta: str) -> None:
        """Persiste dados no GCS com particionamento por data."""
        if not dados:
            logger.info(f"Nenhum dado para salvar em {pasta}")
            return
        
        caminho = (
            f"{Config.CAMINHO_BASE_RAW}/{pasta}/"
            f"data_ref={self.data_alvo}/data.json"
        )
        
        self.gcs.salvar_json(caminho, dados)
        logger.info(f"Salvos {len(dados)} registros em {caminho}")
    
    def extrair_vendas(self) -> int:
        """
        Extrai pedidos de venda do dia alvo.
        Usa filtro por data inicial/final conforme API Bling v3.
        """
        endpoint = "pedidos/vendas"
        parametros = {
            "dataInicial": self.data_alvo,
            "dataFinal": self.data_alvo
        }
        
        dados = self._buscar_todas_paginas(endpoint, parametros)
        self._salvar(dados, "pedidos_vendas")
        
        return len(dados)
    
    def extrair_nfe(self) -> int:
        endpoint = "nfe"
        parametros = {
            "dataInicial": self.data_alvo,
            "dataFinal": self.data_alvo
        }
        
        dados = self._buscar_todas_paginas(endpoint, parametros)  # 1. Busca
        self._salvar(dados, "nfe")  # 2. AQUI! Salva
        
        return len(dados)  # 3. Retorna
    
    def executar_pipeline_diario(self) -> int:
        """Executa pipeline completo de extração diária."""
        logger.info(f"=== Pipeline Iniciado: {self.data_alvo} ===")
        
        total_vendas = 0
        total_nfe = 0
        
        # Extração de Vendas com proteção
        try:
            logger.info("[INICIO] Extraindo vendas...")
            total_vendas = self.extrair_vendas()
            logger.info(f"[SUCESSO] Vendas extraídas: {total_vendas}")
        except Exception as e:
            logger.error(f"[ERRO] Falha ao extrair vendas: {e}")
        
        # Extração de NFe com proteção
        try:
            logger.info("[INICIO] Extraindo NFe...")
            total_nfe = self.extrair_nfe()
            logger.info(f"[SUCESSO] NFe extraídas: {total_nfe}")
        except Exception as e:
            logger.error(f"[ERRO] Falha ao extrair NFe: {e}")
        
        # total_produtos = self.extrair_produtos()
        # logger.info(f"Produtos extraídos: {total_produtos}")
        
        total_processado = total_vendas + total_nfe
        logger.info(f"=== Pipeline Finalizado: {total_processado} registros totais ===")
        
        return total_processado