import requests
import time
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional
from app.config import Config
from app.gcs_handler import logger
import json


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
    
    def _buscar_todas_paginas(self, endpoint: str, parametros: Dict[str, Any], delay_segundos: float = 0.5) -> List[Dict]:
        """
        MÉTODO MOTOR ATUALIZADO:
        Agora aceita 'delay_segundos' para controlar a velocidade.
        """
        pagina = 1
        dados_coletados = []
        url = f"{self.URL_BASE}/{endpoint}"
        
        # Headers iniciais
        cabecalhos = self._obter_cabecalhos()

        logger.info(f"Iniciando extração: {endpoint} | Delay: {delay_segundos}s")

        while True:
            parametros['pagina'] = pagina
            parametros['limite'] = 100 

            try:
                # Adicionei timeout=30 para não travar se a rede cair
                resposta = requests.get(url, headers=cabecalhos, params=parametros, timeout=30)

                # --- MUDANÇA CRÍTICA AQUI (TRATAMENTO DE 429) ---
                if resposta.status_code == 429:
                    logger.warning("Rate limit (429) atingido. O Bling pediu para parar.")
                    logger.warning("Aguardando 15 segundos para esfriar...")
                    time.sleep(15) # Aumentado de 3 para 15 segundos
                    continue
                
                # Tratamento de Token (401)
                if resposta.status_code == 401:
                    logger.warning("Token expirou. Renovando...")
                    cabecalhos = self._obter_cabecalhos()
                    continue

                if resposta.status_code != 200:
                    logger.error(f"Erro API {endpoint}: {resposta.status_code} - {resposta.text}")
                    break

                payload = resposta.json()
                itens = payload.get('data', [])

                if not itens:
                    break

                dados_coletados.extend(itens)
                logger.info(f"Página {pagina} baixada: {len(itens)} itens.")
                
                pagina += 1
                
                # --- MUDANÇA AQUI (USA O DELAY CUSTOMIZADO) ---
                time.sleep(delay_segundos) 

            except Exception as e:
                logger.error(f"Erro crítico: {e}")
                # Em caso de erro de conexão, espera um pouco antes de quebrar ou tentar de novo
                time.sleep(5)
                break
        
        return dados_coletados
    
    def _salvar(self, dados: List[Dict], pasta: str) -> None:
        """Persiste dados no GCS com particionamento por data em formato NDJSON."""
        if not dados:
            logger.info(f"Nenhum dado para salvar em {pasta}")
            return
        
        # Validação básica
        if not all(isinstance(d, dict) for d in dados):
            raise ValueError("Todos os itens devem ser dicionários")
        
        caminho = (
            f"{Config.CAMINHO_BASE_RAW}/{pasta}/"
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
                f"✓ Salvos {len(dados)} registros em gs://{self.bucket.name}/{caminho}"
            )
            
        except Exception as e:
            logger.error(f"Erro ao salvar no GCS ({caminho}): {e}")
            raise
    
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
        # AJUSTE 1: NFe precisa de Data + Hora
        data_inicio_completa = f"{self.data_alvo} 00:00:00"
        data_fim_completa = f"{self.data_alvo} 23:59:59"

        # AJUSTE 2: Nomes de parâmetros específicos para NFe
        parametros = {
            "dataEmissaoInicial": data_inicio_completa,
            "dataEmissaoFinal": data_fim_completa,
            "tipo": 1  # Opcional: 1=Saída (Vendas)
        }
        
        # Mantendo o delay maior para não dar Rate Limit
        dados = self._buscar_todas_paginas(endpoint, parametros, delay_segundos=1.5)
        
        self._salvar(dados, "nfe")
        return len(dados)
    
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