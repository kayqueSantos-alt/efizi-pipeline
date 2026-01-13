import os
import sys
import traceback
import logging
from typing import Tuple
from flask import Flask, jsonify, Response
from app.config import Config
from app.gcs_handler import GCSHandler
from app.bling.auth import BlingAuth
from app.bling.extract import ExtratorBling


app = Flask(__name__)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    stream=sys.stderr
)
logger = logging.getLogger(__name__)


class PipelineOrchestrator:
    """Orquestra execução do pipeline de extração Bling."""
    
    def __init__(self, bucket_name: str):
        self.bucket_name = bucket_name
        self._gcs_handler = None
        self._auth_service = None
        self._extractor = None
    
    def _inicializar_componentes(self) -> None:
        """Inicializa handlers e serviços necessários."""
        self._gcs_handler = GCSHandler(self.bucket_name)
        self._auth_service = BlingAuth(self._gcs_handler)
        self._extractor = ExtratorBling(self._auth_service, self._gcs_handler)
    
    def executar(self) -> int:
        """
        Executa pipeline completo de extração.
        Retorna quantidade de registros processados.
        """
        logger.info("Iniciando execução do pipeline via scheduler")
        
        self._inicializar_componentes()
        count = self._extractor.executar_pipeline_diario()
        
        logger.info(f"Pipeline finalizado com sucesso: {count} registros processados")
        return count


def criar_resposta_sucesso(count: int) -> Tuple[Response, int]:
    """Cria response JSON de sucesso."""
    return jsonify({
        "status": "success",
        "processed": count
    }), 200


def criar_resposta_erro(erro: Exception) -> Tuple[Response, int]:
    """Cria response JSON de erro com logging completo."""
    logger.error(f"Erro fatal durante execução do pipeline: {str(erro)}")
    logger.error("Traceback completo:")
    traceback.print_exc(file=sys.stderr)
    
    return jsonify({
        "status": "error",
        "message": str(erro)
    }), 500


@app.route("/", methods=["GET"])
def index() -> Tuple[Response, int]:
    """Health check endpoint."""
    return jsonify({
        "message": "Pipeline Bling API Rodando"
    }), 200


@app.route("/run", methods=["POST"])
def run_job() -> Tuple[Response, int]:
    """
    Endpoint de execução do pipeline.
    Acionado por Cloud Scheduler para processar dados diários.
    """
    try:
        orchestrator = PipelineOrchestrator(Config.BUCKET_NAME)
        count = orchestrator.executar()
        return criar_resposta_sucesso(count)
        
    except Exception as erro:
        return criar_resposta_erro(erro)


def obter_porta_servidor() -> int:
    """Obtém porta do servidor a partir de variável de ambiente."""
    porta_padrao = 8080
    return int(os.environ.get("PORT", porta_padrao))


if __name__ == "__main__":
    porta = obter_porta_servidor()
    logger.info(f"Iniciando servidor Flask na porta {porta}")
    
    app.run(
        host="0.0.0.0",
        port=porta,
        debug=False
    )