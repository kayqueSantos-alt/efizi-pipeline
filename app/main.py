import os
import sys
import traceback
import logging
from typing import Tuple
from flask import Flask, jsonify, Response

from app.config import Config
from app import bling, anymarket
app = Flask(__name__)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    stream=sys.stderr
)
logger = logging.getLogger(__name__)


PIPELINES_DISPONIVEIS = {
    "bling": bling.executar_pipeline,
    "anymarket": anymarket.executar_pipeline
}


def obter_pipelines_configurados() -> list[str]:
    """
    Lê pipelines a serem executados a partir da variável de ambiente.
    Exemplo: PIPELINES=bling,anymarket
    """
    valor = os.getenv("PIPELINES", "bling")
    return [p.strip().lower() for p in valor.split(",") if p.strip()]


@app.route("/", methods=["GET"])
def healthcheck() -> Tuple[Response, int]:
    return jsonify({"status": "ok"}), 200


@app.route("/run", methods=["POST"])
def run_job() -> Tuple[Response, int]:
    """
    Endpoint acionado pelo Cloud Scheduler.
    Executa todos os pipelines configurados.
    """
    try:
        pipelines = obter_pipelines_configurados()

        total_processado = {}
        logger.info(f"Pipelines configurados para execução: {pipelines}")

        for nome in pipelines:
            if nome not in PIPELINES_DISPONIVEIS:
                logger.warning(f"Pipeline ignorado (não suportado): {nome}")
                continue

            logger.info(f"Iniciando pipeline: {nome}")
            count = PIPELINES_DISPONIVEIS[nome](Config.BUCKET_NAME)
            total_processado[nome] = count
            logger.info(f"Pipeline {nome} finalizado: {count}")

        return jsonify({
            "status": "success",
            "pipelines": total_processado
        }), 200

    except Exception as erro:
        logger.error(str(erro))
        traceback.print_exc(file=sys.stderr)

        return jsonify({
            "status": "error",
            "message": str(erro)
        }), 500


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port, debug=False)
