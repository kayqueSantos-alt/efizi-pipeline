import os
from flask import Flask, jsonify
from app.config import Config
from app.gcs_handler import GCSHandler
from app.auth import BlingAuth
from app.extract import ExtratorBling
import sys
import traceback

app = Flask(__name__)

@app.route("/", methods=["GET"])
def index():
    return jsonify({"message": "Pipeline Bling API Rodando"}), 200

@app.route("/run", methods=["POST"])
def run_job():
    try:
        # Imprime aviso que começou (ajuda a saber se a rota foi chamada)
        print("--- INICIANDO JOB VIA SCHEDULER ---", file=sys.stderr)
        
        gcs = GCSHandler(Config.BUCKET_NAME)
        auth = BlingAuth(gcs)
        extractor = ExtratorBling(auth, gcs)
        
        count = extractor.executar_pipeline_diario()
        
        print(f"--- SUCESSO: {count} processados ---", file=sys.stderr)
        return jsonify({"status": "success", "processed": count}), 200

    except Exception as e:
        # --- AQUI ESTÁ A MÁGICA ---
        # Isso imprime o erro completo (Traceback) nos logs do Cloud Run
        print(f"!!! ERRO FATAL !!!: {str(e)}", file=sys.stderr)
        traceback.print_exc(file=sys.stderr) 
        # --------------------------
        
        return jsonify({"status": "error", "message": str(e)}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))