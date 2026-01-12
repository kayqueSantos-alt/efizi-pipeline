import os
from flask import Flask, jsonify
from app.config import Config
from app.gcs_handler import GCSHandler
from app.auth import BlingAuth
from app.extract import ExtratorBling

app = Flask(__name__)

@app.route("/run", methods=["POST"])
def run_job():
    try:
        gcs = GCSHandler(Config.BUCKET_NAME)
        auth = BlingAuth(gcs)
        extractor = ExtratorBling(auth, gcs)
        
        count = extractor.executar_pipeline_diario()
        
        return jsonify({"status": "success", "processed": count}), 200
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))