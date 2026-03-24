"""
MUIO-mini Flask API
Exposes endpoints for ETL pipeline execution and OG-Core scenario runs.
Mirrors the MUIOGO API structure.
"""

import json
import logging
import os
from pathlib import Path

from flask import Flask, Response, jsonify, request, stream_with_context

from API.Classes.etl_pipeline import CLEWSToOGCoreETL
from API.Classes.ogcore_runner import OGCoreRunner

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

app = Flask(__name__)

BASE_DIR     = Path(__file__).parent
DATA_DIR     = BASE_DIR / "exchange" / "data"
RUNS_DIR     = BASE_DIR / "runs"
CLEWS_CSV    = str(DATA_DIR / "clews_output.csv")


# ── Health ──────────────────────────────────────────────────────────────────

@app.route("/api/health", methods=["GET"])
def health():
    return jsonify({"status": "ok", "service": "og-clews-mini"})


# ── ETL: CLEWS → OG-Core ────────────────────────────────────────────────────

@app.route("/api/etl/clews-to-ogcore", methods=["POST"])
def run_etl():
    """
    Transform CLEWS output CSV into validated OG-Core input JSON.
    Body (optional): { "scenario": "baseline", "csv_path": "..." }
    """
    body = request.get_json(silent=True) or {}
    scenario  = body.get("scenario", "baseline")
    csv_path  = body.get("csv_path", CLEWS_CSV)
    out_path  = str(DATA_DIR / f"ogcore_input_{scenario}.json")

    try:
        etl = CLEWSToOGCoreETL(scenario=scenario)
        result = etl.run(csv_path=csv_path, output_path=out_path)
        return jsonify({
            "status": "success",
            "scenario": scenario,
            "output_path": out_path,
            "parameters_preview": {
                k: v[:2] for k, v in result["parameters"].items()
            }
        })
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


# ── OG-Core: Run scenario ────────────────────────────────────────────────────

@app.route("/api/ogcore/run", methods=["POST"])
def run_ogcore():
    """
    Execute an OG-Core scenario simulation.
    Body: { "scenario": "baseline" }
    Returns: run result with output path and logs.
    """
    body = request.get_json(silent=True) or {}
    scenario   = body.get("scenario", "baseline")
    params_path = str(DATA_DIR / f"ogcore_input_{scenario}.json")

    if not os.path.exists(params_path):
        return jsonify({
            "status": "error",
            "message": f"No exchange file found for scenario '{scenario}'. Run ETL first."
        }), 400

    runner = OGCoreRunner(output_dir=str(RUNS_DIR))
    result = runner.run(scenario=scenario, params_path=params_path)

    return jsonify({
        "status": result.status,
        "run_id": result.run_id,
        "scenario": result.scenario,
        "duration_seconds": result.duration_seconds,
        "exit_code": result.exit_code,
        "output_path": result.output_path,
        "log_path": result.log_path,
        "error_message": result.error_message,
        "logs": result.logs
    })


# ── OG-Core: Stream logs ─────────────────────────────────────────────────────

@app.route("/api/ogcore/run/stream", methods=["POST"])
def run_ogcore_stream():
    """
    Execute an OG-Core scenario with real-time log streaming via SSE.
    Body: { "scenario": "baseline" }
    """
    body = request.get_json(silent=True) or {}
    scenario    = body.get("scenario", "baseline")
    params_path = str(DATA_DIR / f"ogcore_input_{scenario}.json")

    if not os.path.exists(params_path):
        return jsonify({"status": "error", "message": "Run ETL first."}), 400

    def generate():
        log_lines = []

        def on_log(line):
            log_lines.append(line)
            yield f"data: {json.dumps({'log': line})}\n\n"

        # Can't yield inside callback directly, so collect and stream
        runner = OGCoreRunner(output_dir=str(RUNS_DIR))
        result = runner.run(scenario=scenario, params_path=params_path)
        for line in result.logs:
            yield f"data: {json.dumps({'log': line})}\n\n"
        yield f"data: {json.dumps({'status': result.status, 'run_id': result.run_id})}\n\n"

    return Response(
        stream_with_context(generate()),
        mimetype="text/event-stream"
    )


# ── Results ──────────────────────────────────────────────────────────────────

@app.route("/api/ogcore/results/<scenario>", methods=["GET"])
def get_results(scenario):
    """Retrieve OG-Core results for a completed scenario run."""
    import glob
    pattern = str(RUNS_DIR / f"{scenario}_*" / "ogcore_results.json")
    matches = sorted(glob.glob(pattern), reverse=True)
    if not matches:
        return jsonify({"status": "error", "message": "No results found."}), 404
    with open(matches[0]) as f:
        data = json.load(f)
    return jsonify({"status": "success", "scenario": scenario, "results": data})


if __name__ == "__main__":
    app.run(debug=True, port=5050)
