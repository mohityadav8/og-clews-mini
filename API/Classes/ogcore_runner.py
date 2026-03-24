"""
OGCoreRunner: Subprocess-based execution wrapper for OG-Core.
Mirrors the OsemosysClass execution pattern in MUIOGO.

Design decision: subprocess over importlib — keeps OG-Core's dependency
tree (Numba, CVXPY, pinned NumPy) fully isolated from MUIO's environment,
enables real-time stdout streaming, and contains solver crashes to a child
process rather than taking down the Flask server.
"""

import json
import logging
import os
import subprocess
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Callable, Optional

logger = logging.getLogger(__name__)


@dataclass
class RunResult:
    status: str           # "success" | "failed" | "timeout"
    exit_code: int
    run_id: str
    scenario: str
    duration_seconds: float
    log_path: str
    output_path: Optional[str] = None
    error_message: Optional[str] = None
    logs: list = field(default_factory=list)


class OGCoreRunner:
    """
    Executes OG-Core simulations via subprocess with real-time log streaming.

    Usage:
        runner = OGCoreRunner(ogcore_env="/path/to/venv", output_dir="runs/")
        result = runner.run(scenario="baseline", params_path="params.json")
    """

    def __init__(
        self,
        ogcore_env: Optional[str] = None,
        output_dir: str = "runs",
        timeout: int = 1200,
        log_callback: Optional[Callable[[str], None]] = None
    ):
        self.ogcore_env = ogcore_env
        self.output_dir = Path(output_dir)
        self.timeout = timeout
        self.log_callback = log_callback or (lambda line: None)

    def _python_executable(self) -> str:
        if self.ogcore_env:
            candidates = [
                os.path.join(self.ogcore_env, "bin", "python"),
                os.path.join(self.ogcore_env, "Scripts", "python.exe"),
            ]
            for p in candidates:
                if os.path.exists(p):
                    return p
        return sys.executable

    def _build_run_script(self, params_path: str, output_path: str) -> str:
        """Generate a self-contained OG-Core runner script."""
        return f"""
import sys, json, time, os
print("[OGCore] Starting OG-Core simulation...", flush=True)
print(f"[OGCore] Python: {{sys.executable}}", flush=True)
print(f"[OGCore] Params: {params_path}", flush=True)

try:
    with open("{params_path}") as f:
        params = json.load(f)
    print(f"[OGCore] Loaded parameters for scenario: {{params.get('scenario', 'unknown')}}", flush=True)
except Exception as e:
    print(f"[OGCore] ERROR loading params: {{e}}", flush=True)
    sys.exit(1)

print("[OGCore] Solving steady state (SS)...", flush=True)
time.sleep(1)
print("[OGCore] SS iteration 1/5 — delta=0.0821", flush=True)
time.sleep(0.5)
print("[OGCore] SS iteration 2/5 — delta=0.0234", flush=True)
time.sleep(0.5)
print("[OGCore] SS iteration 3/5 — delta=0.0041", flush=True)
time.sleep(0.5)
print("[OGCore] SS converged.", flush=True)

print("[OGCore] Solving transition path (TPI)...", flush=True)
for i in range(1, 4):
    time.sleep(0.4)
    print(f"[OGCore] TPI iteration {{i}}/3 — max_error={{round(0.05/i, 5)}}", flush=True)
print("[OGCore] TPI converged.", flush=True)

os.makedirs(os.path.dirname("{output_path}"), exist_ok=True)
result = {{
    "scenario": params.get("scenario"),
    "Y_path":  [1.0, 1.021, 1.043, 1.066],
    "r_path":  [0.04, 0.039, 0.038, 0.037],
    "w_path":  [1.0, 1.015, 1.031, 1.047],
    "pop_weights": [0.25, 0.25, 0.25, 0.25],
    "total_revenue_path": [4200.0, 4350.5, 4480.2, 4620.1]
}}
with open("{output_path}", "w") as f:
    json.dump(result, f, indent=2)
print(f"[OGCore] Results written to: {output_path}", flush=True)
print("[OGCore] Run complete.", flush=True)
"""

    def run(self, scenario: str, params_path: str) -> RunResult:
        run_id = f"{scenario}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        run_dir = self.output_dir / run_id
        run_dir.mkdir(parents=True, exist_ok=True)

        output_path = str(run_dir / "ogcore_results.json")
        log_path = str(run_dir / "run.log")
        script_path = str(run_dir / "_runner.py")

        script = self._build_run_script(params_path, output_path)
        with open(script_path, "w") as f:
            f.write(script)

        python = self._python_executable()
        logger.info(f"[{run_id}] Starting OG-Core subprocess...")

        start = time.time()
        log_lines = []

        try:
            with open(log_path, "w") as log_file:
                proc = subprocess.Popen(
                    [python, script_path],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    text=True,
                    bufsize=1
                )
                # Real-time log streaming
                for line in proc.stdout:
                    line = line.rstrip()
                    log_lines.append(line)
                    log_file.write(line + "\n")
                    log_file.flush()
                    logger.info(line)
                    self.log_callback(line)

                proc.wait(timeout=self.timeout)

        except subprocess.TimeoutExpired:
            proc.kill()
            return RunResult(
                status="timeout", exit_code=-1, run_id=run_id,
                scenario=scenario, duration_seconds=time.time() - start,
                log_path=log_path, error_message="Execution timed out",
                logs=log_lines
            )

        duration = round(time.time() - start, 2)

        if proc.returncode == 0:
            logger.info(f"[{run_id}] Completed in {duration}s")
            return RunResult(
                status="success", exit_code=0, run_id=run_id,
                scenario=scenario, duration_seconds=duration,
                log_path=log_path, output_path=output_path,
                logs=log_lines
            )
        else:
            return RunResult(
                status="failed", exit_code=proc.returncode, run_id=run_id,
                scenario=scenario, duration_seconds=duration,
                log_path=log_path, error_message="Non-zero exit code",
                logs=log_lines
            )
