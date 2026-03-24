# OG-CLEWS Mini Integration Pipeline

A working prototype of the backend integration described in my GSoC 2026 proposal:  
**OG–CLEWS: Integrating Open-Source Economic and Environmental Models for Sustainable Development**

This project demonstrates the three core backend components I plan to build in MUIOGO:
1. **ETL Pipeline** — transforms CLEWS/OSeMOSYS outputs into validated OG-Core input parameters
2. **OGCoreRunner** — subprocess-based execution wrapper with real-time log streaming
3. **Flask REST API** — endpoints to trigger ETL, run scenarios, and retrieve results

---

## Project Structure

```
og-clews-mini/
├── API/
│   └── Classes/
│       ├── etl_pipeline.py      # CLEWS → OG-Core ETL transformer
│       └── ogcore_runner.py     # Subprocess execution wrapper
├── exchange/
│   ├── schemas/
│   │   └── ogcore_input_schema.json   # JSON schema for exchange validation
│   └── data/
│       └── clews_output.csv           # Sample CLEWS output data
├── tests/
│   └── test_pipeline.py         # Pytest test suite
├── app.py                        # Flask REST API
├── requirements.txt
└── README.md
```

---

## What It Does

### ETL Pipeline (`etl_pipeline.py`)
Implements the **CLEWS → OG-Core variable mapping** from the proposal:

| CLEWS Output Variable | Transformation | OG-Core Parameter |
|---|---|---|
| `TotalAnnualTechnologyActivityByMode` | Normalize to GDP share | `p_m` (import energy price index) |
| `AnnualEmissions` | Apply carbon price → % of GDP | `tau_c` (carbon tax rate) |
| `TotalDiscountedCost` | Annualize → % of GDP | `alpha_T` (govt transfer rate) |
| `ProductionByTechnology` | Compute investment share | `delta` (capital formation rate) |
| `TotalCapacityAnnual` | Year-on-year growth rate | `g_y` (productivity growth rate) |

Each run validates the output against a JSON schema before saving.

### OGCoreRunner (`ogcore_runner.py`)
Implements the **subprocess-over-importlib** design decision from the proposal:
- Executes OG-Core in an isolated child process
- Streams `stdout` line-by-line in real time via `subprocess.Popen(..., stdout=PIPE)`
- Captures exit code, duration, and structured logs
- Supports optional `log_callback` for live frontend streaming
- Contains solver crashes to the child process — Flask server stays alive

### Flask API (`app.py`)
REST endpoints mirroring the MUIOGO API structure:

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/api/health` | Health check |
| `POST` | `/api/etl/clews-to-ogcore` | Run ETL pipeline |
| `POST` | `/api/ogcore/run` | Execute OG-Core scenario |
| `POST` | `/api/ogcore/run/stream` | Run with SSE log streaming |
| `GET` | `/api/ogcore/results/<scenario>` | Retrieve results |

---

## Setup & Run

```bash
# Install dependencies
pip install -r requirements.txt

# Run the Flask API
python app.py
```

### Example API calls

```bash
# 1. Run ETL pipeline
curl -X POST http://localhost:5050/api/etl/clews-to-ogcore \
  -H "Content-Type: application/json" \
  -d '{"scenario": "baseline"}'

# 2. Run OG-Core simulation
curl -X POST http://localhost:5050/api/ogcore/run \
  -H "Content-Type: application/json" \
  -d '{"scenario": "baseline"}'

# 3. Get results
curl http://localhost:5050/api/ogcore/results/baseline
```

---

## Tests

```bash
pytest tests/ -v
```

Test coverage includes:
- CSV loading and column validation
- All 5 parameter transformations
- Schema validation (pass and fail cases)
- Delta values capped at 1.0
- Full ETL run file output
- OGCoreRunner subprocess execution
- Real-time log streaming
- Output key validation (`Y_path`, `r_path`, `w_path`, `pop_weights`, `total_revenue_path`)

---

## Relation to GSoC Proposal

This prototype directly demonstrates the patterns described in my proposal:

- **Subprocess design**: `OGCoreRunner` uses `subprocess.Popen` with `stdout=PIPE` for process isolation and live log streaming — exactly as justified in the proposal's "subprocess vs importlib" section.
- **ETL variable mapping**: The 5 forward-pipeline variables (`p_m`, `tau_c`, `alpha_T`, `delta`, `g_y`) are implemented with the unit transformations described in the proposal's variable mapping table.
- **Schema validation**: Exchange files are validated before being passed to OG-Core, matching the "schema-validated CSV and JSON files" deliverable.
- **Flask API structure**: Endpoints follow the `API/` Blueprint pattern from MUIOGO.

The full GSoC implementation will extend this with bidirectional pipelines, a `WorkflowOrchestrator` for coupled runs, and a `ConvergenceEngine` for iterative execution.

---

## Author

Mohit Yadav — GSoC 2026 Contributor  
[GitHub](https://github.com/mohityadav8) · [LinkedIn](https://www.linkedin.com/in/mohit-yadav-6b2319305)
