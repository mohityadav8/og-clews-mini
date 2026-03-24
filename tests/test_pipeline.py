"""
Test suite for OG-CLEWS mini pipeline.
Tests ETL transformation, schema validation, and OGCoreRunner execution.
"""

import json
import os
import tempfile
from pathlib import Path

import pandas as pd
import pytest

from API.Classes.etl_pipeline import CLEWSToOGCoreETL
from API.Classes.ogcore_runner import OGCoreRunner

SAMPLE_CSV = Path(__file__).parent / "exchange" / "data" / "clews_output.csv"


# ── ETL Tests ────────────────────────────────────────────────────────────────

class TestCLEWSToOGCoreETL:

    def setup_method(self):
        self.etl = CLEWSToOGCoreETL(scenario="test")

    def test_load_valid_csv(self):
        df = self.etl.load(str(SAMPLE_CSV))
        assert isinstance(df, pd.DataFrame)
        assert {"variable", "year", "value", "unit"}.issubset(df.columns)
        assert len(df) > 0

    def test_load_missing_columns_raises(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write("variable,year\nProductionByTechnology,2025\n")
            tmp = f.name
        with pytest.raises(ValueError, match="Missing columns"):
            self.etl.load(tmp)
        os.unlink(tmp)

    def test_transform_produces_all_parameters(self):
        df = self.etl.load(str(SAMPLE_CSV))
        result = self.etl.transform(df)
        assert "parameters" in result
        for key in ["p_m", "tau_c", "alpha_T", "delta", "g_y"]:
            assert key in result["parameters"], f"Missing parameter: {key}"

    def test_transform_values_are_non_negative(self):
        df = self.etl.load(str(SAMPLE_CSV))
        result = self.etl.transform(df)
        for key in ["p_m", "tau_c", "alpha_T", "delta"]:
            for entry in result["parameters"][key]:
                assert entry["value"] >= 0, f"{key} has negative value"

    def test_delta_values_capped_at_one(self):
        df = self.etl.load(str(SAMPLE_CSV))
        result = self.etl.transform(df)
        for entry in result["parameters"]["delta"]:
            assert entry["value"] <= 1.0

    def test_schema_validation_passes(self):
        df = self.etl.load(str(SAMPLE_CSV))
        result = self.etl.transform(df)
        assert self.etl.validate(result) is True

    def test_schema_validation_fails_on_bad_data(self):
        bad_data = {"version": "1.0", "scenario": "test", "parameters": {}}
        with pytest.raises(ValueError, match="missing parameters"):
            self.etl.validate(bad_data)

    def test_full_run_saves_file(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            out_path = os.path.join(tmpdir, "output.json")
            result = self.etl.run(str(SAMPLE_CSV), out_path)
            assert os.path.exists(out_path)
            with open(out_path) as f:
                saved = json.load(f)
            assert saved["scenario"] == "test"
            assert "parameters" in saved


# ── OGCoreRunner Tests ────────────────────────────────────────────────────────

class TestOGCoreRunner:

    def test_run_returns_success(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            # Build a params file
            params = {"version": "1.0", "scenario": "test", "parameters": {}}
            params_path = os.path.join(tmpdir, "params.json")
            with open(params_path, "w") as f:
                json.dump(params, f)

            runner = OGCoreRunner(output_dir=os.path.join(tmpdir, "runs"))
            result = runner.run(scenario="test", params_path=params_path)

            assert result.status == "success"
            assert result.exit_code == 0
            assert result.output_path is not None
            assert os.path.exists(result.output_path)

    def test_run_streams_logs(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            params = {"version": "1.0", "scenario": "test", "parameters": {}}
            params_path = os.path.join(tmpdir, "params.json")
            with open(params_path, "w") as f:
                json.dump(params, f)

            collected = []
            runner = OGCoreRunner(
                output_dir=os.path.join(tmpdir, "runs"),
                log_callback=lambda line: collected.append(line)
            )
            result = runner.run(scenario="test", params_path=params_path)
            assert len(collected) > 0
            assert any("OGCore" in line for line in collected)

    def test_run_output_has_required_keys(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            params = {"version": "1.0", "scenario": "test", "parameters": {}}
            params_path = os.path.join(tmpdir, "params.json")
            with open(params_path, "w") as f:
                json.dump(params, f)

            runner = OGCoreRunner(output_dir=os.path.join(tmpdir, "runs"))
            result = runner.run(scenario="test", params_path=params_path)

            with open(result.output_path) as f:
                output = json.load(f)

            for key in ["Y_path", "r_path", "w_path", "pop_weights", "total_revenue_path"]:
                assert key in output, f"Missing output key: {key}"
