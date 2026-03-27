"""
Test suite for OG-CLEWS mini pipeline.
All OG-Core parameter names verified against ogcore/default_parameters.json.
"""

import json
import os
import tempfile
from pathlib import Path

import pandas as pd
import pytest

from API.Classes.etl_pipeline import CLEWSToOGCoreETL
from API.Classes.ogcore_runner import OGCoreRunner

SAMPLE_CSV = Path(__file__).parent.parent / "exchange" / "data" / "clews_output.csv"


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

    def test_transform_produces_correct_ogcore_params(self):
        """Param names must match real OG-Core names from default_parameters.json."""
        df = self.etl.load(str(SAMPLE_CSV))
        result = self.etl.transform(df)
        # These are the REAL OG-Core parameter names
        for key in ["Z", "tau_c", "alpha_T", "inv_tax_credit", "g_y_annual"]:
            assert key in result["parameters"], f"Missing OG-Core parameter: {key}"
        # These OLD WRONG names must NOT appear
        for bad in ["p_m", "delta", "g_y"]:
            assert bad not in result["parameters"], f"Found incorrect param: {bad}"

    def test_Z_normalized_to_base_year(self):
        """Z should be 1.0 in base year (TFP index)."""
        df = self.etl.load(str(SAMPLE_CSV))
        result = self.etl.transform(df)
        assert result["parameters"]["Z"][0]["value"] == 1.0

    def test_tau_c_has_energy_good_index(self):
        """tau_c is 2D in OG-Core (T+S x I) — each entry needs energy_good_index."""
        df = self.etl.load(str(SAMPLE_CSV))
        result = self.etl.transform(df)
        for entry in result["parameters"]["tau_c"]:
            assert "energy_good_index" in entry

    def test_alpha_T_non_negative(self):
        df = self.etl.load(str(SAMPLE_CSV))
        result = self.etl.transform(df)
        for entry in result["parameters"]["alpha_T"]:
            assert entry["value"] >= 0

    def test_inv_tax_credit_between_0_and_1(self):
        df = self.etl.load(str(SAMPLE_CSV))
        result = self.etl.transform(df)
        for entry in result["parameters"]["inv_tax_credit"]:
            assert 0 <= entry["value"] <= 1.0

    def test_g_y_annual_within_ogcore_validator_bounds(self):
        """OG-Core validates g_y_annual in range [-0.01, 0.08]."""
        df = self.etl.load(str(SAMPLE_CSV))
        result = self.etl.transform(df)
        for entry in result["parameters"]["g_y_annual"]:
            assert -0.01 <= entry["value"] <= 0.08

    def test_schema_validation_passes(self):
        df = self.etl.load(str(SAMPLE_CSV))
        result = self.etl.transform(df)
        assert self.etl.validate(result) is True

    def test_schema_validation_fails_on_bad_data(self):
        bad = {"version": "1.1", "scenario": "test", "parameters": {}}
        with pytest.raises(ValueError, match="missing parameters"):
            self.etl.validate(bad)

    def test_old_wrong_param_names_fail_validation(self):
        """Regression guard — old param names p_m/delta/g_y must be rejected."""
        old = {
            "version": "1.0", "scenario": "test",
            "parameters": {"p_m": [], "tau_c": [], "alpha_T": [], "delta": [], "g_y": []}
        }
        with pytest.raises(ValueError, match="missing parameters"):
            self.etl.validate(old)

    def test_full_run_saves_file_with_correct_params(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            out = os.path.join(tmpdir, "output.json")
            result = self.etl.run(str(SAMPLE_CSV), out)
            assert os.path.exists(out)
            with open(out) as f:
                saved = json.load(f)
            assert saved["version"] == "1.1"
            for key in ["Z", "tau_c", "alpha_T", "inv_tax_credit", "g_y_annual"]:
                assert key in saved["parameters"]


class TestOGCoreRunner:

    def test_run_returns_success(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            params = {"version": "1.1", "scenario": "test", "parameters": {}}
            p = os.path.join(tmpdir, "params.json")
            with open(p, "w") as f:
                json.dump(params, f)
            runner = OGCoreRunner(output_dir=os.path.join(tmpdir, "runs"))
            result = runner.run(scenario="test", params_path=p)
            assert result.status == "success"
            assert result.exit_code == 0
            assert os.path.exists(result.output_path)

    def test_run_streams_logs(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            params = {"version": "1.1", "scenario": "test", "parameters": {}}
            p = os.path.join(tmpdir, "params.json")
            with open(p, "w") as f:
                json.dump(params, f)
            collected = []
            runner = OGCoreRunner(
                output_dir=os.path.join(tmpdir, "runs"),
                log_callback=lambda line: collected.append(line)
            )
            runner.run(scenario="test", params_path=p)
            assert len(collected) > 0
            assert any("OGCore" in line for line in collected)

    def test_run_output_has_required_keys(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            params = {"version": "1.1", "scenario": "test", "parameters": {}}
            p = os.path.join(tmpdir, "params.json")
            with open(p, "w") as f:
                json.dump(params, f)
            runner = OGCoreRunner(output_dir=os.path.join(tmpdir, "runs"))
            result = runner.run(scenario="test", params_path=p)
            with open(result.output_path) as f:
                output = json.load(f)
            for key in ["Y_path", "r_path", "w_path", "pop_weights", "total_revenue_path"]:
                assert key in output