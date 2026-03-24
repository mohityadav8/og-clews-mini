"""
ETL Pipeline: CLEWS → OG-Core
Transforms CLEWS output CSV into validated OG-Core input JSON.
"""

import json
import logging
import os
from pathlib import Path

import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

SCHEMA_PATH = Path(__file__).parent.parent / "exchange" / "schemas" / "ogcore_input_schema.json"
GDP_REFERENCE = 45000.0  # $M — reference GDP for normalization
CARBON_PRICE = 50.0      # $/tCO2 — carbon price assumption


class CLEWSToOGCoreETL:
    """
    Forward ETL pipeline: CLEWS/OSeMOSYS outputs → OG-Core input parameters.

    Variable mapping:
        TotalAnnualTechnologyActivityByMode → p_m  (import energy price index)
        AnnualEmissions (CO2)               → tau_c (carbon tax rate)
        TotalDiscountedCost                 → alpha_T (government transfer rate)
        ProductionByTechnology (renewables) → delta  (capital formation rate)
        TotalCapacityAnnual                 → g_y   (productivity growth rate)
    """

    def __init__(self, scenario: str = "baseline"):
        self.scenario = scenario

    def load(self, csv_path: str) -> pd.DataFrame:
        logger.info(f"Loading CLEWS output from: {csv_path}")
        df = pd.read_csv(csv_path)
        required_cols = {"variable", "year", "value", "unit"}
        if not required_cols.issubset(df.columns):
            raise ValueError(f"Missing columns. Expected: {required_cols}")
        logger.info(f"Loaded {len(df)} rows for variables: {df['variable'].unique().tolist()}")
        return df

    def transform(self, df: pd.DataFrame) -> dict:
        logger.info("Transforming CLEWS variables to OG-Core parameters...")

        def rows(var):
            return df[df["variable"] == var].sort_values("year")

        # p_m: normalize activity by GDP share
        activity = rows("TotalAnnualTechnologyActivityByMode")
        p_m = [{"year": int(r.year), "value": round(r.value / GDP_REFERENCE, 6)}
               for _, r in activity.iterrows()]

        # tau_c: apply carbon price, express as % of GDP
        emissions = rows("AnnualEmissions")
        tau_c = [{"year": int(r.year), "value": round((r.value * CARBON_PRICE) / GDP_REFERENCE, 6)}
                 for _, r in emissions.iterrows()]

        # alpha_T: annualized cost as % of GDP
        costs = rows("TotalDiscountedCost")
        alpha_T = [{"year": int(r.year), "value": round(r.value / GDP_REFERENCE, 6)}
                   for _, r in costs.iterrows()]

        # delta: renewable investment share (capacity growth as proxy)
        production = rows("ProductionByTechnology")
        total_prod = production["value"].sum()
        delta = [{"year": int(r.year), "value": round(min(r.value / total_prod, 1.0), 6)}
                 for _, r in production.iterrows()]

        # g_y: year-on-year capacity growth rate
        capacity = rows("TotalCapacityAnnual").reset_index(drop=True)
        g_y = []
        for i, row in capacity.iterrows():
            if i == 0:
                g_y.append({"year": int(row.year), "value": 0.0})
            else:
                prev = capacity.loc[i - 1, "value"]
                growth = round((row.value - prev) / prev, 6) if prev != 0 else 0.0
                g_y.append({"year": int(row.year), "value": growth})

        result = {
            "version": "1.0",
            "scenario": self.scenario,
            "parameters": {
                "p_m": p_m,
                "tau_c": tau_c,
                "alpha_T": alpha_T,
                "delta": delta,
                "g_y": g_y
            }
        }

        logger.info("Transformation complete.")
        return result

    def validate(self, data: dict) -> bool:
        logger.info("Validating exchange file against JSON schema...")
        required_top = {"version", "scenario", "parameters"}
        missing = required_top - data.keys()
        if missing:
            raise ValueError(f"Schema validation failed: missing fields {missing}")
        required_params = {"p_m", "tau_c", "alpha_T", "delta", "g_y"}
        missing_params = required_params - data["parameters"].keys()
        if missing_params:
            raise ValueError(f"Schema validation failed: missing parameters {missing_params}")
        for key, entries in data["parameters"].items():
            for entry in entries:
                if "year" not in entry or "value" not in entry:
                    raise ValueError(f"Parameter '{key}' entry missing 'year' or 'value'")
                if not isinstance(entry["value"], (int, float)):
                    raise ValueError(f"Parameter '{key}' value must be numeric")
        logger.info("Schema validation passed.")
        return True

    def save(self, data: dict, output_path: str) -> str:
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        with open(output_path, "w") as f:
            json.dump(data, f, indent=2)
        logger.info(f"Exchange file saved to: {output_path}")
        return output_path

    def run(self, csv_path: str, output_path: str) -> dict:
        df = self.load(csv_path)
        data = self.transform(df)
        self.validate(data)
        self.save(data, output_path)
        return data
