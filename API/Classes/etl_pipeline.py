"""
ETL Pipeline: CLEWS → OG-Core
Transforms CLEWS output CSV into validated OG-Core input JSON.

Variable mapping verified against:
  - ogcore/default_parameters.json
  - ogcore/parameters.py (Specifications class)
"""

import json
import logging
import os
from pathlib import Path

import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

GDP_REFERENCE     = 45000.0  # $M — reference GDP for normalization
CARBON_PRICE      = 50.0     # $/tCO2 — carbon price assumption
ENERGY_GOOD_INDEX = 0        # index of energy good in OG-Core's I consumption goods


class CLEWSToOGCoreETL:
    """
    Forward ETL pipeline: CLEWS/OSeMOSYS outputs → OG-Core input parameters.

    Variable mapping (verified against ogcore/default_parameters.json):

        CLEWS variable                        OG-Core param       Source definition
        ──────────────────────────────────────────────────────────────────────────────
        TotalAnnualTechnologyActivityByMode → Z                   "Total factor productivity
                                                                   in firm production function"
                                                                   Shape: (T+S, M). M=industries.

        AnnualEmissions (CO2)              → tau_c[t,energy_i]   "Consumption tax rate"
                                                                   Shape: (T+S, I). I=goods.
                                                                   Targets energy good index only.

        TotalDiscountedCost                → alpha_T              "Exogenous ratio of govt
                                                                   transfers to GDP"
                                                                   Shape: (T+S,).

        ProductionByTechnology (renewables)→ inv_tax_credit       "Investment tax credit rate —
                                                                   reduces cost of new investment"
                                                                   Shape: (T+S, M).

        TotalCapacityAnnual                → g_y_annual           "Growth rate of labor augmenting
                                                                   technological change"
                                                                   Scalar float. OG-Core converts
                                                                   internally via rate_conversion().

    Corrections from source code review:
        - p_m does NOT exist in OG-Core. Z is the correct TFP parameter.
        - delta is computed INTERNALLY from delta_annual via rate_conversion().
          It is NOT an external ETL input.
        - tau_c is shape (T+S, I) — we target energy good index only, not a scalar.
        - g_y_annual is a scalar annual rate; OG-Core handles period conversion internally.
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

        # Z: Total factor productivity (from TotalAnnualTechnologyActivityByMode)
        # OG-Core: shape (T+S, M) — "Total factor productivity in firm production function"
        # Normalize to index: base year = 1.0
        activity = rows("TotalAnnualTechnologyActivityByMode").reset_index(drop=True)
        base_activity = activity.loc[0, "value"] if len(activity) > 0 else 1.0
        Z = [
            {"year": int(r.year), "value": round(r.value / base_activity, 6)}
            for _, r in activity.iterrows()
        ]

        # tau_c: Consumption tax on energy good (from AnnualEmissions CO2)
        # OG-Core: shape (T+S, I) — "Consumption tax rate"
        # Apply carbon price → effective tax on energy consumption good at energy_good_index
        emissions = rows("AnnualEmissions")
        tau_c = [
            {
                "year": int(r.year),
                "energy_good_index": ENERGY_GOOD_INDEX,
                "value": round((r.value * CARBON_PRICE) / GDP_REFERENCE, 6)
            }
            for _, r in emissions.iterrows()
        ]

        # alpha_T: Govt transfers as share of GDP (from TotalDiscountedCost)
        # OG-Core: shape (T+S,) — "Exogenous ratio of govt transfers to GDP"
        costs = rows("TotalDiscountedCost")
        alpha_T = [
            {"year": int(r.year), "value": round(r.value / GDP_REFERENCE, 6)}
            for _, r in costs.iterrows()
        ]

        # inv_tax_credit: Investment tax credit (from ProductionByTechnology renewables)
        # OG-Core: shape (T+S, M) — "Investment tax credit rate that reduces cost of new investment"
        # Renewable share of total production → effective investment credit proxy
        production = rows("ProductionByTechnology")
        total_prod = production["value"].sum()
        inv_tax_credit = [
            {"year": int(r.year), "value": round(min(r.value / total_prod, 1.0), 6)}
            for _, r in production.iterrows()
        ]

        # g_y_annual: Labor-augmenting tech growth rate (from TotalCapacityAnnual)
        # OG-Core: scalar float — "Growth rate of labor augmenting technological change"
        # OG-Core converts internally: g_y = rate_conversion(g_y_annual, start_age, end_age, S)
        capacity = rows("TotalCapacityAnnual").reset_index(drop=True)
        g_y_annual = []
        for i, row in capacity.iterrows():
            if i == 0:
                g_y_annual.append({"year": int(row.year), "value": 0.0})
            else:
                prev = capacity.loc[i - 1, "value"]
                growth = round((row.value - prev) / prev, 6) if prev != 0 else 0.0
                g_y_annual.append({"year": int(row.year), "value": growth})

        result = {
            "version": "1.1",
            "scenario": self.scenario,
            "ogcore_param_source": "verified against ogcore/default_parameters.json",
            "parameters": {
                "Z":              Z,
                "tau_c":          tau_c,
                "alpha_T":        alpha_T,
                "inv_tax_credit": inv_tax_credit,
                "g_y_annual":     g_y_annual,
            }
        }

        logger.info("Transformation complete.")
        return result

    def validate(self, data: dict) -> bool:
        logger.info("Validating exchange file against OG-Core parameter schema...")
        required_top = {"version", "scenario", "parameters"}
        missing = required_top - data.keys()
        if missing:
            raise ValueError(f"Schema validation failed: missing fields {missing}")

        # Real OG-Core parameter names verified from default_parameters.json
        required_params = {"Z", "tau_c", "alpha_T", "inv_tax_credit", "g_y_annual"}
        missing_params = required_params - data["parameters"].keys()
        if missing_params:
            raise ValueError(f"Schema validation failed: missing parameters {missing_params}")

        for key, entries in data["parameters"].items():
            if not isinstance(entries, list):
                raise ValueError(f"Parameter '{key}' must be a list")
            for entry in entries:
                if "year" not in entry or "value" not in entry:
                    raise ValueError(f"Parameter '{key}' entry missing 'year' or 'value'")
                if not isinstance(entry["value"], (int, float)):
                    raise ValueError(f"Parameter '{key}' value must be numeric")
                if key == "tau_c" and "energy_good_index" not in entry:
                    raise ValueError("tau_c entries must include 'energy_good_index'")

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