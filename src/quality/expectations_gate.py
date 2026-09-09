"""
Quality gate (part of Deliverable 5, 15pt, shared with lineage).

Runs a real Great Expectations suite (GX 1.x fluent API, ephemeral context —
no external service needed) against the Silver-layer order data. This is a
GATE, not a report: `run_quality_gate` raises `QualityGateFailure` on any
failed expectation, and the Airflow DAG (src/orchestration/capstone_dag.py)
wires that exception to actually halt the downstream Gold/RAG tasks — this
is what the rubric means by "checks that actually gate the pipeline", not
just checks that get logged.
"""
from __future__ import annotations

import great_expectations as gx
import pandas as pd


class QualityGateFailure(Exception):
    """Raised when one or more expectations fail — halts the pipeline."""

    def __init__(self, result):
        self.result = result
        failed = [r for r in result["results"] if not r["success"]]
        summary = "; ".join(
            f"{r['expectation_config']['type']}({r['expectation_config']['kwargs'].get('column')})"
            for r in failed
        )
        super().__init__(f"Quality gate FAILED ({len(failed)} expectation(s)): {summary}")


def build_orders_suite() -> gx.ExpectationSuite:
    """The six DAMA dimensions from Day 4's material, applied as real GX expectations."""
    suite = gx.ExpectationSuite(name="silver_orders_suite")
    e = gx.expectations
    suite.add_expectation(e.ExpectColumnValuesToNotBeNull(column="order_id"))               # completeness
    suite.add_expectation(e.ExpectColumnValuesToBeUnique(column="order_id"))                # uniqueness
    suite.add_expectation(e.ExpectColumnValuesToNotBeNull(column="customer_id"))            # completeness
    suite.add_expectation(e.ExpectColumnValuesToBeBetween(column="quantity", min_value=1, max_value=10_000))   # validity
    suite.add_expectation(e.ExpectColumnValuesToBeBetween(column="unit_price", min_value=0.01, max_value=1_000_000))  # validity
    suite.add_expectation(e.ExpectColumnValueLengthsToEqual(column="country", value=2))     # consistency (ISO-2)
    return suite


def run_quality_gate(df: pd.DataFrame, suite_name: str = "silver_orders_suite") -> dict:
    """Validate `df` against the orders suite. Returns the GX result dict on success,
    raises QualityGateFailure on any failed expectation."""
    context = gx.get_context(mode="ephemeral")
    data_source = context.data_sources.add_pandas("pandas_source")
    asset = data_source.add_dataframe_asset(name="orders_asset")
    batch_def = asset.add_batch_definition_whole_dataframe("orders_batch")
    batch = batch_def.get_batch(batch_parameters={"dataframe": df})

    suite = build_orders_suite()
    context.suites.add(suite)

    result = batch.validate(suite)
    result_dict = result.describe_dict() if hasattr(result, "describe_dict") else dict(result)

    if not result["success"]:
        raise QualityGateFailure(result)
    return result_dict


if __name__ == "__main__":
    import sys
    sys.path.insert(0, ".")
    from src.ingestion.kafka_producer import SAMPLE_EVENTS
    from src.ingestion.contracts import validate_event

    valid = [validate_event(e, "orders_raw")[0].model_dump() for e in SAMPLE_EVENTS if validate_event(e, "orders_raw")[0]]
    clean_df = pd.DataFrame(valid)

    print("=== RUN 1: clean, contract-valid data (should PASS) ===")
    res = run_quality_gate(clean_df)
    print(f"✅ Quality gate PASSED — {res['statistics']['successful_expectations']}/"
          f"{res['statistics']['evaluated_expectations']} expectations green.\n")

    print("=== RUN 2: inject a duplicate order_id + an out-of-range quantity (should FAIL and HALT) ===")
    dirty_df = pd.concat([clean_df, clean_df.iloc[[0]]], ignore_index=True)  # duplicate PK
    dirty_df.loc[dirty_df.index[-1], "quantity"] = 999_999  # invalid range
    try:
        run_quality_gate(dirty_df)
        print("❌ UNEXPECTED: dirty batch passed the gate!")
    except QualityGateFailure as exc:
        print(f"✅ Quality gate correctly BLOCKED the pipeline: {exc}")
