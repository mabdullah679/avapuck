"""Valid stage events for every stage, shared by the verdict and contract tests.

These are the SHAPE the DAG actually returns; a change to a stage's result
dict must be reflected here, and the contract test will say so.
"""
from __future__ import annotations

TRACE = "trips-20260906T0400-1a2b3c"


def good_events() -> dict[str, dict]:
    return {
        "extract": {"trace_id": TRACE, "stage": "extract", "rows": 100,
                    "mode": "BigQueryExtractor", "bq_attempts": 1,
                    "fell_back_to_csv": False, "window_date": "2025-03-02T00:00:00",
                    "window_end": "2025-03-03T00:00:00", "window_source": "archive_mapping",
                    "bytes_billed": 24_000_000, "bq_job_id": "job_1", "csv": "/data/csv/x.csv"},
        "card_split": {"trace_id": TRACE, "stage": "card_split", "rows": 100, "cards": 0,
                       "columns": [], "dataset": "trips", "output": None},
        "encrypt": {"trace_id": TRACE, "stage": "encrypt", "rows": 100, "engine": "spark",
                    "spark_configured": True, "parquet": "/data/parquet/dt=2026-09-06/trips.parquet",
                    "algorithm": "AES-256-GCM", "identity": "spark-job", "dataset": "trips",
                    "encrypted_columns": 6},
        "publish": {"trace_id": TRACE, "stage": "publish", "rows": 100,
                    "topic_encrypted": "rpos_encrypted", "topic_public": "rpos_flat",
                    "encrypted_fields": 8, "public_fields": 3, "dataset": "trips"},
        "mask": {"trace_id": TRACE, "stage": "mask", "rows": 100,
                 "masked_by": "local-policy-engine", "policies": 6,
                 "hive": "/data/hive/trips/dt=2026-09-06/trips_masked.parquet",
                 "identity": "hive-job", "dataset": "trips",
                 "policy_file": "config/ranger/hive_masking_policies.json"},
        "hive_register": {"trace_id": TRACE, "stage": "hive_register", "registered": True,
                          "table": "trips_warehouse.trips", "partition": "dt=2026-09-06",
                          "reason": None},
        "load": {"trace_id": TRACE, "stage": "load", "inserted": 100, "updated": 0,
                 "run_id": "7a0d2a2e-1", "dataset": "trips"},
        "report": {"trace_id": TRACE, "stage": "report", "rows": 100,
                   "pdf": "/data/reports/trips_report_20260906T0400Z.pdf", "dataset": "trips",
                   "collected": "/opt/pipeline/pipeline-reports/trips_report_20260906T0400Z.pdf"},
    }
