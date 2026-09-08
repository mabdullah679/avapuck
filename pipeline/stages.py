"""The pipeline's stages, as one ordered list that every runner iterates.

Two things run this pipeline: the Airflow DAG (dags/trips_pipeline_dag.py)
and the plain runner (pipeline/run_pipeline.py). They used to each name the
stages, and each drifted -- the runner was missing two. Now both walk
`STAGES` and call `run_stage`, so a stage added here appears in both, and a
test asserts the DAG's task list matches (docs/DECISIONS.md D14).

Each stage function takes a RunEnv and the events of the stages before it,
does its work through the pipeline module that owns it, and returns the
STAGE EVENT: the dict the verdict consumes and the contract in
contracts/stage_event.schema.json describes. Airflow stores it in XCom; the
runner keeps it in a dict. Nothing here imports Airflow.
"""
from __future__ import annotations

import logging
import os
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

from pipeline.common.trace import stage_event, trace_id as make_trace_id

log = logging.getLogger(__name__)

# (stage name, Airflow task id). The stage name is what events and the verdict
# use; the task id is what the Airflow UI shows.
STAGES: list[tuple[str, str]] = [
    ("extract", "extract_to_csv"),
    ("card_split", "split_card_numbers"),
    ("encrypt", "encrypt_to_parquet"),
    ("publish", "publish_kafka"),
    ("mask", "decrypt_and_mask"),
    ("hive_register", "register_hive_table"),
    ("load", "load_postgres"),
    ("report", "build_pdf"),
]
STAGE_NAMES = [s for s, _ in STAGES]
TASK_ID_FOR = dict(STAGES)
VERDICT_TASK_ID = "emit_verdict"


@dataclass(frozen=True)
class RunEnv:
    """Everything a stage needs to know about the run it is part of."""
    data_root: Path
    run_slot: datetime          # start of the schedule interval, UTC
    run_id: str                 # Airflow dag_run.run_id, or a caller-supplied id
    try_number: int = 1
    pipeline: str = "trips"
    # Per-run parameters: the DAG run's conf (an upload from the console), or
    # the runner's flags. Keys: csv_path, dataset, row_limit, source.
    conf: dict = field(default_factory=dict)

    @property
    def trace_id(self) -> str:
        return make_trace_id(self.run_slot, self.pipeline)

    @property
    def date(self):
        return self.run_slot.date()

    @property
    def is_upload(self) -> bool:
        return bool(self.conf.get("csv_path"))

    @classmethod
    def build(cls, data_root: Path, run_slot: datetime, run_id: str | None = None,
              try_number: int = 1, conf: dict | None = None) -> "RunEnv":
        if run_slot.tzinfo is None:
            run_slot = run_slot.replace(tzinfo=timezone.utc)
        run_slot = run_slot.astimezone(timezone.utc)
        return cls(data_root=Path(data_root), run_slot=run_slot,
                   run_id=run_id or f"manual__{run_slot.isoformat()}",
                   try_number=try_number, conf=dict(conf or {}))


@contextmanager
def _source_from_conf(conf: dict):
    """Point the extractor at an uploaded file for the duration of one stage.

    The extractors are configured by environment (CSV_SOURCE_PATH wins over
    everything, including EXTRACT_MODE=live). A DAG run's conf carries the
    upload, so it is translated into that environment here and restored
    afterwards -- one place, rather than every extractor learning about conf.
    Uploads are uncapped unless the conf says otherwise (D16).
    """
    if not conf.get("csv_path"):
        yield
        return
    overrides = {
        "CSV_SOURCE_PATH": str(conf["csv_path"]),
        "CSV_ROW_LIMIT": str(int(conf.get("row_limit") or 0)),
    }
    if conf.get("dataset"):
        overrides["DATASET_NAME"] = str(conf["dataset"])
    saved = {k: os.environ.get(k) for k in overrides}
    os.environ.update(overrides)
    try:
        yield
    finally:
        for k, v in saved.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v


# ── Helpers the stages share ──────────────────────────────────────────────

def crypto_client(client_id: str, secret_env: str):
    """A crypto client authenticated as a specific job identity.

    Each stage uses its OWN identity, so the encrypt stage's token cannot be
    replayed to decrypt: spark-job holds crypto.encrypt, hive-job holds
    crypto.decrypt, and the IdP refuses to mint either for the wrong client.
    """
    from pipeline.common.auth import TokenClient, tls_verify_from_env
    from pipeline.transform.spark_encrypt import CryptoClient
    tokens = TokenClient(
        idp_url=os.environ.get("IDP_URL", "https://localhost:8443"),
        client_id=client_id,
        client_secret=os.environ[secret_env],
        verify_tls=tls_verify_from_env(),
    )
    return CryptoClient(os.environ.get("CRYPTO_URL", "https://localhost:8444"),
                        tokens, verify_tls=tls_verify_from_env())


def find_input(directory: Path, pattern: str, stage: str) -> Path:
    """Locate a stage's input without hardcoding the dataset name.

    Globs for the run's date and takes the newest match, so the handoff works
    for any dataset while still failing loudly -- naming the directory it
    searched -- when nothing upstream produced a file. Each stage verifies its
    input exists rather than trusting that the previous task succeeded; those
    are different guarantees and the pipeline wants both.
    """
    matches = sorted(directory.glob(pattern), key=lambda p: p.stat().st_mtime)
    if not matches:
        raise FileNotFoundError(
            f"{stage}: no file matching {pattern!r} in {directory}. The upstream "
            f"stage reported success but its output is missing -- check that run.")
    return matches[-1]


# ── The stages ────────────────────────────────────────────────────────────

def extract(env: RunEnv, events: dict) -> dict:
    """Bounded, cost-capped read of one window, with a CSV fallback, held to
    the dataset's contract."""
    from pipeline.extract.fixture import extract_with_fallback
    from pipeline.metadata import contract as contract_mod
    from pipeline.metadata import schema as schema_mod

    with _source_from_conf(env.conf):
        r, ex, attempts, fell_back = extract_with_fallback(env.run_slot, env.data_root / "csv", log)

    csv_path = Path(r.csv_path)
    manifest = schema_mod.load_manifest(csv_path)
    check = contract_mod.ensure_contract(manifest, csv_path)

    # Where the data's dates come from, in order of preference: a declared
    # event-time column; the archive mapping (BigQuery); else the run date.
    if check.window:
        window_date, window_end, source = check.window[0], check.window[1], "event_time_column"
    elif type(ex).__name__ == "BigQueryExtractor":
        window_date, window_end, source = r.window_date, r.window_end, "archive_mapping"
    else:
        window_date, window_end, source = r.window_date, r.window_end, "logical_date"

    return stage_event(
        "extract", env.trace_id,
        mode=type(ex).__name__, bq_attempts=attempts, fell_back_to_csv=fell_back,
        rows=r.row_count, window_date=window_date, window_end=window_end,
        window_source=source, bytes_billed=r.bytes_billed, bq_job_id=r.query_id,
        csv=r.csv_path, dataset=manifest.dataset,
        contract=str(check.path), contract_created=check.created,
        source="upload" if env.is_upload else "configured",
        source_file=env.conf.get("csv_path"),
        row_limit=int(env.conf.get("row_limit") or 0) if env.is_upload else None,
    )


def card_split(env: RunEnv, events: dict) -> dict:
    """Break card columns into three parts; encrypt only the middle. A no-op,
    reported as such, when the dataset has no card column."""
    from pipeline.transform.card_split import run
    csv_path = find_input(env.data_root / "csv", f"*_{env.date.isoformat()}.csv", "card_split")
    r = run(env.date, csv_path, env.data_root / "cards",
            crypto_client("spark-job", "CLIENT_SECRET_SPARK_JOB"))
    return stage_event("card_split", env.trace_id, rows=r["rows"], cards=r["cards"],
                       columns=r["columns"], dataset=r["dataset"], output=r["output"])


def encrypt(env: RunEnv, events: dict) -> dict:
    from pipeline.transform.spark_encrypt import run
    csv_path = find_input(env.data_root / "csv", f"*_{env.date.isoformat()}.csv", "encrypt")
    r = run(env.date, csv_path, env.data_root / "parquet",
            crypto_client("spark-job", "CLIENT_SECRET_SPARK_JOB"))
    return stage_event("encrypt", env.trace_id, rows=r["rows"], engine=r["engine"],
                       spark_configured=bool(os.environ.get("SPARK_MASTER_URL")),
                       parquet=r["parquet_path"], algorithm="AES-256-GCM",
                       identity="spark-job", dataset=r["dataset"],
                       encrypted_columns=len(r["sensitive_columns"]))


def publish(env: RunEnv, events: dict) -> dict:
    """Each encrypted row to two Kafka topics, wrapped in the platform envelope."""
    from pipeline.publish.kafka_publish import RunContext, publish_rows
    pq = find_input(env.data_root / "parquet" / f"dt={env.date.isoformat()}", "*.parquet", "publish")
    ctx = RunContext(run_slot=env.run_slot, trace_id=env.trace_id, run_id=env.run_id,
                     try_number=env.try_number,
                     producer=os.environ.get("KAFKA_SASL_USERNAME", "pipeline"))
    r = publish_rows(ctx, pq)
    return stage_event("publish", env.trace_id, rows=r["rows"],
                       topic_encrypted=r["topic_encrypted"], topic_public=r["topic_public"],
                       encrypted_fields=r["encrypted_fields"], public_fields=r["public_fields"],
                       dataset=r["dataset"])


def mask(env: RunEnv, events: dict) -> dict:
    """Decrypt, mask, write. Plaintext exists only in this process's memory."""
    from pipeline.transform.hive_mask import run
    pq = find_input(env.data_root / "parquet" / f"dt={env.date.isoformat()}", "*.parquet", "mask")
    r = run(env.date, pq, env.data_root / "hive" / pq.stem,
            crypto_client("hive-job", "CLIENT_SECRET_HIVE_JOB"))
    return stage_event("mask", env.trace_id, rows=r["rows"], masked_by=r["masked_by"],
                       policies=len(r["masks_applied"]), hive=r["hive_path"],
                       identity="hive-job", dataset=r["dataset"], policy_file=r["policy_file"])


def hive_register(env: RunEnv, events: dict) -> dict:
    """Expose the masked Parquet as a Hive table. Non-fatal when Hive is down:
    the data is already written and correct. The verdict reports it."""
    from pipeline.transform.hive_register import run
    pq = find_input(env.data_root / "hive", f"*/dt={env.date.isoformat()}/*_masked.parquet",
                    "hive_register")
    r = run(env.date, pq)
    table = f"{r['database']}.{r['table']}" if r.get("registered") else None
    return stage_event("hive_register", env.trace_id, registered=bool(r.get("registered")),
                       table=table, partition=r.get("partition"), reason=r.get("reason"))


def load(env: RunEnv, events: dict) -> dict:
    from pipeline.load.postgres_load import run
    pq = find_input(env.data_root / "hive", f"*/dt={env.date.isoformat()}/*_masked.parquet", "load")
    r = run(env.date, pq, events.get("extract"), run_slot=env.run_slot, trace_id=env.trace_id)
    return stage_event("load", env.trace_id, inserted=r["inserted"], updated=r["updated"],
                       run_id=r["run_id"], dataset=r["dataset"])


def report(env: RunEnv, events: dict) -> dict:
    """The PDF, for this run only, carrying the verdict as it stands so far."""
    from pipeline import verdict as verdict_mod
    from pipeline.metadata import schema as schema_mod
    from pipeline.report.collect import collect
    from pipeline.report.pdf_report import run

    pq = find_input(env.data_root / "hive", f"*/dt={env.date.isoformat()}/*_masked.parquet", "report")
    manifest = schema_mod.load_manifest(pq)
    # The verdict over everything before the report: the page prints the
    # warnings that exist by now. The final verdict adds the report itself.
    so_far = verdict_mod.compute({s: events.get(s) for s, _ in STAGES if s != "report"})
    r = run(env.run_slot, env.data_root / "reports", dataset=manifest.dataset,
            primary_key=manifest.primary_key, trace_id=env.trace_id,
            status=so_far.status, reasons=so_far.reasons, data_window=so_far.data_window)
    c = collect(r["pdf_path"])
    return stage_event("report", env.trace_id, rows=r["rows"], pdf=r["pdf_path"],
                       dataset=manifest.dataset, collected=c.get("dest") or c.get("reason"))


RUNNERS = {
    "extract": extract, "card_split": card_split, "encrypt": encrypt, "publish": publish,
    "mask": mask, "hive_register": hive_register, "load": load, "report": report,
}


def run_stage(name: str, env: RunEnv, events: dict) -> dict:
    """Run one stage and return its event, checked against the stage contract."""
    from pipeline.messaging.contracts import validate_stage_event
    ev = RUNNERS[name](env, events)
    validate_stage_event(ev)
    return ev


# ── The verdict: always last, always emitted ──────────────────────────────

def emit_verdict(env: RunEnv, events: dict, dataset: str | None = None,
                 signer=None, publisher=None) -> dict:
    """Compute, sign and publish the run verdict. Returns the publish result
    plus the verdict payload. Raises AFTER publishing if any stage event was
    malformed, so the DAG shows red while downstream still hears a verdict."""
    from pipeline import verdict as verdict_mod
    from pipeline.messaging.contracts import ContractError, validate_stage_event
    from pipeline.publish.kafka_publish import RunContext, publish_verdict

    clean: dict[str, dict | None] = {}
    malformed: list[str] = []
    for stage, _ in STAGES:
        ev = events.get(stage)
        if ev is None:
            clean[stage] = None
            continue
        try:
            validate_stage_event(ev)
            clean[stage] = ev
        except ContractError as e:
            malformed.append(f"{stage}: {e}")
            clean[stage] = None

    v = verdict_mod.compute(clean)
    if malformed:
        v.reasons.extend(f"stage event violated its contract -- {m}" for m in malformed)
    payload = v.to_payload()

    dataset = dataset or next((ev["dataset"] for ev in clean.values()
                               if ev and ev.get("dataset")), None) or env.pipeline
    ctx = RunContext(run_slot=env.run_slot, trace_id=env.trace_id, run_id=env.run_id,
                     try_number=env.try_number,
                     producer=os.environ.get("KAFKA_SASL_USERNAME", "pipeline"))
    result = publish_verdict(ctx, dataset, payload, signer or default_signer(), publisher)
    result["verdict"] = payload
    result["trace_id"] = env.trace_id
    if malformed:
        raise ContractError("verdict emitted with proceed=false because stage events "
                            "violated their contract:\n  " + "\n  ".join(malformed))
    return result


def default_signer():
    """The orchestrator's signing client: authenticates to the IdP as
    orchestrator-job, which may sign verdicts and do nothing else."""
    from pipeline.common.auth import TokenClient, tls_verify_from_env
    from pipeline.messaging.signing import SIGNER_CLIENT_ID, Signer
    idp = os.environ.get("IDP_URL", "https://localhost:8443")
    tokens = TokenClient(idp_url=idp, client_id=SIGNER_CLIENT_ID,
                         client_secret=os.environ["CLIENT_SECRET_ORCHESTRATOR_JOB"],
                         verify_tls=tls_verify_from_env())
    return Signer(idp, tokens, verify_tls=tls_verify_from_env())
