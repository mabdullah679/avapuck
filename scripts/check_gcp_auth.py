"""Verify GCP credentials are usable BEFORE the pipeline tries to bill anything.

Run this after placing a key. It resolves credentials the same way the
pipeline does, confirms the project, and does a FREE dry run against the
target table -- so a misconfiguration surfaces here rather than as a confusing
failure three stages into a DAG.
"""
from __future__ import annotations

import os
import sys
import warnings
from datetime import date, timedelta
from pathlib import Path

warnings.filterwarnings("ignore")
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

OK, BAD, WARN = "  [ok]  ", "  [FAIL]", "  [warn]"


def main() -> int:
    from dotenv import dotenv_values
    env = {k: v for k, v in dotenv_values(ROOT / ".env.local").items() if v}
    os.environ.update(env)

    project = env.get("GCP_PROJECT_ID")
    print("GCP auth preflight\n" + "-" * 52)

    if not project:
        print(f"{BAD} GCP_PROJECT_ID is not set in .env.local")
        return 1
    print(f"{OK} GCP_PROJECT_ID = {project}")

    # 1. Is there a credential at all?
    key_path = env.get("GOOGLE_APPLICATION_CREDENTIALS")
    adc = Path.home() / ".config/gcloud/application_default_credentials.json"
    if key_path and (ROOT / key_path).exists():
        print(f"{OK} service-account key found at {key_path}")
        mode = (ROOT / key_path).stat().st_mode & 0o077
        if mode:
            print(f"{WARN} key is group/world readable — run: chmod 600 {key_path}")
    elif adc.exists():
        print(f"{OK} application-default credentials found")
    else:
        print(f"{BAD} no credential found.")
        print(f"         expected a key at {key_path!r}, or ADC via:")
        print(f"           gcloud auth application-default login")
        print(f"         or create a key:")
        print(f"           gcloud iam service-accounts keys create {key_path} \\")
        print(f"             --iam-account={env.get('GCP_SERVICE_ACCOUNT_EMAIL','<sa-email>')} \\")
        print(f"             --project={project}")
        return 1

    # 2. Does Google accept it?
    try:
        import google.auth
        creds, detected = google.auth.default(
            scopes=["https://www.googleapis.com/auth/bigquery"])
        print(f"{OK} credentials resolved ({type(creds).__name__})")
        if detected and detected != project:
            print(f"{WARN} credential's project is {detected!r}, .env.local says "
                  f"{project!r} — confirm which is right")
    except Exception as e:
        print(f"{BAD} credential rejected: {type(e).__name__}: "
              f"{str(e).splitlines()[0][:160]}")
        return 1

    # 3. Can it actually reach BigQuery, and what would a run cost?
    try:
        from pipeline.extract.bigquery_extract import extractor_from_env
        ex = extractor_from_env()
        # Yesterday, so the partition is closed and populated.
        probe = date.today() - timedelta(days=1)
        b = ex.dry_run_bytes(probe)
        mib = b / 1_048_576
        cap = ex.max_bytes_billed / 1_048_576
        print(f"{OK} dry run succeeded: {mib:.3f} MiB would be scanned "
              f"(cap {cap:.0f} MiB)")
        if b > ex.max_bytes_billed:
            print(f"{BAD} estimate EXCEEDS the cap — the real query would be "
                  f"refused. Narrow the predicate or drop columns.")
            return 1
        print(f"{OK} well inside the 1 TiB/month free tier "
              f"({mib * 30 / 1024:.2f} GiB/month at one run per day)")
    except Exception as e:
        print(f"{BAD} BigQuery call failed: {type(e).__name__}: "
              f"{str(e).splitlines()[0][:200]}")
        print( "         common causes: missing roles/bigquery.jobUser,")
        print( "         wrong project id, or the API not enabled on the project")
        return 1

    print("-" * 52)
    print("Ready. Run a live extract with:")
    print("  EXTRACT_MODE=live .venv/bin/python -m pipeline.run_pipeline \\")
    print("    --date $(date -v-1d +%F) --stage extract")
    return 0


if __name__ == "__main__":
    sys.exit(main())
