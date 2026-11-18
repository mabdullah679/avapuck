# avapuck

Privacy-first data platform: BigQuery public data or console CSV uploads go
through Spark, Kafka, Hive (Ranger masking) and Postgres, produce one PDF per
run, and end in one signed `run.verdict`. Master rules for working with the
owner: `~/.claude/CLAUDE.md` (read it; not copied here). Working agreement for
changing code: `AGENTS.md`. What is stubbed or unverified: `TRUST-BOUNDARY.md`.

## Commands (run 2026-11-18, Python 3.12)

```bash
python3.12 -m venv .venv && .venv/bin/pip install -r requirements-ci.txt
make ci            # service tier + platform tier; 318 passed, 4 skipped
make test          # pytest only
make ci-service    # pytest + contracts_check.py examples
make ci-platform   # contract backward-compat vs CI_BASE_REF (default main)
./setup.sh         # full Docker stack (needs Docker; not run in CI)
```

Skips are environment-dependent (no `google-cloud-bigquery` in
`requirements-ci.txt`, no extract secret, no service-account key). The suite is
pure Python: no Docker, no cloud. GitHub Actions (`.github/workflows/ci.yml`)
runs the same two make tiers plus a gitleaks scan; Jenkins (`Jenkinsfile`)
runs the same tiers locally. `.gitleaks.toml` allowlists only
`contracts/examples/`.

## Layout

`pipeline/` stages (`stages.py` is the single stage list), `dags/` two Airflow
DAGs, `services/` idp, crypto, console, tap, watchdog, scaler, `contracts/`
JSON Schemas + examples, `config/` per-dataset contracts and Ranger policy,
`docker/` images, `scripts/ci/` contract checks, `docs/` design (`DECISIONS.md`
is the why), `tests/`.

## Do not touch / do not do

- Never commit `.env.local`, `certs/`, `secrets/`, `data/` contents, or
  `pipeline-reports/` (all gitignored; `tests/test_secret_hygiene.py` guards it).
- `config/datasets/` and `config/ranger/` files are generated once and then
  owner-edited; do not regenerate over them. Contracts in `contracts/` must stay
  backward-compatible (the platform tier fails otherwise).
- This repo is public. No real data, client names, or account IDs.
- `.claude/settings.json` auto-allows Bash for the owner's sessions; do not
  widen it.
- `docker compose` project name defaults to `avapuck`; running the stack from a
  second checkout shares the first one's volumes and network.

## Non-negotiables

1. **Sensitive data is encrypted before it lands anywhere.** Plaintext PII must
   never reach Parquet, Kafka, Hive, Postgres, logs, or the PDF. The only
   plaintext window is inside the HTTPS crypto service. Which columns are
   sensitive comes from the schema manifest; `assert_no_plaintext` enforces it
   before every write, and `_assert_no_plaintext_published` before every send.
2. **Every job authenticates with a JWT from the IdP.** No job trusts another
   job's word about identity. Tokens are short-lived and verified by signature,
   issuer, audience, and expiry — never merely decoded. The run verdict is
   signed by the IdP and verified the same way.
3. **Masking policy is Apache Ranger policy JSON**, authored once and enforced
   at the Hive boundary. Masking rules never live in transformation code. A
   dataset with no policy gets one GENERATED into `config/ranger/` for review,
   written once and never regenerated over an edit. The same pattern governs
   the dataset contract in `config/datasets/`.
4. **100 rows per BigQuery run, hard-capped**, enforced in the SQL and
   asserted after, because that is a bill. Uploads through the console are
   bounded only by `UPLOAD_MAX_ROWS` (0 = none), and every stage streams in
   chunks of `PIPELINE_BATCH_ROWS` so a large upload and a 100-row run take
   the same path. `docs/DECISIONS.md` D16.
5. **Idempotent by run slot.** A re-run of the same slot replaces that slot's
   partition and never double-loads Postgres. On Kafka the envelope carries an
   `idempotency_key` per unit of work and an `event_id` per emission; a
   consumer upserts on the first and drops a repeat of the second.
6. **The verdict is always emitted and is the only predicate.** `emit_verdict`
   runs with `trigger_rule=all_done`; `pipeline/verdict.py` decides; consumers
   read `proceed` and nothing else.
7. **One place for every value.** `.env.local` is the single source of
   configuration and secrets. No `${VAR:-default}` in compose.

## Definition of done

`airflow dags test` runs the chain end to end, the PDF is produced, the
verdict lands on `pipeline_runs` and the watchdog reports healthy, `make ci`
passes, and `TRUST-BOUNDARY.md` honestly states what is stubbed.
