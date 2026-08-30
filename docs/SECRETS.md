# Secrets

## The rule

**One secret, one home: Google Secret Manager.** `.env.local` declares the
*names*; values are resolved at sync time into a gitignored runtime file.

Why not literal values in a dotfile: they drift. Someone rotates a secret in
GCP, the local file keeps the old value, and it surfaces days later as a
confusing auth failure in a DAG. Storing a *reference* instead of a *value*
makes that class of bug impossible, because nothing stale is stored at all.

## Files

| File | Committed? | What it is |
|---|---|---|
| `.env.local.example` | **yes** | template — names only, no values |
| `.env.local` | **no** | your copy; SM references, optional literal overrides |
| `.env.local.resolved` | **no** | generated at sync; what the pipeline loads |
| `secrets/gcp-sa.json` | **no** | the one bootstrap credential |

## First-time setup

```bash
cp .env.local.example .env.local
# set GCP_PROJECT_ID (the ID string, not the number 58996819807)

gcloud auth application-default login     # preferred: no key file at all
# or: place a key at secrets/gcp-sa.json && chmod 600 secrets/gcp-sa.json

make secrets-bootstrap    # creates the secrets in GCP, generated values
make secrets-sync         # resolves them into .env.local.resolved
```

## Keeping it 1:1

```bash
make secrets-verify   # every declared name exists in Secret Manager
make secrets-drift    # any literal *_VALUE that has gone stale
make secrets-sync     # re-resolve after a rotation
```

`secrets-drift` is the one that catches the silent failure. Run it after any
rotation, and in CI if this ever gets a pipeline.

## The bootstrap exception

`GOOGLE_APPLICATION_CREDENTIALS` cannot itself live in Secret Manager —
reading from SM requires GCP auth already. That is the one unavoidable
chicken-and-egg, and it is why ADC (`gcloud auth application-default login`)
is preferable to a key file locally: it leaves nothing long-lived on disk to
leak or rotate.

## Rotation

```bash
printf '%s' "$NEW" | gcloud secrets versions add pipeline-postgres-password \
  --project "$GCP_PROJECT_ID" --data-file=-
make secrets-sync
```

References pin `/versions/latest`, so a sync picks up the new value with no
edit to `.env.local`. That is the 1:1 property working as intended.

## If something leaks

Rotate first, investigate second — a leaked secret is valid until revoked.
Removing it from a later commit does not help; git history keeps it.
`tests/test_secret_hygiene.py` fails the suite if a credential ever becomes
tracked or an ignore rule is weakened.
