.PHONY: help secrets-bootstrap secrets-sync secrets-verify secrets-drift test

help:
	@echo "secrets-bootstrap  create the pipeline's secrets in GCP Secret Manager (once)"
	@echo "secrets-sync       resolve .env.local's SM_* refs -> .env.local.resolved"
	@echo "secrets-verify     check every declared secret exists in Secret Manager"
	@echo "secrets-drift      compare literal *_VALUE entries against Secret Manager"
	@echo "test               run the test suite (includes secret hygiene)"

secrets-bootstrap:
	@set -a; [ -f .env.local ] && . ./.env.local; set +a; ./scripts/secrets_bootstrap.sh

secrets-sync:
	@python3 scripts/secrets_sync.py

secrets-verify:
	@python3 scripts/secrets_sync.py --verify

secrets-drift:
	@python3 scripts/secrets_sync.py --check-drift

test:
	@python3 -m pytest tests/ -q
