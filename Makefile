.PHONY: help test ci ci-service ci-platform hooks run up down check-auth

# The repo's venv when present, else whatever python3 is on PATH. Jenkins sets
# PYTHON to its own venv (docker/Dockerfile.jenkins).
PYTHON ?= $(if $(wildcard .venv/bin/python),.venv/bin/python,python3)
# The platform tier diffs contracts against this git ref.
CI_BASE_REF ?= main
COMPOSE = docker compose --env-file .env.local --profile core

help:
	@echo "test          run the test suite"
	@echo "ci            both CI tiers, exactly what Jenkins and the pre-push hook run"
	@echo "ci-service    tests + contract examples"
	@echo "ci-platform   envelope embedding + backward compatibility vs $(CI_BASE_REF)"
	@echo "hooks         install the pre-push and post-commit hooks into .git/hooks"
	@echo "run           run the pipeline for the current hour without Airflow"
	@echo "up / down     start / stop the compose stack (core profile)"
	@echo "check-auth    verify GCP credentials work before running live"

test:
	@$(PYTHON) -m pytest tests/ -q

ci: ci-service ci-platform

ci-service:
	@$(PYTHON) -m pytest tests/ -q
	@$(PYTHON) scripts/ci/contracts_check.py examples

ci-platform:
	@$(PYTHON) scripts/ci/contracts_check.py platform --base $(CI_BASE_REF)

hooks:
	@test -d .git || { echo "not a git checkout; nothing to install"; exit 1; }
	@install -m 0755 scripts/git-hooks/pre-push .git/hooks/pre-push
	@install -m 0755 scripts/git-hooks/post-commit .git/hooks/post-commit
	@echo "installed .git/hooks/pre-push and .git/hooks/post-commit"

run:
	@$(PYTHON) -m pipeline.run_pipeline

up:
	@$(COMPOSE) up -d --build

down:
	@$(COMPOSE) down

check-auth:
	@$(PYTHON) scripts/check_gcp_auth.py
