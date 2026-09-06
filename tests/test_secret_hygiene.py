"""Secret hygiene, asserted rather than assumed.

A leaked service-account key is the single worst failure this repo could have,
and it is a failure of process rather than of code -- someone renames a file,
weakens an ignore rule, or pastes a value into a config. These tests fail the
suite when that happens, which is the only reliable defence.
"""
from __future__ import annotations

import json
import re
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]


def test_secrets_dir_is_ignored():
    """The whole directory, not just known filenames."""
    r = subprocess.run(["git", "check-ignore", "-v", "secrets/gcp-sa.json"],
                       cwd=ROOT, capture_output=True, text=True)
    assert r.returncode == 0, (
        "secrets/gcp-sa.json is NOT gitignored. Fix .gitignore before doing "
        "anything else -- a committed key is compromised the moment it lands."
    )


def test_no_secret_files_staged_or_tracked():
    r = subprocess.run(["git", "ls-files"], cwd=ROOT, capture_output=True, text=True)
    tracked = r.stdout.splitlines()
    offenders = [f for f in tracked
                 if f.startswith("secrets/") and not f.endswith("README.md")]
    assert not offenders, f"credential files are tracked by git: {offenders}"


def test_env_files_not_tracked():
    """.env.local is the one place values live; it must never be committed."""
    r = subprocess.run(["git", "ls-files", ".env", ".env.local", ".env.*.local"],
                       cwd=ROOT, capture_output=True, text=True)
    assert not r.stdout.strip(), f"an env file is tracked by git: {r.stdout.split()}"


def test_only_one_env_template_is_committed():
    """One .env.example; a second template is where values start to drift."""
    r = subprocess.run(["git", "ls-files", "--cached", "--others", "--exclude-standard"],
                       cwd=ROOT, capture_output=True, text=True)
    templates = [f for f in r.stdout.split() if f.startswith(".env") and f != ".env.example"]
    assert templates == [], f"extra env templates: {templates}"


def test_compose_has_no_builtin_secret_defaults():
    """`${VAR:-default}` in compose is a second place a secret can live."""
    import re
    text = (ROOT / "docker-compose.yml").read_text()
    defaults = re.findall(r"\$\{([A-Z0-9_]+):-[^}]*\}", text)
    assert defaults == [], f"compose carries built-in defaults for: {sorted(set(defaults))}"


def test_every_compose_variable_is_in_the_template():
    """A variable compose requires must be declared in .env.example, or a
    fresh clone fails `up` with no way to know what to add."""
    import re
    compose = "\n".join(l for l in (ROOT / "docker-compose.yml").read_text().splitlines()
                        if not l.strip().startswith("#"))
    required = set(re.findall(r"\$\{([A-Z0-9_]+)\??:?\?", compose))
    template = (ROOT / ".env.example").read_text()
    declared = set(re.findall(r"^([A-Z0-9_]+)=", template, flags=re.M))
    missing = sorted(required - declared)
    assert missing == [], f"compose requires variables .env.example does not declare: {missing}"


# Shapes that indicate a real credential rather than a placeholder.
_SECRET_PATTERNS = [
    (re.compile(r"-----BEGIN [A-Z ]*PRIVATE KEY-----"), "private key block"),
    (re.compile(r"\"private_key_id\"\s*:\s*\"[a-f0-9]{40}\""), "GCP key id"),
    (re.compile(r"\bAIza[0-9A-Za-z_\-]{35}\b"), "Google API key"),
    (re.compile(r"\bya29\.[0-9A-Za-z_\-]+"), "OAuth access token"),
]


def test_no_credential_material_in_tracked_files():
    r = subprocess.run(["git", "ls-files"], cwd=ROOT, capture_output=True, text=True)
    findings = []
    for rel in r.stdout.splitlines():
        p = ROOT / rel
        if not p.is_file() or p.suffix in {".png", ".pdf", ".parquet"}:
            continue
        try:
            text = p.read_text(errors="ignore")
        except OSError:
            continue
        for pat, label in _SECRET_PATTERNS:
            if pat.search(text):
                findings.append(f"{rel}: {label}")
    assert not findings, f"credential material found in tracked files: {findings}"


@pytest.mark.skipif(not (ROOT / "secrets" / "gcp-sa.json").exists(),
                    reason="no service-account key present yet")
def test_key_file_permissions_and_shape():
    p = ROOT / "secrets" / "gcp-sa.json"
    mode = p.stat().st_mode & 0o077
    assert mode == 0, (
        f"secrets/gcp-sa.json is group/world readable (mode {oct(p.stat().st_mode)}). "
        f"Run: chmod 600 secrets/gcp-sa.json"
    )
    d = json.loads(p.read_text())
    assert d.get("type") == "service_account", "not a service-account key file"
    for field in ("project_id", "client_email", "private_key"):
        assert d.get(field), f"key file is missing {field!r}"
