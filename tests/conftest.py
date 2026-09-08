"""Test-wide defaults.

The traffic auditor is off for app tests: the in-process test client has no
IP address, and the per-client limits would otherwise trip across a run of
the suite. tests/test_traffic.py exercises the auditor itself, directly and
through a middleware it installs with the audit switched on.
"""
import os

os.environ.setdefault("TRAFFIC_AUDIT", "off")
os.environ.setdefault("CONSOLE_SESSION_SECRET", "test-session-secret")
