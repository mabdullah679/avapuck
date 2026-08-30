"""Source paths per service, declared once."""
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

SOURCE_PATHS = {
    "service_a": ROOT / "data/raw/service_a/ocio_quarterly.dat",
    "service_b": ROOT / "data/raw/service_b/merchant_platform_export.xml",
    "service_c": ROOT / "data/raw/service_c/risk_identity_quarterly.csv",
    "service_d": ROOT / "data/raw/service_d/legacy_setl_extract.psv",
}
