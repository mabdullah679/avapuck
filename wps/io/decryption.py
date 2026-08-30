"""The decryption boundary.

Service D delivers PII and PCI fields as opaque blobs. The pipeline knows that
a field is encrypted, which key alias governs it, and what type it becomes
once decrypted -- WITHOUT this repository ever holding key material.

This module is the named interface that makes that true. It is deliberately
NOT a key-management implementation. See TRUST-BOUNDARY.md section 2.1.

At a fintech, "what happens to the encrypted fields" is the first objection
raised. The answer is: they pass through a declared seam that a real provider
plugs into, and PCI-classified fields are never decrypted here at all.
"""
from __future__ import annotations

import hashlib
from typing import Protocol


class DecryptionProvider(Protocol):
    """The seam. A production implementation would call a KMS/HSM here."""

    def decrypt(self, ciphertext: str, key_alias: str, plaintext_type: str) -> str: ...

    def can_decrypt(self, key_alias: str) -> bool: ...


class SyntheticDemoProvider:
    """DEMO ONLY. Not cryptography.

    The synthetic corpus produces blobs as sha256(key_alias + plaintext), which
    is one-way, so this provider cannot and does not recover the original. It
    returns a STABLE DERIVED SURROGATE instead -- which is honest about what it
    is, and is sufficient for everything downstream, because every decrypted
    value in this pipeline is immediately tokenized anyway.

    Refusing to pretend it recovered the plaintext is the point: a demo that
    silently fabricated a plausible tax reference would be teaching the wrong
    lesson about what is real here.
    """

    #: PCI keys are deliberately absent. This provider CANNOT decrypt them,
    #: which means the pipeline's PCI path is not merely policy-blocked --
    #: it is incapable.
    _KNOWN_ALIASES = {"legacy-setl-pii-v2"}

    def can_decrypt(self, key_alias: str) -> bool:
        return key_alias in self._KNOWN_ALIASES

    def decrypt(self, ciphertext: str, key_alias: str, plaintext_type: str) -> str:
        if not self.can_decrypt(key_alias):
            raise PermissionError(
                f"No key material for alias {key_alias!r}. PCI-scoped aliases are "
                f"intentionally unavailable to this pipeline -- see "
                f"config/classification/policy.yaml and TRUST-BOUNDARY.md 2.1."
            )
        digest = hashlib.sha256(f"{key_alias}|{ciphertext}".encode()).hexdigest()
        return f"SURROGATE-{digest[:16].upper()}"


class NullProvider:
    """Used where decryption must be provably impossible."""

    def can_decrypt(self, key_alias: str) -> bool:
        return False

    def decrypt(self, ciphertext: str, key_alias: str, plaintext_type: str) -> str:
        raise PermissionError("decryption is not available in this context")


def default_provider() -> DecryptionProvider:
    return SyntheticDemoProvider()
