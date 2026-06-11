"""Symmetric encryption box using libsodium SecretBox (XSalsa20-Poly1305).

The aios server holds a single 32-byte master key in the ``AIOS_VAULT_KEY`` env
var (base64-encoded). Every *active* encrypted row stores a randomly-generated
nonce alongside its ciphertext; encryption is authenticated, so any tampering
or key mismatch produces a clean error rather than silent corruption. Archived
rows have their ciphertext and nonce zeroed out so that a future DB dump or
query cannot leak the secret — read paths filter ``WHERE archived_at IS NULL``
to avoid attempting to decrypt the scrubbed bytes.

Multi-tenancy (#367): the master ``CryptoBox`` can derive a per-account
subkey via HKDF-SHA256, returning a new ``CryptoBox`` whose secrets can't
be decrypted with the master key OR any sibling tenant's derived key.
This is the building block for per-account encryption — see
:meth:`CryptoBox.derive_account_subkey`.
"""

from __future__ import annotations

import base64
import binascii
import hashlib
import hmac
import json
from dataclasses import dataclass
from typing import Any

from nacl.exceptions import CryptoError
from nacl.secret import SecretBox
from nacl.utils import random as nacl_random

from aios.errors import CryptoDecryptError

# SecretBox uses 24-byte nonces and 32-byte keys.
KEY_BYTES = SecretBox.KEY_SIZE
NONCE_BYTES = SecretBox.NONCE_SIZE


@dataclass(frozen=True, slots=True)
class EncryptedBlob:
    """A ciphertext + nonce pair as stored in the credentials table."""

    ciphertext: bytes
    nonce: bytes


class CryptoBox:
    """libsodium-backed encrypt/decrypt wrapper around a single master key.

    Construct once at process start with the master key bytes; subsequent
    operations are pure and stateless beyond the held key.
    """

    def __init__(self, master_key: bytes) -> None:
        if len(master_key) != KEY_BYTES:
            raise ValueError(f"master key must be {KEY_BYTES} bytes, got {len(master_key)}")
        self._key = master_key
        self._box = SecretBox(master_key)

    def derive_account_subkey(self, account_id: str) -> CryptoBox:
        """Return a new :class:`CryptoBox` keyed to ``account_id`` via HKDF.

        Uses HKDF-SHA256 with the current box's key as IKM, an aios-specific
        salt, and ``f"aios-account-{account_id}"`` as the info context.
        Properties:

        * Deterministic — re-deriving for the same account_id returns
          the same subkey bytes.
        * Domain-separated — two different account_ids produce two
          unrelated 32-byte subkeys; an attacker holding one subkey
          cannot recover the master or any sibling subkey.
        * One-way — knowing a subkey doesn't reveal the master.

        The returned subkey is itself a :class:`CryptoBox` and supports
        ``encrypt`` / ``decrypt`` exactly like the master.
        """
        if not account_id:
            raise ValueError("account_id must be non-empty")
        # Single-extract HKDF-SHA256: the "extract" step normalises an
        # arbitrary-strength IKM (always 32 bytes for us) into a PRK,
        # then the "expand" step stretches PRK + info into the output.
        # Salt is a fixed application-specific constant — operators
        # rotate by reissuing keys, not by changing salt.
        salt = b"aios-vault-hkdf-v1"
        info = f"aios-account-{account_id}".encode()
        prk = hmac.new(salt, self._key, hashlib.sha256).digest()
        # Single-block expand: output is one SHA256 block (32 bytes),
        # which matches SecretBox's KEY_BYTES so we don't need to chain
        # multiple T(i) outputs.
        t1 = hmac.new(prk, info + b"\x01", hashlib.sha256).digest()
        return CryptoBox(t1)

    @classmethod
    def from_base64(cls, encoded: str) -> CryptoBox:
        """Load a CryptoBox from a base64-encoded master key string.

        This is the form stored in ``AIOS_VAULT_KEY`` and ``.env`` files.
        """
        try:
            key_bytes = base64.b64decode(encoded, validate=True)
        except (ValueError, binascii.Error) as exc:
            raise ValueError(f"AIOS_VAULT_KEY is not valid base64: {exc}") from exc
        return cls(key_bytes)

    def encrypt(self, plaintext: str) -> EncryptedBlob:
        """Encrypt ``plaintext`` and return the (ciphertext, nonce) pair.

        A fresh random nonce is generated for every call. The plaintext is
        UTF-8-encoded before encryption.
        """
        nonce = nacl_random(NONCE_BYTES)
        ciphertext = self._box.encrypt(plaintext.encode("utf-8"), nonce).ciphertext
        return EncryptedBlob(ciphertext=ciphertext, nonce=nonce)

    def decrypt(self, blob: EncryptedBlob) -> str:
        """Decrypt and return the original plaintext string.

        Raises :class:`~aios.errors.CryptoDecryptError` if the master key
        doesn't match (key rotation without re-encryption) or the ciphertext
        has been tampered with.
        """
        try:
            plaintext_bytes = self._box.decrypt(blob.ciphertext, blob.nonce)
        except CryptoError as exc:
            raise CryptoDecryptError(
                "could not decrypt — wrong key or corrupted ciphertext",
                detail={"reason": str(exc)},
            ) from exc
        return plaintext_bytes.decode("utf-8")

    def encrypt_dict(self, payload: dict[str, Any]) -> EncryptedBlob:
        """Encrypt a JSON-serialisable dict.  Convenience over ``encrypt``."""
        return self.encrypt(json.dumps(payload))

    def decrypt_dict(self, blob: EncryptedBlob) -> dict[str, Any]:
        """Decrypt and JSON-decode to a dict.  Raises ``ValueError`` if the
        decrypted plaintext doesn't shape as a JSON object."""
        plaintext = self.decrypt(blob)
        decoded = json.loads(plaintext)
        if not isinstance(decoded, dict):
            raise ValueError(
                f"decrypted blob did not decode to a dict; got {type(decoded).__name__}"
            )
        return decoded
