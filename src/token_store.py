"""
Flat-file JSON token store for Nest session credentials.

Replaces the pickle-based cache in nest_client.py so the token
file is human-readable, easily volume-mounted in Docker, and
simple to inspect or wipe without Python tooling.

Token file format (JSON):
  {
    "access_token":        "g.0.eyJ...",
    "transport_url":       "https://...",
    "user":                "user.1234567",
    "userid":              "1234567",
        "issue_token":         "https://accounts.google.com/o/oauth2/iframerpc?...",
    "google_bearer_token": "ya29...",
        "google_cookie_header": "SID=...; __Secure-1PSID=...",
        "google_bearer_expiration": 1717230967.0,
    "cache_expiration":    1717234567.0
  }

Default path: config/tokens.json  (relative to cwd, or set
NEST_TOKEN_FILE env-var or pass token_file= explicitly).
"""

from __future__ import annotations

import base64
import hashlib
import json
import logging
import os
import time
from pathlib import Path
from typing import Any

log = logging.getLogger(__name__)

_DEFAULT_SECRET_PATH = "/run/secrets/nest_key"

_DEFAULT_TOKEN_FILE = os.getenv(
    "NEST_TOKEN_FILE",
    os.path.join(os.getcwd(), "config", "tokens.json"),
)

# Keys persisted to disk.
_FIELDS = (
    "access_token",
    "transport_url",
    "user",
    "userid",
    "issue_token",
    "google_bearer_token",
    "google_cookie_header",
    "google_bearer_expiration",
    "cache_expiration",
)


def _get_fernet() -> Any | None:
    """Derive a Fernet key from Docker secret or NEST_TOKEN_ENCRYPTION_KEY."""
    key = _get_encryption_key()
    if not key:
        return None
    try:
        from cryptography.fernet import Fernet
    except ImportError:
        raise ImportError(
            "The 'cryptography' package is required for encrypted tokens. "
            "Please run: pip install cryptography"
        )
    # Derive a valid Fernet key from any passphrase using SHA-256
    derived = hashlib.sha256(key.encode("utf-8")).digest()
    fernet_key = base64.urlsafe_b64encode(derived)
    return Fernet(fernet_key)


def _get_encryption_key() -> str:
    """
    Return the encryption key, preferring Docker secret file first.

    Resolution order:
    1) NEST_TOKEN_ENCRYPTION_KEY_FILE (if provided)
    2) /run/secrets/nest_key (Docker secrets default path)
    3) NEST_TOKEN_ENCRYPTION_KEY env var (fallback)
    """
    secret_file = os.getenv("NEST_TOKEN_ENCRYPTION_KEY_FILE", _DEFAULT_SECRET_PATH)
    if secret_file:
        try:
            p = Path(secret_file)
            if p.exists() and p.is_file():
                secret = p.read_text(encoding="utf-8").strip()
                if secret:
                    return secret
        except Exception as exc:
            log.debug("Unable to read encryption key from %s: %s", secret_file, exc)
    return os.getenv("NEST_TOKEN_ENCRYPTION_KEY", "").strip()


def load(token_file: str | Path | None = None) -> dict[str, Any]:
    """
    Load and return the token dict from *token_file*.

    Returns an empty dict if the file does not exist or is unreadable.
    The caller should check whether the returned ``cache_expiration``
    (float Unix timestamp) is in the future before trusting the tokens.
    """
    path = Path(token_file or _DEFAULT_TOKEN_FILE)
    if not path.exists():
        log.debug("Token file %s not found — starting unauthenticated", path)
        return {}
    try:
        with path.open("rb") as fh:
            raw_data = fh.read()
        if not raw_data:
            return {}

        data = None
        # 1. Try to load as plaintext JSON
        try:
            data = json.loads(raw_data.decode("utf-8"))
            # If successful and key is set, migrate to encrypted immediately
            key = _get_encryption_key()
            if key and data:
                log.info("Plaintext token file detected; transparently encrypting it")
                save(data, path)
        except Exception:
            pass

        # 2. If it wasn't plaintext JSON, attempt decryption
        if data is None:
            fernet = _get_fernet()
            if fernet is None:
                log.warning(
                    "Token file %s appears encrypted (not valid JSON), "
                    "but no encryption key is set (Docker secret or NEST_TOKEN_ENCRYPTION_KEY). Cannot load tokens.",
                    path,
                )
                return {}
            try:
                decrypted_bytes = fernet.decrypt(raw_data)
                data = json.loads(decrypted_bytes.decode("utf-8"))
            except Exception as decrypt_exc:
                log.error("Failed to decrypt token file %s: %s", path, decrypt_exc)
                return {}

        log.debug("Loaded token cache from %s (expires %.0f)", path, data.get("cache_expiration", 0))
        return {k: data.get(k, "") for k in _FIELDS}
    except Exception as exc:
        log.warning("Could not load token file %s: %s", path, exc)
        return {}


def save(
    tokens: dict[str, Any],
    token_file: str | Path | None = None,
) -> None:
    """
    Persist *tokens* dict to *token_file*.

    Only the keys in ``_FIELDS`` are written; all others are ignored.
    The parent directory is created if it does not exist.
    """
    path = Path(token_file or _DEFAULT_TOKEN_FILE)
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = {k: tokens.get(k, "") for k in _FIELDS}
        json_bytes = json.dumps(payload, indent=2).encode("utf-8")

        fernet = _get_fernet()
        if fernet is not None:
            write_data = fernet.encrypt(json_bytes)
            log.debug("Saving encrypted token cache to %s", path)
        else:
            write_data = json_bytes
            log.debug("Saving plaintext token cache to %s", path)

        with path.open("wb") as fh:
            fh.write(write_data)
    except Exception as exc:
        log.warning("Could not save token file %s: %s", path, exc)


def is_valid(tokens: dict[str, Any], margin_sec: float = 0.0) -> bool:
    """
    Return True if *tokens* contains a non-expired access_token.

    *margin_sec* allows pre-emptive refresh (e.g. 600s before expiry).
    """
    return bool(
        tokens.get("access_token")
        and tokens.get("transport_url")
        and float(tokens.get("cache_expiration", 0)) > time.time() + margin_sec
    )


def expiry_str(tokens: dict[str, Any]) -> str:
    """Return a human-readable expiry timestamp, or 'unknown'."""
    exp = tokens.get("cache_expiration", 0)
    if not exp:
        return "unknown"
    import datetime
    return datetime.datetime.fromtimestamp(float(exp)).isoformat(timespec="seconds")
