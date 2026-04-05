#!/usr/bin/env python3
"""Extract Nest/Google auth state from a Firefox HAR into config/tokens.json.

Usage:
  python utilities/ff_har_tokens.py --har "home.nest.com_relogin_main_Archive [26-05-20 16-48-49].har"
  python utilities/ff_har_tokens.py --har <file.har> --out config/tokens.json
"""

from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse


_DEFAULT_SECRET_PATH = "/run/secrets/nest_key"


def _header_value(headers: list[dict[str, Any]], name: str) -> str:
    needle = name.lower()
    for header in headers or []:
        if str(header.get("name", "")).lower() == needle:
            return str(header.get("value", ""))
    return ""


def _parse_json_text(text: str) -> dict[str, Any]:
    if not text:
        return {}
    try:
        return json.loads(text)
    except Exception:
        return {}


def _started_ts(entry: dict[str, Any]) -> float:
    raw = str(entry.get("startedDateTime") or "")
    if not raw:
        return 0.0
    try:
        return datetime.fromisoformat(raw).timestamp()
    except Exception:
        return 0.0


def _select_latest(entries: list[dict[str, Any]]) -> dict[str, Any] | None:
    if not entries:
        return None
    return max(entries, key=_started_ts)


def _extract_issue_token_entry(entries: list[dict[str, Any]]) -> dict[str, Any] | None:
    matches: list[dict[str, Any]] = []
    for entry in entries:
        req = entry.get("request", {})
        url = str(req.get("url", ""))
        if "accounts.google.com/o/oauth2/iframerpc" not in url:
            continue
        parsed = urlparse(url)
        action = parse_qs(parsed.query).get("action", [""])[0]
        if action != "issueToken":
            continue
        matches.append(entry)
    return _select_latest(matches)


def _extract_issue_jwt_entry(entries: list[dict[str, Any]]) -> dict[str, Any] | None:
    matches: list[dict[str, Any]] = []
    for entry in entries:
        req = entry.get("request", {})
        if req.get("method") != "POST":
            continue
        url = str(req.get("url", ""))
        if "nestauthproxyservice-pa.googleapis.com/v1/issue_jwt" in url:
            matches.append(entry)
    return _select_latest(matches)


def _extract_app_launch_entry(entries: list[dict[str, Any]]) -> dict[str, Any] | None:
    matches: list[dict[str, Any]] = []
    for entry in entries:
        req = entry.get("request", {})
        if req.get("method") != "POST":
            continue
        url = str(req.get("url", ""))
        if "/api/0.1/user/" in url and url.endswith("/app_launch"):
            matches.append(entry)
    return _select_latest(matches)


def _iso_to_unix(iso_str: str) -> float:
    if not iso_str:
        return 0.0
    raw = iso_str.strip()
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(raw).timestamp()
    except Exception:
        return 0.0


def extract_tokens(har_path: Path) -> dict[str, Any]:
    with har_path.open("r", encoding="utf-8") as fh:
        har = json.load(fh)

    entries = list(har.get("log", {}).get("entries", []))
    issue_entry = _extract_issue_token_entry(entries)
    if not issue_entry:
        raise RuntimeError("Could not find issueToken request in HAR")

    issue_req = issue_entry.get("request", {})
    issue_url = str(issue_req.get("url", ""))
    issue_cookie_header = _header_value(issue_req.get("headers", []), "Cookie")
    if not issue_cookie_header:
        raise RuntimeError("issueToken request did not include a Cookie header")

    query = parse_qs(urlparse(issue_url).query)

    out: dict[str, Any] = {
        "issue_token": issue_url,
        "google_cookie_header": issue_cookie_header,
        "issue_token_client_id": query.get("client_id", [""])[0],
        "issue_token_login_hint": query.get("login_hint", [""])[0],
        "issue_token_origin": query.get("origin", [""])[0],
        "issue_token_scope": query.get("scope", [""])[0],
        "issue_token_ss_domain": query.get("ss_domain", [""])[0],
        "issue_token_auto": query.get("auto", [""])[0],
        "issue_token_include_granted_scopes": query.get("include_granted_scopes", [""])[0],
        "source_har": str(har_path),
        "extracted_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }

    issue_resp = issue_entry.get("response", {})
    issue_resp_json = _parse_json_text(str(issue_resp.get("content", {}).get("text", "")))
    google_bearer = str(issue_resp_json.get("access_token", "") or "")
    if google_bearer:
        out["google_bearer_token"] = google_bearer

    expires_in = issue_resp_json.get("expires_in")
    if expires_in is not None:
        try:
            started = _started_ts(issue_entry)
            out["google_bearer_expiration"] = float(started + float(expires_in))
        except Exception:
            pass

    issue_jwt_entry = _extract_issue_jwt_entry(entries)
    if issue_jwt_entry:
        jwt_json = _parse_json_text(str(issue_jwt_entry.get("response", {}).get("content", {}).get("text", "")))
        jwt_token = str(jwt_json.get("jwt", "") or "")
        if jwt_token:
            out["access_token"] = jwt_token

        claims = jwt_json.get("claims", {}) if isinstance(jwt_json, dict) else {}
        expiry = str(claims.get("expirationTime", "") or "")
        exp_unix = _iso_to_unix(expiry)
        if exp_unix > 0:
            out["cache_expiration"] = exp_unix

        userid = str(
            claims.get("subject", {})
            .get("nestId", {})
            .get("id", "")
            or ""
        )
        if userid:
            out["userid"] = userid
            out["user_id"] = userid
            out["user"] = f"user.{userid}"

    app_launch_entry = _extract_app_launch_entry(entries)
    if app_launch_entry:
        launch_json = _parse_json_text(str(app_launch_entry.get("response", {}).get("content", {}).get("text", "")))
        transport_url = str(
            launch_json.get("service_urls", {})
            .get("urls", {})
            .get("transport_url", "")
            or ""
        )
        if transport_url:
            out["transport_url"] = transport_url

        if not out.get("user"):
            for bucket in launch_json.get("updated_buckets", []):
                object_key = str(bucket.get("object_key", "") or "")
                if object_key.startswith("user."):
                    out["user"] = object_key
                    maybe_id = object_key.split(".", 1)[-1]
                    if maybe_id and maybe_id.isdigit():
                        out["userid"] = maybe_id
                        out["user_id"] = maybe_id
                    break

    return out


def _get_fernet(key: str | None = None) -> Any | None:
    if not key:
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
    import hashlib
    import base64
    derived = hashlib.sha256(key.encode("utf-8")).digest()
    fernet_key = base64.urlsafe_b64encode(derived)
    return Fernet(fernet_key)


def _get_encryption_key() -> str:
    """Return key from Docker secret file first, then NEST_TOKEN_ENCRYPTION_KEY."""
    secret_file = os.getenv("NEST_TOKEN_ENCRYPTION_KEY_FILE", _DEFAULT_SECRET_PATH)
    if secret_file:
        try:
            p = Path(secret_file)
            if p.exists() and p.is_file():
                secret = p.read_text(encoding="utf-8").strip()
                if secret:
                    return secret
        except Exception:
            pass
    return os.getenv("NEST_TOKEN_ENCRYPTION_KEY", "").strip()


def merge_write(out_path: Path, extracted: dict[str, Any], key: str | None = None) -> dict[str, Any]:
    merged: dict[str, Any] = {}
    fernet = _get_fernet(key)

    if out_path.exists():
        try:
            with out_path.open("rb") as fh:
                raw_data = fh.read()
            if raw_data:
                # Try parsing as plaintext JSON first
                try:
                    current = json.loads(raw_data.decode("utf-8"))
                except Exception:
                    # If plaintext parsing fails, attempt decryption if key is available
                    if fernet is not None:
                        decrypted = fernet.decrypt(raw_data)
                        current = json.loads(decrypted.decode("utf-8"))
                    else:
                        print("Warning: File appears encrypted but no key was provided. Cannot read existing file.")
                        current = {}
                if isinstance(current, dict):
                    merged.update(current)
        except Exception as e:
            print(f"Warning: Could not read existing file: {e}")

    for key_name, value in extracted.items():
        if value is None:
            continue
        if isinstance(value, str) and not value.strip():
            continue
        merged[key_name] = value

    out_path.parent.mkdir(parents=True, exist_ok=True)
    json_bytes = json.dumps(merged, indent=2).encode("utf-8")

    if fernet is not None:
        write_data = fernet.encrypt(json_bytes)
    else:
        write_data = json_bytes + b"\n"

    with out_path.open("wb") as fh:
        fh.write(write_data)
    return merged


def main() -> int:
    parser = argparse.ArgumentParser(description="Extract tokens.json fields from Firefox HAR")
    parser.add_argument("--har", help="Path to Firefox HAR file")
    parser.add_argument("--out", default="config/tokens.json", help="Output tokens.json path")
    parser.add_argument("--key", help="Passphrase to encrypt or decrypt the tokens file")
    parser.add_argument("--decrypt", action="store_true", help="Decrypt the tokens file back to plaintext JSON")
    args = parser.parse_args()

    if not args.decrypt and not args.har:
        parser.error("Either --har or --decrypt is required.")

    out_path = Path(args.out)

    if args.decrypt:
        if not out_path.exists():
            print(f"Error: File {out_path} does not exist.")
            return 1
        fernet = _get_fernet(args.key)
        if fernet is None:
            print("Error: --key, Docker secret file, or NEST_TOKEN_ENCRYPTION_KEY environment variable is required for decryption.")
            return 1
        try:
            with out_path.open("rb") as fh:
                raw_data = fh.read()
            if not raw_data:
                print(f"Error: File {out_path} is empty.")
                return 1
            # Check if it's already plaintext JSON
            try:
                json.loads(raw_data.decode("utf-8"))
                print(f"File {out_path} is already plaintext JSON.")
                return 0
            except Exception:
                pass

            decrypted_bytes = fernet.decrypt(raw_data)
            data = json.loads(decrypted_bytes.decode("utf-8"))

            with out_path.open("w", encoding="utf-8") as fh:
                json.dump(data, fh, indent=2)
                fh.write("\n")
            print(f"Successfully decrypted {out_path} to plaintext JSON.")
            return 0
        except Exception as exc:
            print(f"Error decrypting file: {exc}")
            return 1

    har_path = Path(args.har)
    extracted = extract_tokens(har_path)
    merged = merge_write(out_path, extracted, key=args.key)

    print(f"Wrote {out_path}")
    print(f"  issue_token: {'yes' if merged.get('issue_token') else 'no'}")
    print(f"  google_cookie_header: {'yes' if merged.get('google_cookie_header') else 'no'}")
    print(f"  userid: {merged.get('userid', '')}")
    print(f"  transport_url: {'yes' if merged.get('transport_url') else 'no'}")
    print(f"  access_token: {'yes' if merged.get('access_token') else 'no'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
