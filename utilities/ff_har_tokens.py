#!/usr/bin/env python3
"""Extract Google Home / Nest auth state from a Firefox HAR into config/tokens.json.

Supports both:
  1. New ``home.google.com`` HAR captures (Foyer SAPISIDHASH cookies + RotateCookies
     keep-alive + accounts.google.com cookies for Nest Observe JWT exchange).
  2. Legacy ``home.nest.com`` HAR captures (iframerpc issueToken + issue_jwt + app_launch).

Usage:
  python utilities/ff_har_tokens.py --har "home.google.com_Archive.har"
  python utilities/ff_har_tokens.py --har <file.har> --out config/tokens.json --verify
"""

from __future__ import annotations

import argparse
import base64
import json
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse


_DEFAULT_SECRET_PATH = "/run/secrets/nest_key"

_ROTATING_COOKIE_NAMES: frozenset[str] = frozenset({
    "SIDCC",
    "__Secure-1PSIDCC",
    "__Secure-3PSIDCC",
    "__Secure-1PSIDTS",
    "__Secure-3PSIDTS",
    "NID",
})


def _header_value(headers: list[dict[str, Any]], name: str) -> str:
    needle = name.lower()
    values = [
        str(header.get("value", ""))
        for header in (headers or [])
        if str(header.get("name", "")).lower() == needle
    ]
    if not values:
        return ""
    if needle == "cookie":
        return "; ".join(v for v in values if v)
    return values[-1]


def _parse_cookie_header(cookie_header: str) -> dict[str, str]:
    out: dict[str, str] = {}
    for raw_part in (cookie_header or "").split(";"):
        part = raw_part.strip()
        if not part or "=" not in part:
            continue
        k, v = part.split("=", 1)
        k = k.strip()
        v = v.strip()
        if k:
            out[k] = v
    return out


def _apply_entry_set_cookies(
    cookie_map: dict[str, str],
    entry: dict[str, Any],
    *,
    only_rotating: bool = True,
) -> dict[str, str]:
    updated = dict(cookie_map)
    for header in entry.get("response", {}).get("headers", []) or []:
        if str(header.get("name", "")).lower() != "set-cookie":
            continue
        raw = str(header.get("value", ""))
        first = raw.split(";", 1)[0]
        if "=" not in first:
            continue
        k, v = first.split("=", 1)
        k = k.strip()
        v = v.strip()
        if not k or not v:
            continue
        if only_rotating and k not in _ROTATING_COOKIE_NAMES:
            continue
        updated[k] = v
    return updated


def _format_cookie_header(cookie_map: dict[str, str]) -> str:
    return "; ".join(f"{k}={v}" for k, v in cookie_map.items())


def _response_text(entry: dict[str, Any]) -> str:
    content = entry.get("response", {}).get("content", {}) or {}
    text = str(content.get("text", "") or "")
    if not text:
        return ""
    if str(content.get("encoding", "")).lower() == "base64":
        try:
            return base64.b64decode(text).decode("utf-8", errors="replace")
        except Exception:
            return ""
    return text


def _parse_json_text(text: str) -> Any:
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


def _extract_foyer_post_entry(entries: list[dict[str, Any]]) -> dict[str, Any] | None:
    matches: list[dict[str, Any]] = []
    for entry in entries:
        req = entry.get("request", {})
        resp = entry.get("response", {})
        if req.get("method") != "POST":
            continue
        url = str(req.get("url", ""))
        if "googlehomefoyer-pa.clients6.google.com" not in url:
            continue
        if int(resp.get("status", 0) or 0) != 200:
            continue
        if not _header_value(req.get("headers", []), "Cookie"):
            continue
        matches.append(entry)
    return _select_latest(matches)


def _extract_foyer_link_state_entry(entries: list[dict[str, Any]]) -> dict[str, Any] | None:
    matches: list[dict[str, Any]] = []
    for entry in entries:
        req = entry.get("request", {})
        resp = entry.get("response", {})
        if req.get("method") != "POST":
            continue
        url = str(req.get("url", ""))
        if "UsersService/GetNestAccountLinkState" in url and int(resp.get("status", 0) or 0) == 200:
            matches.append(entry)
    return _select_latest(matches)


def _extract_rotate_cookies_entry(entries: list[dict[str, Any]]) -> dict[str, Any] | None:
    matches: list[dict[str, Any]] = []
    for entry in entries:
        req = entry.get("request", {})
        resp = entry.get("response", {})
        if req.get("method") != "POST":
            continue
        url = str(req.get("url", ""))
        if "accounts.google.com/RotateCookies" not in url:
            continue
        if int(resp.get("status", 0) or 0) != 200:
            continue
        if not _header_value(req.get("headers", []), "Cookie"):
            continue
        matches.append(entry)
    return _select_latest(matches)


def _extract_rotate_cookies_page_entry(entries: list[dict[str, Any]]) -> dict[str, Any] | None:
    matches: list[dict[str, Any]] = []
    for entry in entries:
        req = entry.get("request", {})
        resp = entry.get("response", {})
        if req.get("method") != "GET":
            continue
        url = str(req.get("url", ""))
        if "accounts.google.com/RotateCookiesPage" not in url:
            continue
        if int(resp.get("status", 0) or 0) != 200:
            continue
        matches.append(entry)
    return _select_latest(matches)


def _extract_accounts_cookie_entry(entries: list[dict[str, Any]]) -> dict[str, Any] | None:
    # Prefer RotateCookies (most complete, most recent authenticated accounts.google.com call)
    rot = _extract_rotate_cookies_entry(entries)
    if rot:
        return rot

    # Fallback: RotateCookiesPage or ListAccounts with 200 status and Cookie header
    matches: list[dict[str, Any]] = []
    for entry in entries:
        req = entry.get("request", {})
        resp = entry.get("response", {})
        url = str(req.get("url", ""))
        if "accounts.google.com/" not in url:
            continue
        if int(resp.get("status", 0) or 0) != 200:
            continue
        if not any(p in url for p in ("/RotateCookiesPage", "/ListAccounts")):
            continue
        if not _header_value(req.get("headers", []), "Cookie"):
            continue
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


def _extract_home_google_tokens(har_path: Path, entries: list[dict[str, Any]]) -> dict[str, Any]:
    foyer_entry = _extract_foyer_post_entry(entries)
    if not foyer_entry:
        raise RuntimeError("Could not find an authenticated googlehomefoyer-pa POST request in HAR")

    foyer_req = foyer_entry.get("request", {})
    foyer_headers = foyer_req.get("headers", [])
    foyer_cookie_map = _parse_cookie_header(_header_value(foyer_headers, "Cookie"))
    if not foyer_cookie_map:
        raise RuntimeError("googlehomefoyer-pa request did not include a Cookie header")
    foyer_cookie_map = _apply_entry_set_cookies(foyer_cookie_map, foyer_entry, only_rotating=True)

    foyer_api_key = _header_value(foyer_headers, "X-Goog-Api-Key")
    foyer_authuser = _header_value(foyer_headers, "X-Goog-AuthUser") or "0"
    foyer_client_env = _header_value(foyer_headers, "x-foyer-client-environment")
    foyer_user_agent = _header_value(foyer_headers, "User-Agent")

    # Accounts cookies (for RotateCookies and accounts.google.com iframerpc OAuth)
    acct_entry = _extract_accounts_cookie_entry(entries)
    if acct_entry:
        acct_cookie_map = _parse_cookie_header(
            _header_value(acct_entry.get("request", {}).get("headers", []), "Cookie")
        )
        acct_cookie_map = _apply_entry_set_cookies(acct_cookie_map, acct_entry, only_rotating=True)
        # Ensure rotating cookies from the latest foyer entry (if newer) are carried over too
        if _started_ts(foyer_entry) > _started_ts(acct_entry):
            for cname in _ROTATING_COOKIE_NAMES:
                if cname in foyer_cookie_map:
                    acct_cookie_map[cname] = foyer_cookie_map[cname]
    else:
        acct_cookie_map = dict(foyer_cookie_map)

    rotate_body = ""
    rotate_referer = ""
    rot_entry = _extract_rotate_cookies_entry(entries)
    if rot_entry:
        rotate_body = str(rot_entry.get("request", {}).get("postData", {}).get("text", "") or "").strip()
        rotate_referer = _header_value(rot_entry.get("request", {}).get("headers", []), "Referer")
    if not rotate_body:
        rot_page = _extract_rotate_cookies_page_entry(entries)
        if rot_page:
            rotate_referer = str(rot_page.get("request", {}).get("url", "") or "")
            page_html = _response_text(rot_page)
            m = re.search(r"init\(\s*'([^']+)'\s*,\s*([0-9\.]+)", page_html)
            if m:
                nonce = m.group(1)
                pid = int(float(m.group(2)))
                rotate_body = json.dumps([pid, nonce], separators=(",", ":"))

    out: dict[str, Any] = {
        "auth_mode": "home_google",
        "foyer_cookie_header": _format_cookie_header(foyer_cookie_map),
        "google_cookie_header": _format_cookie_header(acct_cookie_map),
        "foyer_api_key": foyer_api_key,
        "foyer_authuser": foyer_authuser,
        "foyer_client_environment": foyer_client_env,
        "foyer_user_agent": foyer_user_agent,
        "rotate_cookies_body": rotate_body,
        "rotate_cookies_referer": rotate_referer,
        "source_har": str(har_path),
        "extracted_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }

    # Extract Nest link state (user.123456, USER_...) if present in HAR
    link_entry = _extract_foyer_link_state_entry(entries)
    if link_entry:
        link_data = _parse_json_text(_response_text(link_entry))
        if isinstance(link_data, list) and len(link_data) >= 2 and isinstance(link_data[1], list):
            ids = link_data[1]
            if len(ids) >= 1 and isinstance(ids[0], str) and ids[0]:
                out["user"] = ids[0]
                if "." in ids[0]:
                    uid = ids[0].split(".", 1)[-1]
                    if uid:
                        out["userid"] = uid
                        out["user_id"] = uid
            if len(ids) >= 2 and isinstance(ids[1], str) and ids[1]:
                out["nest_user_id"] = ids[1]

    return out


def _extract_legacy_nest_tokens(har_path: Path, entries: list[dict[str, Any]]) -> dict[str, Any]:
    issue_entry = _extract_issue_token_entry(entries)
    if not issue_entry:
        raise RuntimeError(
            "Could not find home.google.com Foyer POST requests or legacy Nest issueToken request in HAR"
        )

    issue_req = issue_entry.get("request", {})
    issue_url = str(issue_req.get("url", ""))
    issue_cookie_header = _header_value(issue_req.get("headers", []), "Cookie")
    if not issue_cookie_header:
        raise RuntimeError("issueToken request did not include a Cookie header")

    query = parse_qs(urlparse(issue_url).query)

    out: dict[str, Any] = {
        "auth_mode": "legacy_nest",
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

    issue_resp_json = _parse_json_text(_response_text(issue_entry))
    if isinstance(issue_resp_json, dict):
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
        jwt_json = _parse_json_text(_response_text(issue_jwt_entry))
        if isinstance(jwt_json, dict):
            jwt_token = str(jwt_json.get("jwt", "") or "")
            if jwt_token:
                out["access_token"] = jwt_token

            claims = jwt_json.get("claims", {})
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
        launch_json = _parse_json_text(_response_text(app_launch_entry))
        if isinstance(launch_json, dict):
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


def extract_tokens(har_path: Path) -> dict[str, Any]:
    with har_path.open("r", encoding="utf-8") as fh:
        har = json.load(fh)

    entries = list(har.get("log", {}).get("entries", []))
    if _extract_foyer_post_entry(entries) is not None:
        return _extract_home_google_tokens(har_path, entries)
    return _extract_legacy_nest_tokens(har_path, entries)


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
    derived = hashlib.sha256(key.encode("utf-8")).digest()
    fernet_key = base64.urlsafe_b64encode(derived)
    return Fernet(fernet_key)


def _get_encryption_key() -> str:
    """Return key from Docker secret file, NEST_TOKEN_ENCRYPTION_KEY env, or repo .env."""
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
    env_val = os.getenv("NEST_TOKEN_ENCRYPTION_KEY", "").strip()
    if env_val:
        return env_val
    # Fallback: read NEST_TOKEN_ENCRYPTION_KEY from repo .env (matches docker-compose.yml)
    dotenv_path = Path(__file__).resolve().parent.parent / ".env"
    if dotenv_path.exists() and dotenv_path.is_file():
        try:
            for line in dotenv_path.read_text(encoding="utf-8").splitlines():
                stripped = line.strip()
                if not stripped or stripped.startswith("#") or "=" not in stripped:
                    continue
                k, v = stripped.split("=", 1)
                if k.strip() == "NEST_TOKEN_ENCRYPTION_KEY":
                    val = v.strip().strip('"').strip("'")
                    if val:
                        return val
        except Exception:
            pass
    return ""


def merge_write(out_path: Path, extracted: dict[str, Any], key: str | None = None) -> dict[str, Any]:
    merged: dict[str, Any] = {}
    fernet = _get_fernet(key)

    if out_path.exists():
        try:
            with out_path.open("rb") as fh:
                raw_data = fh.read()
            if raw_data:
                try:
                    current = json.loads(raw_data.decode("utf-8"))
                except Exception:
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

    # When switching to home_google mode from an old legacy tokens.json,
    # clear stale Nest JWT/bearer/issue_token so NestClient performs a clean
    # home.google.com exchange on next startup.
    if extracted.get("auth_mode") == "home_google":
        for stale_key in (
            "access_token",
            "google_bearer_token",
            "google_bearer_expiration",
            "cache_expiration",
            "issue_token",
        ):
            merged[stale_key] = "" if isinstance(merged.get(stale_key, ""), str) else 0.0

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


def _verify_extracted(out_path: Path, key: str | None = None, *, verify_observe: bool = False) -> bool:
    """Optionally live-verify extracted tokens via Google Home Foyer (and optionally legacy Observe)."""
    repo_root = Path(__file__).resolve().parent.parent
    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))
    effective_key = key or _get_encryption_key()
    if effective_key:
        os.environ["NEST_TOKEN_ENCRYPTION_KEY"] = effective_key

    from src.nest_client import NestClient

    print("\nVerifying extracted credentials live ...")
    client = NestClient(token_file=str(out_path), foyer_only=not verify_observe)
    if client.foyer_client is not None:
        link = client.foyer_client.get_nest_account_link_state()
        print(f"  Foyer GetNestAccountLinkState: user={link.get('user', '')} nest_user_id={link.get('nest_user_id', '')}")
        hg = client.foyer_client.get_home_graph(force_refresh=True)
        print(f"  Foyer GetHomeGraph: {len(hg.get('structures', []))} structure(s), {len(hg.get('devices', {}))} device(s)")
        rotated = client.rotate_google_cookies(force=True)
        print(f"  RotateCookies: {'ok' if rotated else 'skipped/failed'}")
    if verify_observe:
        print(f"  Nest Observe JWT: {'ok' if client.access_token else 'missing'} (userid={client._userid}, expires={client.token_expiry:.0f})")
    return bool(client.foyer_client is not None or client.access_token)


def main() -> int:
    parser = argparse.ArgumentParser(description="Extract tokens.json fields from Firefox HAR (home.google.com or legacy home.nest.com)")
    parser.add_argument("--har", help="Path to Firefox HAR file")
    parser.add_argument("--out", default="config/tokens.json", help="Output tokens.json path")
    parser.add_argument("--key", help="Passphrase to encrypt or decrypt the tokens file")
    parser.add_argument("--decrypt", action="store_true", help="Decrypt the tokens file back to plaintext JSON")
    parser.add_argument("--verify", action="store_true", help="Live-verify extracted credentials via Google Home Foyer after writing tokens.json")
    parser.add_argument("--verify-observe", action="store_true", help="Also verify legacy Nest Observe JWT exchange (implies --verify)")
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

    print(f"Wrote {out_path} (auth_mode={merged.get('auth_mode', 'legacy_nest')})")
    print(f"  foyer_cookie_header: {'yes' if merged.get('foyer_cookie_header') else 'no'}")
    print(f"  google_cookie_header: {'yes' if merged.get('google_cookie_header') else 'no'}")
    print(f"  foyer_api_key: {'yes' if merged.get('foyer_api_key') else 'no'}")
    print(f"  rotate_cookies_body: {'yes' if merged.get('rotate_cookies_body') else 'no'}")
    print(f"  issue_token: {'yes' if merged.get('issue_token') else 'no'}")
    print(f"  userid: {merged.get('userid', '')}")
    print(f"  nest_user_id: {merged.get('nest_user_id', '')}")
    print(f"  transport_url: {'yes' if merged.get('transport_url') else 'no'}")
    print(f"  access_token: {'yes' if merged.get('access_token') else 'no'}")

    if args.verify or args.verify_observe:
        ok = _verify_extracted(out_path, key=args.key, verify_observe=bool(args.verify_observe))
        return 0 if ok else 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
