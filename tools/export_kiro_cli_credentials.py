#!/usr/bin/env python3
"""
Export official Kiro CLI login state to kiro-rs credentials JSON.

Examples:
  python tools/export_kiro_cli_credentials.py \
      --sqlite-path ~/.local/share/kiro-cli/data.sqlite3 \
      --output credentials.cli.json

  py tools/export_kiro_cli_credentials.py \
      --wsl-distro Debian \
      --output credentials.cli.json \
      --region us-east-1

  py tools/export_kiro_cli_credentials.py \
      --wsl-distro Debian \
      --merge-into credentials.json \
      --priority 0
"""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
import subprocess
import sys
from pathlib import Path
from typing import Any

from login_kiro_cli import (
    build_opener,
    builder_id_placeholder_profile_arn,
    resolve_profile_arn,
)


DEFAULT_WSL_DB_PATH = "/root/.local/share/kiro-cli/data.sqlite3"
TOKEN_KEY_SUFFIX = ":token"
DEVICE_KEY_SUFFIX = ":device-registration"


def fail(message: str) -> "NoReturn":
    raise SystemExit(message)


def default_sqlite_candidates() -> list[Path]:
    candidates: list[Path] = []
    home = Path.home()

    for candidate in (
        home / ".local" / "share" / "kiro-cli" / "data.sqlite3",
        home / "Library" / "Application Support" / "kiro-cli" / "data.sqlite3",
    ):
        candidates.append(candidate)

    if os.name == "nt":
        for env_key in ("LOCALAPPDATA", "APPDATA"):
            base = os.environ.get(env_key)
            if base:
                candidates.append(Path(base) / "kiro-cli" / "data.sqlite3")

    seen: set[Path] = set()
    existing: list[Path] = []
    for candidate in candidates:
        if candidate in seen:
            continue
        seen.add(candidate)
        if candidate.exists():
            existing.append(candidate)
    return existing


def read_auth_kv_from_sqlite(db_path: Path) -> dict[str, str]:
    if not db_path.exists():
        fail(f"SQLite file not found: {db_path}")

    conn = sqlite3.connect(str(db_path))
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT key, value FROM auth_kv")
        rows = cursor.fetchall()
    except sqlite3.Error as exc:
        fail(f"Failed to read auth_kv from {db_path}: {exc}")
    finally:
        conn.close()

    if not rows:
        fail(f"No auth_kv rows found in {db_path}")

    return {key: value for key, value in rows}


def read_auth_kv_from_wsl(distro: str, user: str, db_path: str) -> dict[str, str]:
    script = r"""
import json
import sqlite3
import sys

db_path = sys.argv[1]
conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
try:
    cursor = conn.cursor()
    cursor.execute("SELECT key, value FROM auth_kv")
    rows = cursor.fetchall()
finally:
    conn.close()

print(json.dumps({key: value for key, value in rows}))
"""

    cmd = [
        "wsl.exe",
        "-d",
        distro,
        "--user",
        user,
        "--",
        "python3",
        "-c",
        script,
        db_path,
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True)
    if proc.returncode != 0:
        stderr = proc.stderr.strip() or proc.stdout.strip() or "unknown error"
        fail(
            "Failed to read Kiro CLI state from WSL. "
            f"distro={distro} user={user} path={db_path}\n{stderr}"
        )

    try:
        data = json.loads(proc.stdout)
    except json.JSONDecodeError as exc:
        fail(f"WSL returned invalid JSON: {exc}")

    if not isinstance(data, dict) or not data:
        fail(f"No auth_kv rows found in WSL database: {db_path}")

    return {str(key): str(value) for key, value in data.items()}


def find_entry(auth_kv: dict[str, str], suffix: str, label: str) -> dict[str, Any]:
    matches = [value for key, value in auth_kv.items() if key.endswith(suffix)]
    if not matches:
        available = ", ".join(sorted(auth_kv)) or "<empty>"
        fail(f"Missing {label} entry in auth_kv. Available keys: {available}")

    try:
        data = json.loads(matches[0])
    except json.JSONDecodeError as exc:
        fail(f"Invalid JSON in {label} entry: {exc}")

    if not isinstance(data, dict):
        fail(f"{label} entry is not a JSON object")

    return data


def build_credential(args: argparse.Namespace, auth_kv: dict[str, str]) -> dict[str, Any]:
    device_registration = find_entry(auth_kv, DEVICE_KEY_SUFFIX, "device-registration")
    token = find_entry(auth_kv, TOKEN_KEY_SUFFIX, "token")
    token_region = str(token.get("region") or args.region or "us-east-1")
    resolved_profile_arn = args.profile_arn

    if not resolved_profile_arn and not args.skip_profile_arn:
        access_token = token.get("access_token")
        if access_token:
            try:
                opener = build_opener(args.proxy)
                resolved_profile_arn = resolve_profile_arn(
                    opener,
                    token_region,
                    str(access_token),
                    args.timeout,
                )
            except SystemExit as exc:
                print(
                    f"Warning: failed to resolve profileArn automatically: {exc}",
                    file=sys.stderr,
                )

    if not resolved_profile_arn and not token.get("start_url"):
        resolved_profile_arn = builder_id_placeholder_profile_arn(token_region)

    credential = {
        "accessToken": token.get("access_token"),
        "refreshToken": token.get("refresh_token"),
        "expiresAt": token.get("expires_at"),
        "authMethod": "idc",
        "clientId": device_registration.get("client_id"),
        "clientSecret": device_registration.get("client_secret"),
    }

    required_fields = (
        "accessToken",
        "refreshToken",
        "expiresAt",
        "clientId",
        "clientSecret",
    )
    missing = [field for field in required_fields if not credential.get(field)]
    if missing:
        fail(
            "Kiro CLI state is incomplete. Missing fields: "
            + ", ".join(missing)
        )

    optional_fields = {
        "profileArn": resolved_profile_arn,
        "region": args.region or token_region,
        "authRegion": args.auth_region,
        "apiRegion": args.api_region,
        "machineId": args.machine_id,
        "email": args.email,
    }
    for key, value in optional_fields.items():
        if value:
            credential[key] = value

    if args.priority:
        credential["priority"] = args.priority

    return credential


def load_existing_credentials(path: Path) -> list[dict[str, Any]]:
    if not path.exists() or not path.read_text(encoding="utf-8").strip():
        return []

    data = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(data, dict):
        return [data]
    if isinstance(data, list):
        return data
    fail(f"Unsupported credentials JSON structure in {path}")


def merge_credentials(existing: list[dict[str, Any]], incoming: dict[str, Any]) -> list[dict[str, Any]]:
    refresh_token = incoming.get("refreshToken")
    for index, item in enumerate(existing):
        if item.get("refreshToken") == refresh_token and refresh_token:
            merged = dict(item)
            merged.update(incoming)
            existing[index] = merged
            return existing

    existing.append(incoming)
    return existing


def write_output(
    args: argparse.Namespace,
    credential: dict[str, Any],
) -> None:
    if args.merge_into:
        merge_path = Path(args.merge_into)
        merged = merge_credentials(load_existing_credentials(merge_path), credential)
        merge_path.write_text(
            json.dumps(merged, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
        print(f"Merged Kiro CLI credential into {merge_path}")
        return

    content = json.dumps(credential, ensure_ascii=False, indent=2) + "\n"
    if args.output:
        output_path = Path(args.output)
        output_path.write_text(content, encoding="utf-8")
        print(f"Wrote exported credential to {output_path}")
        return

    sys.stdout.write(content)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Export official Kiro CLI login state as kiro-rs credentials JSON.",
    )
    source_group = parser.add_mutually_exclusive_group()
    source_group.add_argument(
        "--sqlite-path",
        help="Read Kiro CLI state from a local SQLite file.",
    )
    source_group.add_argument(
        "--wsl-distro",
        help="Read Kiro CLI state from a WSL distro via wsl.exe.",
    )
    parser.add_argument(
        "--wsl-user",
        default="root",
        help="WSL user for --wsl-distro. Default: root",
    )
    parser.add_argument(
        "--wsl-db-path",
        default=DEFAULT_WSL_DB_PATH,
        help=f"SQLite path inside WSL. Default: {DEFAULT_WSL_DB_PATH}",
    )
    parser.add_argument("--output", help="Write the exported credential JSON to this path.")
    parser.add_argument(
        "--merge-into",
        help="Merge or replace by refreshToken into an existing credentials.json file.",
    )
    parser.add_argument("--profile-arn", help="Optional profileArn override.")
    parser.add_argument("--region", help="Optional region value.")
    parser.add_argument("--auth-region", help="Optional authRegion value.")
    parser.add_argument("--api-region", help="Optional apiRegion value.")
    parser.add_argument("--machine-id", help="Optional machineId value.")
    parser.add_argument("--email", help="Optional email value.")
    parser.add_argument("--proxy", help="Optional HTTP/HTTPS proxy URL for resolving profileArn.")
    parser.add_argument("--timeout", type=int, default=30, help="HTTP timeout in seconds. Default: 30")
    parser.add_argument(
        "--skip-profile-arn",
        action="store_true",
        help="Do not try resolving profileArn from the current Kiro CLI access token.",
    )
    parser.add_argument(
        "--priority",
        type=int,
        default=0,
        help="Optional priority value for multi-credential mode. Default: 0",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    if args.sqlite_path:
        auth_kv = read_auth_kv_from_sqlite(Path(args.sqlite_path).expanduser())
    elif args.wsl_distro:
        auth_kv = read_auth_kv_from_wsl(args.wsl_distro, args.wsl_user, args.wsl_db_path)
    else:
        candidates = default_sqlite_candidates()
        if not candidates:
            fail(
                "Could not auto-detect Kiro CLI state. "
                "Use --sqlite-path or --wsl-distro explicitly."
            )
        auth_kv = read_auth_kv_from_sqlite(candidates[0])

    credential = build_credential(args, auth_kv)
    write_output(args, credential)


if __name__ == "__main__":
    main()
