#!/usr/bin/env python3
"""
Start a Kiro CLI-compatible device login flow and export kiro-rs credentials.

Examples:
  py tools/login_kiro_cli.py

  py tools/login_kiro_cli.py --open-browser

  py tools/login_kiro_cli.py --merge-into credentials.json --priority 0 --region us-east-1

  py tools/login_kiro_cli.py --mode identity-center --start-url https://your-domain.awsapps.com/start

  py tools/login_kiro_cli.py --no-poll
"""

from __future__ import annotations

import argparse
import json
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import webbrowser
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from uuid import uuid4


DEFAULT_REGION = "us-east-1"
BUILDER_ID_START_URL = "https://view.awsapps.com/start"
DEFAULT_CLIENT_NAME = "Kiro CLI"
LOGIN_MODE_BUILDER_ID = "builder-id"
LOGIN_MODE_IDENTITY_CENTER = "identity-center"
BUILDER_ID_PROFILE_ACCOUNT_ID = "638616132270"
BUILDER_ID_PROFILE_ID = "AAAACCCCXXXX"
CLI_RUNTIME_SDK_VERSION = "aws-sdk-rust/1.3.14"
CLI_RUNTIME_API_VERSION = "api/codewhispererruntime/0.1.14474"
CLI_RUNTIME_LANG_VERSION = "lang/rust/1.92.0"
CLI_APP_VERSION = "2.1.1"
DEFAULT_SCOPES = [
    "codewhisperer:completions",
    "codewhisperer:analysis",
    "codewhisperer:conversations",
]
DEVICE_GRANT_TYPE = "urn:ietf:params:oauth:grant-type:device_code"
TOKEN_KEY = "token"
CREDENTIAL_KEY = "credential"


def fail(message: str) -> "NoReturn":
    raise SystemExit(message)


def build_opener(proxy_url: str | None) -> urllib.request.OpenerDirector:
    if not proxy_url:
        return urllib.request.build_opener()

    parsed = urllib.parse.urlparse(proxy_url)
    if not parsed.scheme:
        fail(f"Invalid proxy URL: {proxy_url}")

    return urllib.request.build_opener(
        urllib.request.ProxyHandler(
            {
                "http": proxy_url,
                "https": proxy_url,
            }
        )
    )


def post_json(
    opener: urllib.request.OpenerDirector,
    url: str,
    payload: dict[str, Any],
    headers: dict[str, str],
    timeout: int,
) -> tuple[int, dict[str, Any]]:
    request = urllib.request.Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        headers=headers,
        method="POST",
    )

    try:
        with opener.open(request, timeout=timeout) as response:
            body = response.read().decode("utf-8")
            return response.status, json.loads(body or "{}")
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        try:
            payload = json.loads(body)
        except json.JSONDecodeError:
            payload = {"message": body}
        return exc.code, payload
    except urllib.error.URLError as exc:
        fail(f"Network error while calling {url}: {exc}")


def build_oidc_base_url(region: str) -> str:
    return f"https://oidc.{region}.amazonaws.com"


def builder_id_placeholder_profile_arn(region: str) -> str:
    return (
        f"arn:aws:codewhisperer:{region}:{BUILDER_ID_PROFILE_ACCOUNT_ID}"
        f":profile/{BUILDER_ID_PROFILE_ID}"
    )


def cli_runtime_user_agent(app_version: str = CLI_APP_VERSION) -> str:
    return (
        f"{CLI_RUNTIME_SDK_VERSION} ua/2.1 {CLI_RUNTIME_API_VERSION} os/linux "
        f"{CLI_RUNTIME_LANG_VERSION} md/appVersion-{app_version} app/AmazonQ-For-CLI"
    )


def cli_runtime_x_amz_user_agent() -> str:
    return (
        f"{CLI_RUNTIME_SDK_VERSION} ua/2.1 {CLI_RUNTIME_API_VERSION} os/linux "
        f"{CLI_RUNTIME_LANG_VERSION} m/F,C app/AmazonQ-For-CLI"
    )


def register_client(
    opener: urllib.request.OpenerDirector,
    region: str,
    client_name: str,
    timeout: int,
) -> dict[str, Any]:
    status, payload = post_json(
        opener,
        f"{build_oidc_base_url(region)}/client/register",
        {
            "clientName": client_name,
            "clientType": "public",
            "scopes": DEFAULT_SCOPES,
        },
        {
            "Content-Type": "application/json",
            "Accept": "application/json",
        },
        timeout,
    )

    if status >= 400:
        fail(f"Client registration failed: HTTP {status} {payload}")

    if not payload.get("clientId") or not payload.get("clientSecret"):
        fail(f"Client registration response is incomplete: {payload}")

    return payload


def start_device_authorization(
    opener: urllib.request.OpenerDirector,
    region: str,
    client_id: str,
    client_secret: str,
    start_url: str,
    timeout: int,
) -> dict[str, Any]:
    status, payload = post_json(
        opener,
        f"{build_oidc_base_url(region)}/device_authorization",
        {
            "clientId": client_id,
            "clientSecret": client_secret,
            "startUrl": start_url,
        },
        {
            "Content-Type": "application/json",
            "Accept": "application/json",
        },
        timeout,
    )

    if status >= 400:
        fail(f"Device authorization failed: HTTP {status} {payload}")

    required = ("deviceCode", "userCode", "verificationUri")
    missing = [field for field in required if not payload.get(field)]
    if missing:
        fail(f"Device authorization response is incomplete, missing: {', '.join(missing)}")

    return payload


def poll_for_token(
    opener: urllib.request.OpenerDirector,
    region: str,
    client_id: str,
    client_secret: str,
    device_code: str,
    interval_seconds: int,
    expires_in_seconds: int,
    timeout: int,
) -> dict[str, Any]:
    started_at = time.monotonic()
    interval = max(interval_seconds, 1)

    while True:
        if time.monotonic() - started_at >= expires_in_seconds:
            fail("Authorization timed out before token was issued")

        status, payload = request_device_token(
            opener,
            region,
            client_id,
            client_secret,
            device_code,
            timeout,
        )

        if status < 400 and payload.get("accessToken"):
            return payload

        error_code = payload.get("error")
        if error_code == "authorization_pending":
            time.sleep(interval)
            continue
        if error_code == "slow_down":
            interval += 5
            time.sleep(interval)
            continue
        if error_code == "expired_token":
            fail("Device code expired. Run the script again to get a new authorization link.")
        if error_code == "access_denied":
            fail("Authorization was denied by the user.")

        fail(f"Token polling failed: HTTP {status} {payload}")


def request_device_token(
    opener: urllib.request.OpenerDirector,
    region: str,
    client_id: str,
    client_secret: str,
    device_code: str,
    timeout: int,
) -> tuple[int, dict[str, Any]]:
    return post_json(
        opener,
        f"{build_oidc_base_url(region)}/token",
        {
            "clientId": client_id,
            "clientSecret": client_secret,
            "deviceCode": device_code,
            "grantType": DEVICE_GRANT_TYPE,
        },
        {
            "Content-Type": "application/json",
            "Accept": "application/json",
        },
        timeout,
    )


def resolve_profile_arn(
    opener: urllib.request.OpenerDirector,
    region: str,
    access_token: str,
    timeout: int,
) -> str | None:
    url = f"https://q.{region}.amazonaws.com/?origin=KIRO_CLI"
    request = urllib.request.Request(
        url,
        data=b'{"origin":"KIRO_CLI"}',
        headers={
            "Content-Type": "application/x-amz-json-1.0",
            "Accept": "*/*",
            "Authorization": f"Bearer {access_token}",
            "x-amz-target": "AmazonCodeWhispererService.ListAvailableProfiles",
            "x-amz-user-agent": cli_runtime_x_amz_user_agent(),
            "User-Agent": cli_runtime_user_agent(),
        },
        method="POST",
    )

    try:
        with opener.open(request, timeout=timeout) as response:
            payload = json.loads(response.read().decode("utf-8") or "{}")
    except urllib.error.HTTPError as exc:
        if exc.code in (401, 403):
            return None
        body = exc.read().decode("utf-8", errors="replace")
        fail(f"ListAvailableProfiles failed: HTTP {exc.code} {body}")
    except urllib.error.URLError as exc:
        fail(f"Network error while resolving profileArn: {exc}")

    for item in payload.get("profiles", []):
        profile_arn = item.get("profileArn") or item.get("arn")
        if profile_arn:
            return str(profile_arn)

    return None


def save_session(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def load_session(path: Path) -> dict[str, Any]:
    if not path.exists():
        fail(f"Session file not found: {path}")

    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        fail(f"Invalid session JSON in {path}: {exc}")

    if not isinstance(payload, dict):
        fail(f"Session file must contain a JSON object: {path}")

    return payload


def save_session_value(path: Path, payload: dict[str, Any], key: str, value: Any) -> dict[str, Any]:
    payload[key] = value
    save_session(path, payload)
    return payload


def resolve_session_expires_at(
    session_payload: dict[str, Any],
    device_auth: dict[str, Any],
) -> str | None:
    expires_at = session_payload.get("expiresAt")
    if isinstance(expires_at, str) and expires_at:
        return expires_at

    created_at = session_payload.get("createdAt")
    if not isinstance(created_at, str) or not created_at:
        return None

    try:
        created = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
    except ValueError:
        return None

    expires_in = int(device_auth.get("expiresIn") or 600)
    return (created + timedelta(seconds=expires_in)).isoformat().replace("+00:00", "Z")


def build_credential(
    args: argparse.Namespace,
    region: str,
    registration: dict[str, Any],
    token_payload: dict[str, Any] | None,
    profile_arn: str | None,
) -> dict[str, Any]:
    credential: dict[str, Any] = {
        "authMethod": "idc",
        "clientId": registration["clientId"],
        "clientSecret": registration["clientSecret"],
        "region": region,
    }

    if args.auth_region:
        credential["authRegion"] = args.auth_region
    if args.api_region:
        credential["apiRegion"] = args.api_region
    if args.machine_id:
        credential["machineId"] = args.machine_id
    if args.email:
        credential["email"] = args.email
    if args.priority:
        credential["priority"] = args.priority

    if profile_arn:
        credential["profileArn"] = profile_arn

    if token_payload:
        access_token = token_payload.get("accessToken")
        refresh_token = token_payload.get("refreshToken")
        expires_in = token_payload.get("expiresIn")

        if not access_token or not refresh_token:
            fail(f"Token response is incomplete: {token_payload}")

        credential["accessToken"] = access_token
        credential["refreshToken"] = refresh_token

        if isinstance(expires_in, int):
            expires_at = datetime.now(UTC) + timedelta(seconds=expires_in)
            credential["expiresAt"] = expires_at.isoformat().replace("+00:00", "Z")

    return credential


def build_and_cache_credential(
    args: argparse.Namespace,
    opener: urllib.request.OpenerDirector,
    session_path: Path | None,
    session_payload: dict[str, Any] | None,
    region: str,
    registration: dict[str, Any],
    token_payload: dict[str, Any],
) -> dict[str, Any]:
    profile_arn = None
    if not args.skip_profile_arn:
        profile_arn = resolve_profile_arn(
            opener,
            region,
            token_payload["accessToken"],
            args.timeout,
        )
        if not profile_arn:
            login_mode = LOGIN_MODE_BUILDER_ID
            if isinstance(session_payload, dict):
                login_mode = str(session_payload.get("loginMode") or login_mode)
            elif getattr(args, "mode", None):
                login_mode = str(args.mode)

            if login_mode == LOGIN_MODE_BUILDER_ID:
                profile_arn = builder_id_placeholder_profile_arn(region)
            else:
                print("Warning: profileArn was not resolved automatically.", file=sys.stderr)

    credential = build_credential(args, region, registration, token_payload, profile_arn)
    if session_path and session_payload is not None:
        save_session_value(session_path, session_payload, CREDENTIAL_KEY, credential)
    return credential


def check_session_status(
    args: argparse.Namespace,
    opener: urllib.request.OpenerDirector,
    session_path: Path,
    session_payload: dict[str, Any],
    region: str,
    registration: dict[str, Any],
    device_auth: dict[str, Any],
) -> dict[str, Any]:
    expires_at = resolve_session_expires_at(session_payload, device_auth)

    cached_credential = session_payload.get(CREDENTIAL_KEY)
    if isinstance(cached_credential, dict):
        return {
            "status": "ready",
            "message": "Authorization completed and credential is ready to import.",
            "expiresAt": expires_at,
        }

    token_payload = session_payload.get(TOKEN_KEY)
    if isinstance(token_payload, dict) and token_payload.get("accessToken"):
        build_and_cache_credential(
            args,
            opener,
            session_path,
            session_payload,
            region,
            registration,
            token_payload,
        )
        return {
            "status": "ready",
            "message": "Authorization completed and credential is ready to import.",
            "expiresAt": expires_at,
        }

    interval = int(device_auth.get("interval") or 5)
    status, payload = request_device_token(
        opener,
        region,
        registration["clientId"],
        registration["clientSecret"],
        device_auth["deviceCode"],
        args.timeout,
    )

    if status < 400 and payload.get("accessToken"):
        session_payload = save_session_value(session_path, session_payload, TOKEN_KEY, payload)
        build_and_cache_credential(
            args,
            opener,
            session_path,
            session_payload,
            region,
            registration,
            payload,
        )
        return {
            "status": "ready",
            "message": "Authorization completed and credential is ready to import.",
            "expiresAt": expires_at,
        }

    error_code = payload.get("error")
    if error_code == "authorization_pending":
        return {
            "status": "pending",
            "message": "Waiting for authorization completion.",
            "retryAfterSeconds": interval,
            "expiresAt": expires_at,
        }
    if error_code == "slow_down":
        return {
            "status": "pending",
            "message": "Authorization server asked to slow down.",
            "retryAfterSeconds": interval + 5,
            "expiresAt": expires_at,
        }
    if error_code == "expired_token":
        return {
            "status": "expired",
            "message": "Device code expired. Run the script again to get a new authorization link.",
            "expiresAt": expires_at,
        }
    if error_code == "access_denied":
        return {
            "status": "denied",
            "message": "Authorization was denied by the user.",
            "expiresAt": expires_at,
        }

    return {
        "status": "error",
        "message": f"Token polling failed: HTTP {status} {payload}",
        "retryAfterSeconds": interval,
        "expiresAt": expires_at,
    }


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
    client_id = incoming.get("clientId")

    for index, item in enumerate(existing):
        if refresh_token and item.get("refreshToken") == refresh_token:
            merged = dict(item)
            merged.update(incoming)
            existing[index] = merged
            return existing
        if client_id and item.get("clientId") == client_id and item.get("authMethod") == "idc":
            merged = dict(item)
            merged.update(incoming)
            existing[index] = merged
            return existing

    existing.append(incoming)
    return existing


def write_output(args: argparse.Namespace, credential: dict[str, Any]) -> None:
    if args.merge_into:
        merge_path = Path(args.merge_into)
        merged = merge_credentials(load_existing_credentials(merge_path), credential)
        merge_path.write_text(
            json.dumps(merged, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
        print(f"Merged new Kiro CLI credential into {merge_path}")
        return

    content = json.dumps(credential, ensure_ascii=False, indent=2) + "\n"
    if args.output:
        output_path = Path(args.output)
        output_path.write_text(content, encoding="utf-8")
        print(f"Wrote Kiro CLI credential to {output_path}")
        return

    sys.stdout.write(content)


def print_authorization_instructions(device_auth: dict[str, Any]) -> None:
    verification_uri = device_auth.get("verificationUri")
    verification_complete = device_auth.get("verificationUriComplete") or verification_uri
    user_code = device_auth.get("userCode")
    expires_in = device_auth.get("expiresIn")

    print("Code:")
    print(user_code)
    print()
    print("Open this URL:")
    print(verification_complete)
    print()
    if expires_in:
        print(f"Expires in: {expires_in}s")
        print()


def build_auth_info(
    region: str,
    login_mode: str,
    device_auth: dict[str, Any],
) -> dict[str, Any]:
    expires_in = int(device_auth.get("expiresIn") or 600)
    expires_at = datetime.now(UTC) + timedelta(seconds=expires_in)
    return {
        "authUrl": device_auth.get("verificationUriComplete") or device_auth["verificationUri"],
        "verificationUri": device_auth["verificationUri"],
        "verificationUriComplete": device_auth.get("verificationUriComplete"),
        "userCode": device_auth["userCode"],
        "deviceCode": device_auth["deviceCode"],
        "interval": int(device_auth.get("interval") or 5),
        "expiresIn": expires_in,
        "expiresAt": expires_at.isoformat().replace("+00:00", "Z"),
        "region": region,
        "loginMode": login_mode,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run a Kiro CLI device login and export kiro-rs credentials.",
    )
    parser.add_argument("--region", default=DEFAULT_REGION, help=f"OIDC region. Default: {DEFAULT_REGION}")
    parser.add_argument(
        "--mode",
        choices=[LOGIN_MODE_BUILDER_ID, LOGIN_MODE_IDENTITY_CENTER],
        default=LOGIN_MODE_BUILDER_ID,
        help="Login mode. builder-id matches `kiro-cli login --license free --use-device-flow`; identity-center is for custom awsapps start URLs.",
    )
    parser.add_argument(
        "--start-url",
        help="Required when --mode identity-center. Example: https://your-domain.awsapps.com/start",
    )
    parser.add_argument(
        "--client-name",
        default=DEFAULT_CLIENT_NAME,
        help=f"OIDC client registration name. Default: {DEFAULT_CLIENT_NAME}",
    )
    parser.add_argument("--auth-region", help="Optional authRegion value for the exported credential.")
    parser.add_argument("--api-region", help="Optional apiRegion value for the exported credential.")
    parser.add_argument("--machine-id", help="Optional machineId value for the exported credential.")
    parser.add_argument("--email", help="Optional email value for the exported credential.")
    parser.add_argument("--priority", type=int, default=0, help="Optional priority in credentials.json.")
    parser.add_argument("--proxy", help="Optional HTTP/HTTPS proxy URL.")
    parser.add_argument("--timeout", type=int, default=30, help="HTTP timeout in seconds. Default: 30")
    parser.add_argument("--output", help="Write exported credential JSON to this path.")
    parser.add_argument("--merge-into", help="Merge the exported credential into an existing credentials.json.")
    parser.add_argument("--open-browser", action="store_true", help="Open the authorization URL in the default browser.")
    parser.add_argument("--no-poll", action="store_true", help="Only print the authorization URL and exit without polling for token.")
    parser.add_argument(
        "--skip-profile-arn",
        action="store_true",
        help="Do not try resolving profileArn after login.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print authorization info as JSON when used with --no-poll.",
    )
    parser.add_argument(
        "--session-output",
        help="Write login session JSON to this path so polling can be resumed later.",
    )
    parser.add_argument(
        "--resume-session",
        help="Resume polling from a previously saved session JSON file.",
    )
    parser.add_argument(
        "--check-session",
        action="store_true",
        help="Check session status once without waiting. Requires --resume-session.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    opener = build_opener(args.proxy)
    session_path: Path | None = None
    session_payload: dict[str, Any] | None = None

    if args.resume_session and args.no_poll:
        fail("--resume-session cannot be combined with --no-poll")
    if args.check_session and not args.resume_session:
        fail("--check-session requires --resume-session")

    if args.resume_session:
        session_path = Path(args.resume_session)
        session_payload = load_session(session_path)
        region = str(session_payload.get("region") or DEFAULT_REGION)
        registration = session_payload.get("registration")
        device_auth = session_payload.get("deviceAuthorization")
        if not isinstance(registration, dict) or not isinstance(device_auth, dict):
            fail("Session file is incomplete: missing registration or deviceAuthorization")
        login_mode = str(session_payload.get("loginMode") or LOGIN_MODE_BUILDER_ID)
        start_url = str(session_payload.get("startUrl") or BUILDER_ID_START_URL)
        client_name = str(session_payload.get("clientName") or DEFAULT_CLIENT_NAME)
    else:
        region = args.region
        login_mode = args.mode
        client_name = args.client_name
        if login_mode == LOGIN_MODE_BUILDER_ID:
            start_url = BUILDER_ID_START_URL
        else:
            start_url = (args.start_url or "").strip()
            if not start_url:
                fail("--start-url is required when --mode identity-center")
        registration = register_client(opener, region, client_name, args.timeout)
        device_auth = start_device_authorization(
            opener,
            region,
            registration["clientId"],
            registration["clientSecret"],
            start_url,
            args.timeout,
        )

        auth_info = build_auth_info(region, login_mode, device_auth)
        if args.session_output:
            session_path = Path(args.session_output)
            session_payload = {
                "sessionId": session_path.stem or f"kiro-login-{uuid4().hex}",
                "createdAt": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
                "region": region,
                "loginMode": login_mode,
                "startUrl": start_url,
                "clientName": client_name,
                "registration": registration,
                "deviceAuthorization": device_auth,
                "expiresAt": auth_info["expiresAt"],
            }
            save_session(session_path, session_payload)
            auth_info["sessionPath"] = str(session_path)

        if args.json:
            sys.stdout.write(json.dumps(auth_info, ensure_ascii=False, indent=2) + "\n")
        else:
            print_authorization_instructions(device_auth)

        if args.open_browser:
            target = device_auth.get("verificationUriComplete") or device_auth["verificationUri"]
            webbrowser.open(target)

        if args.no_poll:
            return

    if args.check_session:
        if session_path is None or session_payload is None:
            fail("--check-session requires a resumable session file")
        status_payload = check_session_status(
            args,
            opener,
            session_path,
            session_payload,
            region,
            registration,
            device_auth,
        )
        sys.stdout.write(json.dumps(status_payload, ensure_ascii=False, indent=2) + "\n")
        return

    cached_credential = (
        session_payload.get(CREDENTIAL_KEY)
        if isinstance(session_payload, dict)
        else None
    )
    if isinstance(cached_credential, dict):
        sys.stdout.write(json.dumps(cached_credential, ensure_ascii=False, indent=2) + "\n")
        return

    token_payload = session_payload.get(TOKEN_KEY) if isinstance(session_payload, dict) else None
    if not isinstance(token_payload, dict) or not token_payload.get("accessToken"):
        interval = int(device_auth.get("interval") or 5)
        expires_in = int(device_auth.get("expiresIn") or 600)
        if not args.json:
            print("Waiting for authorization completion...")
        token_payload = poll_for_token(
            opener,
            region,
            registration["clientId"],
            registration["clientSecret"],
            device_auth["deviceCode"],
            interval,
            expires_in,
            args.timeout,
        )
        if session_path and session_payload is not None:
            session_payload = save_session_value(
                session_path,
                session_payload,
                TOKEN_KEY,
                token_payload,
            )

    credential = build_and_cache_credential(
        args,
        opener,
        session_path,
        session_payload,
        region,
        registration,
        token_payload,
    )
    write_output(args, credential)


if __name__ == "__main__":
    main()
