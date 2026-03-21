#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import re
import select
import shutil
import stat
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any


DEFAULT_BASE_ROOT = Path.home() / ".telex-codex-switcher"


class CodexIdentityError(RuntimeError):
    pass


def slugify(value: str) -> str:
    slug = re.sub(r"[^a-zA-Z0-9._-]+", "-", value.strip()).strip("-").lower()
    if not slug:
        raise CodexIdentityError("identity name resolves to an empty slug")
    return slug


def ensure_dir(path: Path, mode: int = 0o700) -> None:
    path.mkdir(parents=True, exist_ok=True)
    os.chmod(path, mode)


def chmod_if_exists(path: Path, mode: int) -> None:
    if path.exists() or path.is_symlink():
        os.chmod(path, mode)


def atomic_write_text(path: Path, content: str, mode: int = 0o600) -> None:
    ensure_dir(path.parent)
    tmp = path.with_name(f".{path.name}.tmp")
    tmp.write_text(content)
    os.chmod(tmp, mode)
    tmp.replace(path)
    os.chmod(path, mode)


def atomic_write_json(path: Path, payload: dict[str, Any], mode: int = 0o600) -> None:
    atomic_write_text(path, json.dumps(payload, indent=2, sort_keys=True) + "\n", mode=mode)


def load_registry(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"version": 1, "identities": {}}
    return normalize_registry(json.loads(path.read_text()))


def store_registry(path: Path, registry: dict[str, Any]) -> None:
    atomic_write_json(path, normalize_registry(registry))


def normalize_registry(registry: dict[str, Any]) -> dict[str, Any]:
    identities = registry.get("identities") or {}
    return {
        "version": registry.get("version", 1),
        "identities": {
            slug: normalize_identity_record(slug, payload)
            for slug, payload in identities.items()
        },
    }


def normalize_identity_record(slug: str, payload: dict[str, Any]) -> dict[str, Any]:
    if "display_name" in payload and "codex_home" in payload and "shared_sessions_root" in payload:
        normalized = dict(payload)
        normalized.setdefault("id", payload.get("id", slug))
        return normalized

    auth_mode = payload.get("auth_mode", "chatgpt")
    kind = "chatgpt_workspace" if auth_mode == "chatgpt" else "api_key"
    return {
        "id": payload.get("id", payload.get("slug", slug)),
        "display_name": payload.get("display_name", payload.get("name", slug)),
        "kind": payload.get("kind", kind),
        "auth_mode": auth_mode,
        "codex_home": payload.get("codex_home", payload.get("home")),
        "shared_sessions_root": payload.get(
            "shared_sessions_root", payload.get("shared_sessions")
        ),
        "forced_login_method": payload.get(
            "forced_login_method", "chatgpt" if auth_mode == "chatgpt" else None
        ),
        "forced_chatgpt_workspace_id": payload.get("forced_chatgpt_workspace_id"),
        "api_key_env_var": payload.get(
            "api_key_env_var", "OPENAI_API_KEY" if auth_mode == "apikey" else None
        ),
        "email": payload.get("email"),
        "plan_type": payload.get("plan_type"),
        "account_type": payload.get("account_type"),
        "authenticated": payload.get("authenticated"),
        "last_auth_method": payload.get("last_auth_method"),
        "enabled": payload.get("enabled", True),
        "priority": payload.get("priority", 0),
        "notes": payload.get("notes"),
        "workspace_force_probe": payload.get("workspace_force_probe"),
        "imported_auth": payload.get("imported_auth", False),
        "created_at": payload.get("created_at", 0),
        "last_verified_at": payload.get("last_verified_at"),
    }


def build_registry_identity(
    name: str,
    identity_slug: str,
    auth_mode: str,
    home: Path,
    shared_sessions: Path,
    imported_auth: bool,
    existing: dict[str, Any] | None = None,
) -> dict[str, Any]:
    identity = normalize_identity_record(identity_slug, existing or {})
    identity.update(
        {
            "id": identity_slug,
            "display_name": name,
            "kind": "chatgpt_workspace" if auth_mode == "chatgpt" else "api_key",
            "auth_mode": auth_mode,
            "codex_home": str(home),
            "shared_sessions_root": str(shared_sessions),
            "forced_login_method": "chatgpt" if auth_mode == "chatgpt" else None,
            "api_key_env_var": (
                identity.get("api_key_env_var") or "OPENAI_API_KEY"
                if auth_mode == "apikey"
                else None
            ),
            "imported_auth": imported_auth,
            "created_at": identity.get("created_at") or int(time.time()),
        }
    )
    return identity


def build_config(auth_mode: str) -> str:
    lines = [
        '# Managed by tools/codex_identity.py',
        'cli_auth_credentials_store = "file"',
    ]
    if auth_mode == "chatgpt":
        lines.append('forced_login_method = "chatgpt"')
    return "\n".join(lines) + "\n"


def resolve_identity_home(base_root: Path, name: str) -> Path:
    return base_root / "homes" / slugify(name)


def resolve_registry(base_root: Path) -> Path:
    return base_root / "registry.json"


def resolve_shared_sessions(base_root: Path) -> Path:
    return base_root / "shared" / "sessions"


def create_sessions_link(home: Path, shared_sessions: Path) -> None:
    ensure_dir(shared_sessions)
    link_path = home / "sessions"
    if link_path.exists() or link_path.is_symlink():
        if not link_path.is_symlink():
            raise CodexIdentityError(f"{link_path} exists and is not a symlink")
        current_target = link_path.resolve()
        if current_target != shared_sessions.resolve():
            raise CodexIdentityError(
                f"{link_path} points to {current_target}, expected {shared_sessions}"
            )
        return
    link_path.symlink_to(shared_sessions)


@dataclass
class AppServerSummary:
    authenticated: bool
    auth_method: str | None
    email: str | None
    plan_type: str | None
    account_type: str | None
    rate_limits: dict[str, Any]


class JsonRpcAppServer:
    def __init__(self, codex_home: Path):
        env = os.environ.copy()
        env["CODEX_HOME"] = str(codex_home)
        self.proc = subprocess.Popen(
            ["codex", "app-server", "--listen", "stdio://"],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
            env=env,
        )

    def close(self) -> None:
        try:
            self.proc.terminate()
            self.proc.wait(timeout=5)
        except Exception:
            try:
                self.proc.kill()
            except Exception:
                pass

    def _send(self, payload: dict[str, Any]) -> None:
        assert self.proc.stdin is not None
        self.proc.stdin.write(json.dumps(payload) + "\n")
        self.proc.stdin.flush()

    def _recv_for_id(self, request_id: str, timeout: float) -> dict[str, Any]:
        assert self.proc.stdout is not None
        deadline = time.time() + timeout
        while time.time() < deadline:
            ready, _, _ = select.select([self.proc.stdout], [], [], 0.5)
            if not ready:
                continue
            line = self.proc.stdout.readline()
            if not line:
                continue
            message = json.loads(line)
            if message.get("id") == request_id:
                return message
        raise CodexIdentityError(f"timeout waiting for app-server response to {request_id}")

    def request(self, method: str, params: dict[str, Any] | None = None, timeout: float = 20.0) -> dict[str, Any]:
        request_id = f"{method}-{time.time_ns()}"
        payload: dict[str, Any] = {"jsonrpc": "2.0", "id": request_id, "method": method}
        if params is not None:
            payload["params"] = params
        self._send(payload)
        return self._recv_for_id(request_id, timeout)

    def initialize(self) -> None:
        response = self.request(
            "initialize",
            {
                "clientInfo": {
                    "name": "codex-identity",
                    "title": "Codex Identity",
                    "version": "0.1.0",
                },
                "capabilities": {"experimentalApi": False},
            },
        )
        if "error" in response:
            raise CodexIdentityError(f"initialize failed: {response['error']}")
        self._send({"jsonrpc": "2.0", "method": "initialized"})


def summarize_home(codex_home: Path) -> AppServerSummary:
    server = JsonRpcAppServer(codex_home)
    try:
        server.initialize()
        auth_status = server.request("getAuthStatus", {})
        if "error" in auth_status:
            raise CodexIdentityError(f"getAuthStatus failed: {auth_status['error']}")
        auth_result = auth_status.get("result") or {}

        account_response = server.request("account/read", {})
        rate_limits_response = server.request("account/rateLimits/read", {})

        account_result = account_response.get("result") if isinstance(account_response, dict) else None
        rate_limits_result = rate_limits_response.get("result") if isinstance(rate_limits_response, dict) else None

        account = (account_result or {}).get("account") or {}
        rate_limits = (rate_limits_result or {}).get("rateLimitsByLimitId") or {}
        return AppServerSummary(
            authenticated=bool(auth_result.get("authMethod")),
            auth_method=auth_result.get("authMethod"),
            email=account.get("email"),
            plan_type=account.get("planType"),
            account_type=account.get("type"),
            rate_limits=rate_limits,
        )
    finally:
        server.close()


def format_rate_limits(rate_limits: dict[str, Any]) -> list[str]:
    lines: list[str] = []
    for limit_id, payload in sorted(rate_limits.items()):
        primary = payload.get("primary") or {}
        secondary = payload.get("secondary") or {}
        primary_used = primary.get("usedPercent")
        secondary_used = secondary.get("usedPercent")
        primary_window = primary.get("windowDurationMins")
        secondary_window = secondary.get("windowDurationMins")
        lines.append(
            f"{limit_id}: primary={primary_used}%/{primary_window}m secondary={secondary_used}%/{secondary_window}m"
        )
    return lines


def command_prepare(args: argparse.Namespace) -> int:
    base_root = Path(args.base_root).expanduser().resolve()
    identity_slug = slugify(args.name)
    home = resolve_identity_home(base_root, args.name)
    shared_sessions = resolve_shared_sessions(base_root)
    registry_path = resolve_registry(base_root)

    ensure_dir(base_root)
    ensure_dir(base_root / "homes")
    ensure_dir(base_root / "shared")
    ensure_dir(home)
    create_sessions_link(home, shared_sessions)

    config_path = home / "config.toml"
    if config_path.exists() and not args.overwrite_config:
        raise CodexIdentityError(
            f"{config_path} already exists; rerun with --overwrite-config if you want to replace it"
        )
    atomic_write_text(config_path, build_config(args.auth_mode))

    if args.import_auth_from_home:
        source_home = Path(args.import_auth_from_home).expanduser().resolve()
        source_auth = source_home / "auth.json"
        if not source_auth.exists():
            raise CodexIdentityError(f"cannot import auth; missing {source_auth}")
        shutil.copy2(source_auth, home / "auth.json")
        chmod_if_exists(home / "auth.json", 0o600)

    registry = load_registry(registry_path)
    registry.setdefault("identities", {})
    registry["identities"][identity_slug] = build_registry_identity(
        args.name,
        identity_slug,
        args.auth_mode,
        home,
        shared_sessions,
        bool(args.import_auth_from_home),
        registry["identities"].get(identity_slug),
    )
    store_registry(registry_path, registry)

    login_cmd = f'CODEX_HOME="{home}" codex login'
    print(f"prepared {args.name}")
    print(f"home: {home}")
    print(f"shared sessions: {shared_sessions}")
    if args.import_auth_from_home:
        print(f"auth imported from: {Path(args.import_auth_from_home).expanduser().resolve()}")
    else:
        print("auth imported from: none")
        print("next:")
        print(f"  {login_cmd}")
    print("verify:")
    print(f'  python3 tools/codex_identity.py verify "{args.name}" --base-root "{base_root}"')
    return 0


def command_verify(args: argparse.Namespace) -> int:
    base_root = Path(args.base_root).expanduser().resolve()
    registry = load_registry(resolve_registry(base_root))
    identity_slug = slugify(args.name)
    identity = (registry.get("identities") or {}).get(identity_slug)
    if not identity:
        raise CodexIdentityError(f"identity {args.name!r} is not registered in {base_root}")
    home = Path(identity["codex_home"])
    summary = summarize_home(home)
    print(f"name: {identity['display_name']}")
    print(f"home: {home}")
    print(f"authenticated: {'yes' if summary.authenticated else 'no'}")
    print(f"auth method: {summary.auth_method}")
    print(f"account type: {summary.account_type}")
    print(f"email: {summary.email}")
    print(f"plan type: {summary.plan_type}")
    if summary.rate_limits:
        print("rate limits:")
        for line in format_rate_limits(summary.rate_limits):
            print(f"  {line}")
    else:
        print("rate limits: none")
    return 0


def command_list(args: argparse.Namespace) -> int:
    base_root = Path(args.base_root).expanduser().resolve()
    registry = load_registry(resolve_registry(base_root))
    identities = registry.get("identities") or {}
    if not identities:
        print("no identities registered")
        return 0
    for slug, identity in sorted(identities.items()):
        print(f"{identity['display_name']} ({slug})")
        print(f"  home: {identity['codex_home']}")
        print(f"  auth mode: {identity['auth_mode']}")
        print(f"  imported auth: {'yes' if identity.get('imported_auth') else 'no'}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Prepare and verify managed Codex identity homes.")
    parser.set_defaults(func=None)

    subparsers = parser.add_subparsers(dest="command")

    prepare = subparsers.add_parser("prepare", help="Create or update a managed identity home.")
    prepare.add_argument("name", help="Human-readable identity name.")
    prepare.add_argument(
        "--base-root",
        default=str(DEFAULT_BASE_ROOT),
        help=f"Managed runtime root. Default: {DEFAULT_BASE_ROOT}",
    )
    prepare.add_argument(
        "--auth-mode",
        choices=["chatgpt", "apikey"],
        default="chatgpt",
        help="Authentication mode for this identity.",
    )
    prepare.add_argument(
        "--import-auth-from-home",
        help="Copy auth.json from another CODEX_HOME, for example ~/.codex.",
    )
    prepare.add_argument(
        "--overwrite-config",
        action="store_true",
        help="Replace an existing managed config.toml.",
    )
    prepare.set_defaults(func=command_prepare)

    verify = subparsers.add_parser("verify", help="Verify auth and rate limits for a managed identity.")
    verify.add_argument("name", help="Human-readable identity name.")
    verify.add_argument(
        "--base-root",
        default=str(DEFAULT_BASE_ROOT),
        help=f"Managed runtime root. Default: {DEFAULT_BASE_ROOT}",
    )
    verify.set_defaults(func=command_verify)

    list_cmd = subparsers.add_parser("list", help="List registered managed identities.")
    list_cmd.add_argument(
        "--base-root",
        default=str(DEFAULT_BASE_ROOT),
        help=f"Managed runtime root. Default: {DEFAULT_BASE_ROOT}",
    )
    list_cmd.set_defaults(func=command_list)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.func is None:
        parser.print_help()
        return 1
    try:
        return args.func(args)
    except CodexIdentityError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
