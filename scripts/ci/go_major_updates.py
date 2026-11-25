#!/usr/bin/env python3
"""Finds and applies new major versions of core Go modules.

Dependabot handles every other update, but it cannot move a Go module to a new
major version because that changes the module path (pion/webrtc/v4 to /v5). The
Go Major Updates workflow uses this script to fill that gap.

  go_major_updates.py detect
      Prints a JSON list of updates that do not have an auto/go/ branch yet.
  go_major_updates.py apply MODULE NEW_MODULE VERSION
      Rewrites imports and go.mod in services/go.
"""

import json
import re
import subprocess
import sys
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
GO_ROOT = ROOT / "services" / "go"

# Core direct modules, without their major version suffix. Keep in step with the
# gomod allow list in .github/dependabot.yml.
CORE_MODULES = {
    "aidanwoods.dev/go-paseto",
    "github.com/coder/websocket",
    "github.com/jackc/pgx",
    "github.com/k2-fsa/sherpa-onnx-go",
    "github.com/pion/webrtc",
    "github.com/redis/go-redis",
    "github.com/twilio/twilio-go",
    "google.golang.org/grpc",
    "google.golang.org/protobuf",
    "gopkg.in/yaml",
    "modernc.org/sqlite",
}


def direct_requires(go_mod: str) -> dict[str, str]:
    """Maps each direct module path in go.mod to its version."""
    found = re.findall(r"^(?:require\s+|\s+)(\S+)\s+(v\S+)\s*$", go_mod, re.MULTILINE)
    return dict(found)


def split_major(path: str) -> tuple[str, int]:
    """Splits a module path into its base path and major version (v0 counts as 1)."""
    match = re.fullmatch(r"(gopkg\.in/.+)\.v(\d+)", path) or re.fullmatch(r"(.+)/v(\d+)", path)
    return (match.group(1), int(match.group(2))) if match else (path, 1)


def module_path(base: str, major: int) -> str:
    return f"{base}.v{major}" if base.startswith("gopkg.in/") else f"{base}/v{major}"


def latest_version(path: str) -> str | None:
    """Asks the Go module proxy for a module's latest version, None if there is no answer.

    The proxy can be slow on paths it has never seen. A transient failure only
    delays the update to the next daily run, so it warns instead of failing.
    """
    escaped = re.sub(r"[A-Z]", lambda m: "!" + m.group().lower(), path)
    try:
        with urllib.request.urlopen(f"https://proxy.golang.org/{escaped}/@latest", timeout=90) as reply:
            return json.load(reply)["Version"]
    except urllib.error.HTTPError as error:
        if error.code not in (404, 410):
            print(f"::warning::Go proxy returned {error.code} for {path}", file=sys.stderr)
    except OSError as error:
        print(f"::warning::Go proxy lookup failed for {path}: {error}", file=sys.stderr)
    return None


def newest_major(base: str, major: int) -> tuple[str, str] | None:
    """Walks up the major versions and returns the newest stable (path, version) above major."""
    newest = None
    candidate = major + 1
    while (version := latest_version(module_path(base, candidate))) is not None:
        if "-" not in version:
            newest = (module_path(base, candidate), version)
        candidate += 1
    return newest


def existing_branches() -> set[str]:
    refs = subprocess.run(
        ["git", "ls-remote", "--heads", "origin", "auto/go/*"],
        cwd=ROOT, check=True, capture_output=True, text=True,
    ).stdout
    return {line.split("refs/heads/", 1)[1] for line in refs.splitlines()}


def detect() -> None:
    taken = existing_branches()
    updates = []
    for path, version in sorted(direct_requires((GO_ROOT / "go.mod").read_text()).items()):
        base, major = split_major(path)
        if base not in CORE_MODULES:
            continue
        newest = newest_major(base, major)
        if newest is None:
            continue
        new_path, new_version = newest
        slug = re.sub(r"[^A-Za-z0-9.]+", "-", "/".join(base.split("/")[-2:]))
        branch = f"auto/go/{slug}-{new_version.split('.')[0]}"
        if branch not in taken:
            updates.append({
                "module": path,
                "new_module": new_path,
                "from_version": version,
                "version": new_version,
                "branch": branch,
            })
    print(json.dumps(updates))


def apply(module: str, new_module: str, version: str) -> None:
    imports = re.compile(r'"' + re.escape(module) + r'(?=[/"])')
    for source in GO_ROOT.rglob("*.go"):
        text = source.read_text(encoding="utf-8")
        rewritten = imports.sub(f'"{new_module}', text)
        if rewritten != text:
            source.write_text(rewritten, encoding="utf-8")

    go = lambda *args, **kwargs: subprocess.run(["go", *args], cwd=GO_ROOT, **kwargs)
    go("mod", "edit", f"-droprequire={module}", f"-require={new_module}@{version}", check=True)
    # A failed tidy still produces a branch: the tests then show the update needs porting work.
    if go("mod", "tidy").returncode != 0:
        print(f"::warning::go mod tidy failed after moving to {new_module}@{version}")


if __name__ == "__main__":
    command, *arguments = sys.argv[1:]
    {"detect": detect, "apply": apply}[command](*arguments)
