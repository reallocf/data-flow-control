"""Receive only an auth record and prompt; never mount the host Codex home."""

import json
import os
from pathlib import Path
import subprocess
import sys


def build_command(payload):
    benchmark = payload.get("benchmark", "predicate")
    if benchmark not in {"predicate", "workflow"}:
        raise ValueError("Unknown benchmark")
    schema = "workflow.schema.json" if benchmark == "workflow" else "submission.schema.json"
    command = [
        "codex",
        "exec",
        "--json",
        "--ephemeral",
        "--ignore-user-config",
        "--skip-git-repo-check",
        "--sandbox",
        "read-only",
        "--output-schema",
        "/opt/experiment/" + schema,
        "-m",
        payload["model"],
        "-c",
        'forced_login_method="chatgpt"',
        "-c",
        "project_doc_max_bytes=0",
        "-c",
        'web_search="disabled"',
        "-c",
        "memories.use_memories=false",
        "-c",
        "memories.generate_memories=false",
    ]
    if payload.get("effort") is not None:
        command.extend(["-c", "model_reasoning_effort=" + json.dumps(payload["effort"])])
    for feature in (
        "memories",
        "shell_tool",
        "shell_snapshot",
        "apps",
        "plugins",
        "remote_plugin",
        "multi_agent",
        "hooks",
        "browser_use",
        "computer_use",
        "image_generation",
        "view_image",
        "skill_search",
        "sleep_tool",
        "code_mode_host",
    ):
        command.extend(["--disable", feature])
    command.extend(["--enable", "skip_host_skill_discovery", "-"])
    return command


def main():
    payload = json.load(sys.stdin)
    home = Path("/home/node/experiment-home")
    home.mkdir(mode=0o700)
    codex_home = home / ".codex"
    codex_home.mkdir(mode=0o700)
    auth_path = codex_home / "auth.json"
    auth_path.write_text(json.dumps(payload["auth"]))
    auth_path.chmod(0o600)
    env = dict(os.environ, HOME=str(home), CODEX_HOME=str(codex_home))
    for key in ("OPENAI_API_KEY", "CODEX_API_KEY"):
        env.pop(key, None)
    command = build_command(payload)
    try:
        if payload.get("mode") == "auth-check":
            command = ["codex", "login", "status"]
        result = subprocess.run(
            command, input=payload.get("prompt", ""), text=True, env=env, check=False
        )
        sys.exit(result.returncode)
    finally:
        auth_path.unlink(missing_ok=True)


if __name__ == "__main__":
    main()
