"""A workspace-published env var, from this process or from the ``.env`` the
server mirrors it into (0600, written at boot).

Copied from ``aw-app-agents-platform-runners/agents_platform_runners_app/
platform_base.py::workspace_env`` (same helper, same callers, no shared
library between app repos in this estate) — needed here for the same reason:
this Tier-1 process reaches another of this workspace's own apps
(``aw-app-secrets``) over loopback with ``AW_WORKSPACE_API_KEY``, and that
key is not guaranteed to be in ``os.environ`` for every process shape this
app might run under.
"""
from __future__ import annotations

import os


def workspace_env(name: str) -> str:
    value = os.environ.get(name)
    if value:
        return value
    home = os.environ.get("AW_WORKSPACE_HOME") or os.path.join(
        os.environ.get("AW_WORKSPACE_CONTAINER_DIR", "/opt/aw-workspace"), ".aw-workspace")
    try:
        with open(os.path.join(home, ".env"), "r", encoding="utf-8") as fh:
            for line in fh:
                if line.startswith(f"{name}="):
                    return line.split("=", 1)[1].strip()
    except OSError:
        pass
    return ""
