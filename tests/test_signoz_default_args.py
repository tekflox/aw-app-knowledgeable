"""Pins the shipped `aw-app.json` default's `forward_upstreams[signoz]` tool
args against what the real SigNoz query tools actually require — the drift
class found live on card `feature:knowledgeable-sources-planner-s1-catalog`
(PO scope addition, 2026-10-29): the manifest default declared
`signoz_get_field_values`'s arg as `key`, the real tool requires `name`, so
the forwarder's allow-list (`forwarding.py:489`) rejected every value the
adapter could possibly send — broken in both directions, silently, until a
live adapter call hit it.

The ground truth below is the real `signoz_get_field_values` tool's required
parameter (`signal`, `name` — verified against the live `signoz-query` MCP
tool schema, not re-derived here) — deliberately NOT `key`, so this test
cannot pass by accidentally copying the bug it exists to catch.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

APP_JSON = Path(__file__).resolve().parent.parent / "aw-app.json"


def _signoz_default_entry() -> dict:
    manifest = json.loads(APP_JSON.read_text())
    default = manifest["config_schema"]["properties"]["forward_upstreams"]["default"]
    for entry in default:
        if entry.get("id") == "signoz":
            return entry
    raise AssertionError("no 'signoz' entry in forward_upstreams default")


def test_signoz_get_field_values_declares_name_not_key():
    entry = _signoz_default_entry()
    declared = set(entry["tools"]["signoz_get_field_values"]["args"])
    assert "key" not in declared, (
        "the real signoz_get_field_values tool has no 'key' parameter — "
        "declaring it lets the adapter propose an arg the tool will reject"
    )
    # The real tool's two required parameters, both must be allow-listed or
    # the adapter can never call it successfully at all.
    assert {"signal", "name"}.issubset(declared)


def test_signoz_get_field_values_hint_does_not_name_the_wrong_arg():
    hint = _signoz_default_entry()["tools"]["signoz_get_field_values"].get("hint") or ""
    assert "key" not in hint.split()
