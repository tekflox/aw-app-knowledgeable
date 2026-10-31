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


def test_signoz_get_field_values_hint_states_signal_enum():
    """The real tool's `signal` arg is a strict 3-value enum, confirmed from
    its own JSON schema. The hint is the model's only source of truth for
    valid values (it has no MCP access of its own) — omitting the enum is
    exactly what let the adapter guess a value the tool rejects with
    'Parameter validation failed: "signal" must be one of: "metrics",
    "traces", "logs"' (bug:signoz-nl-adapter-invalid-signal-poisons-whole-source).
    """
    hint = _signoz_default_entry()["tools"]["signoz_get_field_values"].get("hint") or ""
    for value in ("metrics", "traces", "logs"):
        assert value in hint, f"hint must state the real tool's signal enum value {value!r}"


def test_signoz_get_field_keys_hint_states_signal_enum():
    hint = _signoz_default_entry()["tools"]["signoz_get_field_keys"].get("hint") or ""
    for value in ("metrics", "traces", "logs"):
        assert value in hint, f"hint must state the real tool's signal enum value {value!r}"
