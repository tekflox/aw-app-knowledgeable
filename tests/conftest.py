"""Suite-wide guards for things that are NOT the same inside this workspace
as they are in CI.

## Why this file exists: a test that could only be green here

Slice 4 of `docs/design/exec-index-per-tool-policy.md` shipped a test that
read the real `/opt/aw-workspace/apps` and asserted that a never-declared
tool's result is indexed. That directory exists for every human and every
agent who runs this suite inside the workspace container, and does NOT exist
on a GitHub runner. So `exec_policy.PolicyTable.from_workspace()` degraded
in CI, every MCP-routed name failed closed exactly as §3.5 says it must, the
result was denied, and the assertion was false.

The part that makes this worth a conftest rather than a one-line fix: the
test was verified twice, by two parties, each running the full suite, each
getting 715 passed — and **neither verification could see the defect**, because
both ran in the one environment where the dependency is satisfied. That is
not carelessness, it is a structural blind spot: a suite that reads the
machine it runs on has no local signal that it is doing so. The fix has to
remove the ability to acquire the dependency by accident, not just the one
instance of it.

## What the guard does

`exec_policy._apps_root()` is redirected, for every test, to a path that
does not exist — which is CI's shape, so local and CI agree by default and
a test can no longer be green only here. A test that wants a tree declares
one (`_policy_tree` in `test_exec_sync.py`, `_estate` in
`test_exec_policy.py`, both of which patch `_apps_root` themselves and are
unaffected by this).

**A test that genuinely wants the real installed tree asks for it**, with
the `real_apps_root` fixture below. That is an explicit, greppable opt-in
whose name says what it costs, instead of an invisible default — and such a
test must then tolerate the tree being absent, because on a runner it is.

Deliberately NOT done here: pointing the default at an empty-but-readable
directory. "No apps installed" (`scan_error=None`, every routed name
unresolved) and "apps root unreadable" (`scan_error` set) are two different
states that the counters report differently — `unresolved_split` suppresses
the `UNRESOLVABLE_UPSTREAMS` classification only in the second — so
collapsing them in the fixture would hide a real distinction the production
code makes.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from knowledgeable_app import exec_policy  # noqa: E402

#: Where the guard points `_apps_root` when a test has not declared a tree.
#: Under `/opt/aw-workspace` on purpose: a path far from the real one would
#: make a leaked dependency fail with a confusing message about some unrelated
#: directory, where this one reads as "the apps root, absent" — which is
#: precisely what a runner sees.
ABSENT_APPS_ROOT = "/opt/aw-workspace/apps-absent-in-tests"


@pytest.fixture(autouse=True)
def _apps_root_is_not_the_real_one(monkeypatch):
    """Make the real installed tree unreachable unless a test asks for it.

    Autouse, so the dependency cannot be acquired by forgetting something.
    Tests that patch `_apps_root` themselves simply win — `monkeypatch`
    applies in order and theirs is applied second.
    """
    monkeypatch.setattr(exec_policy, "_apps_root", lambda: ABSENT_APPS_ROOT)


@pytest.fixture()
def real_apps_root(monkeypatch):
    """Opt back in to the REAL `/opt/aw-workspace/apps`, and skip when it is
    not there.

    For the measurement-style checks §12 risk 1 asks for — "38 of the
    gateway's 40 live upstream segments resolve, each to the right app" is a
    claim about this workspace that no fixture can make. The skip is the
    honest outcome on a runner: the claim is unverifiable there, and a test
    that quietly asserted it anyway would be back to being green for the
    wrong reason.

    The import is guarded because CI runs `python3 -m pytest tests/` from
    this repo alone, with no workspace checkout on the path — so `src.apps`
    is not importable there and an unguarded import would be a hard ERROR
    inside a fixture rather than the skip this is supposed to produce. That
    is the same class of mistake as the one this whole file exists to
    prevent, one layer down.
    """
    try:
        from src.apps import paths  # noqa: PLC0415

        root = paths.apps_root()
    except Exception:
        root = None
    if not root or not Path(root).is_dir():
        pytest.skip(f"no installed apps tree at {root!r} — not a CI-verifiable claim")
    monkeypatch.setattr(exec_policy, "_apps_root", lambda: root)
    return root
