"""The §6 point 1 replacement guarantee — monitored, not argued once and
forgotten (docs/design/execution-history-in-production-neo4j.md §6 point 1
correction, §12 risk 8).

Vault credentials are pattern-scrubbed only: ``exec_scrub.workspace_secret_
values()`` enumerates the per-app FILE SecretStore, never aw-vault, because
the vault's read path is approval-gated by design and an unattended bulk
read every 10-minute sync tick would either spam a human's phone or require
a new ungated endpoint (§10 — both rejected). What replaces the lost
exact-match guarantee is this: every vault secret NAME this registry knows
about carries an argued, TESTED pattern family covering its value's shape
(``tests/test_exec_scrub.py`` pins the Neo4j shapes; ``tests/
test_vault_coverage.py`` pins that every registry entry's declared family
actually redacts a representative invented shape, so the registry cannot
drift from the pattern catalog). A vault secret with no entry here is
reported as ``vault_names_uncovered`` on the sync status every tick — a new
uncovered credential degrades *visibly* instead of silently.

**List, never read (§12 risk 8).** ``list_secrets`` returns names and
metadata with no approval gate — "names + metadata. Never a value. Cheap,
ungated." (aw-app-secrets). This module calls that and nothing else; it
never calls ``read_secret``, which would put an approval prompt on a human's
phone every tick and would pull plaintext into this connector — the exact
exposure §10 rejected reading the vault to avoid.

**Degrade visibly, never silently.** If the secrets app is not installed or
the workspace backend link is down, this reports ``vault_coverage_unknown``
rather than an empty, clean-looking uncovered list — the same
degrade-visibly-never-fatally contract ``exec_scrub.workspace_secret_values``
already keeps for its own degraded path.
"""
from __future__ import annotations

import logging
from typing import Iterable

import httpx

from .key_vault import CALLER_AGENT_HEADER_VALUE, _secrets_api_base

log = logging.getLogger("aw_apps.knowledgeable.vault_coverage")

TIMEOUT_S = 20.0

#: Vault secret NAME -> the `exec_scrub` pattern-family `kind` strings argued
#: to cover every shape that value can appear in run text. Every family named
#: here must be a real kind in `exec_scrub.PATTERN_KINDS` and must be proven,
#: by a test in `tests/test_vault_coverage.py`, to redact a representative
#: invented shape of the credential — not just asserted in a comment.
#:
#: Add an entry the day a credential is first written to the vault, not
#: after it is measured missing from a run. `aw-stack-neo4j-auth` was
#: pattern-verified (not assumed) on 2026-10-19: `NEO4J_AUTH=user/pass` and
#: `bolt://user:pass@host` both redact today — the pins this registry
#: depends on staying green are `tests/test_exec_scrub.py:146-152` and
#: `:237-248`.
VAULT_COVERAGE_REGISTRY: dict[str, tuple[str, ...]] = {
    "aw-stack-neo4j-auth": ("env-assignment", "url-userinfo"),
}


def uncovered_names(names: Iterable[str]) -> list[str]:
    """Names from `list_secrets` with no argued coverage entry — the
    visible-degradation signal (§12 risk 8).

    Pure and synchronous: no I/O, so the uncovered-detection logic is
    testable without a fake HTTP layer. Sorted and de-duplicated so the
    result is stable for comparison/logging.
    """
    return sorted({name for name in names if name and name not in VAULT_COVERAGE_REGISTRY})


async def _list_secret_names() -> tuple[list[str] | None, str | None]:
    """`GET /api/apps/secrets/secrets` — names and metadata, no approval.

    Reuses `key_vault`'s loopback base/caller-identity rather than a second
    implementation of "how do I reach aw-app-secrets" (the same reasoning
    `key_vault.py`'s own docstring gives for factoring the read out of three
    push loops). Error handling mirrors `key_vault.read_secret` exactly:
    never raises, every failure is a `(None, reason)` the caller reports
    rather than silently treating as "no vault secrets".
    """
    base = _secrets_api_base()
    if base is None:
        return None, (
            "AW_WORKSPACE_API_URL/AW_WORKSPACE_API_KEY not published yet — "
            "cannot reach the secrets app"
        )
    api_url, api_key = base
    url = f"{api_url}/api/apps/secrets/secrets"
    headers = {"X-Api-Key": api_key, "X-Aw-Caller-Agent": CALLER_AGENT_HEADER_VALUE}
    try:
        async with httpx.AsyncClient(timeout=TIMEOUT_S) as c:
            resp = await c.get(url, headers=headers)
    except httpx.HTTPError as exc:
        return None, f"could not reach the secrets app: {exc}"
    if resp.status_code >= 400:
        return None, f"list_secrets failed (HTTP {resp.status_code}): {resp.text[:300]}"
    data = resp.json()
    names = [s.get("name") for s in (data.get("secrets") or []) if s.get("name")]
    return names, None


async def coverage_status() -> dict:
    """What a sync tick reports (§8's observability), alongside
    `exact_match_unavailable`:

    * ``vault_coverage_unknown`` — set when the check itself could not run
      (the secrets app is absent, or the backend link is down). Never
      conflated with "fully covered": an unreachable check must not look
      like a clean pass.
    * ``vault_names_uncovered`` — the vault secret names visible today with
      no argued pattern coverage. An empty list means every vault secret
      this connector can currently see is covered; it is `None` exactly when
      `vault_coverage_unknown` is set, because the list was never obtained.
    """
    names, error = await _list_secret_names()
    if error:
        log.warning("vault coverage check degraded — %s", error)
        return {"vault_coverage_unknown": error, "vault_names_uncovered": None}
    return {"vault_coverage_unknown": None, "vault_names_uncovered": uncovered_names(names)}
