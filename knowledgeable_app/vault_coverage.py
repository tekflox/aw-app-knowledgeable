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

**Three states, not two (card triaging all 21 names, 2026-10-19).** A vault
secret NAME is one of: covered by an argued, tested family
(``VAULT_COVERAGE_REGISTRY``); reviewed and found not credential-shaped, with
a recorded reason (``VAULT_REVIEWED_NOT_CREDENTIAL``); or never looked at.
Only the third turns ``vault_names_uncovered`` non-empty — a permanently red
doctor over the same ~20 pre-existing names detects nothing and gets
ignored, which is the exact silent degradation this mechanism exists to
prevent. The reason an "harmless" name gets its own dict rather than an
empty tuple in the first one: an absent entry and a reviewed-harmless entry
must stay distinguishable at a glance, or the next person re-triages names
someone already cleared.
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
#: after it is measured missing from a run. Each entry below records WHY the
#: declared families cover that credential's actual shape — judged from
#: `list_secrets`' own name/description for it, never from its value (list,
#: never read).
VAULT_COVERAGE_REGISTRY: dict[str, tuple[str, ...]] = {
    # Pattern-verified (not assumed) on 2026-10-19: `NEO4J_AUTH=user/pass` and
    # `bolt://user:pass@host` both redact today — the pins this registry
    # depends on staying green are `tests/test_exec_scrub.py:146-152` and
    # `:237-248`.
    "aw-stack-neo4j-auth": ("env-assignment", "url-userinfo"),
    # `list_secrets` describes this as "KEY=VALUE format, eval-ready" — a
    # shell-sourceable AWS_ACCESS_KEY_ID=/AWS_SECRET_ACCESS_KEY= pair. The
    # access key ID's own AKIA… shape is a `provider-token` match with no
    # context needed; the secret access key has no shape of its own and is
    # caught by `env-assignment` on the `…_ACCESS_KEY=` name.
    "AWS_CREDENTIALS": ("provider-token", "env-assignment"),
    # Same AKIA-access-key / opaque-secret-key pair shape as AWS_CREDENTIALS
    # — IAM user credentials for the ddns / local-dns Route53 publishers.
    "aw_ddns_route53_credential": ("provider-token", "env-assignment"),
    "aw_local_dns_route53_credential": ("provider-token", "env-assignment"),
    # Redis passwords: a `REDIS…PASSWORD=` env assignment (the `.env`
    # consumers `list_secrets` names) and inline in a connection URL —
    # `url-userinfo`'s empty-username form is the one this estate's own
    # Redis 2-arg-AUTH scars already pinned.
    "aw-stack-redis-admin": ("env-assignment", "url-userinfo"),
    "aw-stack-redis-svc-backend": ("env-assignment", "url-userinfo"),
    "aw-stack-redis-svc-runs": ("env-assignment", "url-userinfo"),
    "aw-stack-redis-svc-workspace": ("env-assignment", "url-userinfo"),
    # Google OAuth client secret — no fixed public shape, caught by
    # `env-assignment` on a `…CLIENT_SECRET=`-style name.
    "google_oauth_aw_dashboard": ("env-assignment",),
    "google_oauth_workspace_broker": ("env-assignment",),
    # Classic GitHub PAT (`ghp_…`) — `provider-token` matches the literal
    # prefix with no context needed.
    "knowledge_base_git_token": ("provider-token",),
    # AP-MT scoped API keys: pushed into an app's own config and used as a
    # Bearer credential against AP-MT's OpenAI-compatible API — caught by
    # `authorization-header` when sent, `env-assignment` when configured.
    "knowledgeable-explorer-apmt-key": ("authorization-header", "env-assignment"),
    "knowledgeable-extractor-apmt-key": ("authorization-header", "env-assignment"),
    "knowledgeable-labeller-apmt-key": ("authorization-header", "env-assignment"),
    "knowledgeable-playground-apmt-key": ("authorization-header", "env-assignment"),
    # VNC password with no shape of its own, caught by `env-assignment` on a
    # `…VNC_PASSWORD=`-style name.
    "mac_home_vnc_password": ("env-assignment",),
    # Whole PEM/OpenSSH private-key blocks — `private-key` matches the
    # BEGIN/END markers regardless of key type.
    "private_root_aw.tekflox.com": ("private-key",),
    "private_root_dev.tekflox.com": ("private-key",),
    "private_u888-smwcgrxvnrg3_ssh.sapatariacrispal.com": ("private-key",),
    # Resend API key — no fixed public shape, caught by `env-assignment`
    # when configured and `authorization-header` when sent as a Bearer
    # credential (Resend's own API convention).
    "resend_api_key": ("env-assignment", "authorization-header"),
    # Zadarma SIP account password — no fixed shape, caught by
    # `env-assignment` on a `…PASSWORD=`-style name.
    "zadarma_account_password": ("env-assignment",),
}

#: Vault secret NAMES reviewed and found NOT credential-shaped — a hostname,
#: slug, account id or similar config string that cannot leak a secret value
#: on its own, so no pattern family applies. Every entry records WHY, so a
#: future reader can tell "deliberately accepted" from "nobody looked yet"
#: at a glance. Kept as its own dict rather than an empty tuple in
#: `VAULT_COVERAGE_REGISTRY` for exactly that reason — folding it in would
#: make a reviewed-harmless name indistinguishable from a covered credential
#: at a glance, which is the ambiguity this dict exists to remove.
#:
#: Empty today: triaging the 21 names current in production (2026-10-19)
#: found none that qualify — every one is credential-shaped. This dict
#: exists so the NEXT name that genuinely is a hostname/slug/account id has
#: somewhere to go other than `VAULT_COVERAGE_REGISTRY` with an invented
#: family or `vault_names_uncovered` forever.
VAULT_REVIEWED_NOT_CREDENTIAL: dict[str, str] = {}


def uncovered_names(names: Iterable[str]) -> list[str]:
    """Names from `list_secrets` that are neither covered by a pattern
    family nor reviewed-and-cleared — the visible-degradation signal (§12
    risk 8). A name leaves this list by being registered in
    `VAULT_COVERAGE_REGISTRY` OR by being reviewed and found harmless in
    `VAULT_REVIEWED_NOT_CREDENTIAL`; only a name nobody has looked at yet
    belongs here, so a doctor that goes red over it means something NEW.

    Pure and synchronous: no I/O, so the uncovered-detection logic is
    testable without a fake HTTP layer. Sorted and de-duplicated so the
    result is stable for comparison/logging.
    """
    return sorted(
        {
            name
            for name in names
            if name
            and name not in VAULT_COVERAGE_REGISTRY
            and name not in VAULT_REVIEWED_NOT_CREDENTIAL
        }
    )


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
