"""Credential scrubbing for execution-history text, before it leaves the
workspace (docs/design/execution-history-in-production-neo4j.md §6).

**Why this file is the sharp end of the whole feature.** Agent run text
contains live tokens — measured, not assumed: the retrieval test's throwaway
Neo4j was built with no published port and had its password rotated
afterwards precisely because of what was in there. The production Neo4j backs
a publicly reachable app, its nightly dumps leave the box for S3, and (until
T2 per-token bucket scoping lands) every *registered* bucket in a tenant is
granted read+write to every caller in that tenant. So a credential that gets
past this module is durable: it is in the graph, in every backup for that
backup's retention life, and it cannot be un-written by a later fix.

**Why it runs here and not in aw-knowledgeable.** Two reasons, and they are
the same reason:

* This process is where ``SecretStore`` lives, so the set of known-live
  secret VALUES is enumerable. Exact matching against that set is a far
  stronger guarantee than any pattern — it catches a credential with no
  recognisable shape, which is the whole residual risk pattern scrubbing
  leaves behind.
* The workspace must never hold the production Neo4j password (§4). Scrubbing
  and loading are connector-side for exactly that reason: this side holds the
  Postgres credential and the service secret it already had, and posts
  scrubbed text to a service-authed route. It never speaks Bolt.

**What this is honestly not.** Pattern scrubbing is best-effort. A credential
pasted into a run with no recognisable shape, shorter than
``MIN_EXACT_MATCH_LEN``, and absent from the secret store, survives it. §6
point 2 states that residual plainly and names the layers that contain it
(the reserved unregistered bucket, service-caller-only routes, disjoint
labels and zero cross edges, no published Neo4j port) — and the real argument
is an equivalence: a scrubber miss is exposed only to callers who already
hold the service secret, which is the same population that can read the
unscrubbed original in Postgres. Scrubbing is what keeps that equivalence
from degrading when T2 widens the audience, and what keeps the backups from
carrying live tokens off the box.

**Never put a real credential in a test fixture, a file or an env var to
exercise this.** The tests next door build their own fake secret store with
values they invent; the workspace secret store is the only sanctioned source
of a real one, and it is read here and nowhere else.
"""
from __future__ import annotations

import logging
import os
import re
import sys
from typing import Any, Iterable

log = logging.getLogger("aw_apps.knowledgeable.exec_scrub")

#: Tool calls whose arguments are DROPPED WHOLESALE rather than scrubbed
#: (§6 point 3). Name and timestamp load; arguments never do. These tools'
#: arguments are a credential by definition rather than by accident, so there
#: is nothing a pattern needs to recognise — and `collect_secret`'s RESULT is
#: a secret value even though its argument is only a request id, which is why
#: it is here too.
#:
#: aw-knowledgeable declares the same set server-side
#: (`core/exec_history.py`'s `SECRET_TOOL_NAMES`). Two copies on purpose: this
#: one is the scrub, that one is the refusal — a connector that regressed must
#: not be able to land these arguments just because it forgot.
SECRET_TOOL_NAMES = frozenset(
    {
        "read_secret",
        "collect_secret",
        "write_secret",
        "delete_secret",
    }
)

#: The shortest secret-store value this module will exact-match on.
#:
#: The trade this number makes, in both directions, because getting it wrong
#: is worse than not having the pass at all:
#:
#: * Too LOW and the redaction table starts containing short, low-entropy
#:   config values that happen to live in the secret store — `"true"`,
#:   `"8090"`, `"neo4j"`, a bucket slug. Those appear all over ordinary run
#:   text, and redacting them would shred the corpus this feature exists to
#:   make searchable, while *looking* like thorough scrubbing.
#: * Too HIGH and a genuinely short credential falls through to the pattern
#:   pass alone.
#:
#: 10 characters, plus the "not a bare number/boolean" filter below. A
#: credential shorter than that is covered only if it has a recognisable
#: shape — stated here rather than discovered later.
MIN_EXACT_MATCH_LEN = 10

#: Values never worth exact-matching even above the length floor: they are
#: configuration that happens to be stored as a secret, and they recur in
#: ordinary text. Matched case-insensitively against the WHOLE value.
_EXACT_MATCH_DENYLIST = frozenset(
    {
        "true",
        "false",
        "none",
        "null",
        "localhost",
        "127.0.0.1",
        "neo4j",
        "postgres",
        "default",
        "changeme",
        "password",
    }
)

_REDACTION = "<REDACTED:{kind}>"


def _redaction(kind: str) -> str:
    return _REDACTION.format(kind=kind)


# ---------------------------------------------------------------------------
# Pattern families (§6 point 1b) — for secrets NOT in the store.
#
# Every entry is (kind, compiled regex, replacement template). The regex MUST
# keep whatever context makes the match readable afterwards — a scrubbed run
# that reads `NEO4J_AUTH=<REDACTED:env-assignment>` is still useful to an
# agent, and one that reads `<REDACTED>` has lost the fact that a Neo4j auth
# var was being set at all. Retrieval value and redaction are not in tension
# here; destroying the context is just sloppiness.
#
# Ordering matters. The longest, most specific shapes run FIRST, so a bearer
# token inside a curl header is redacted as a header rather than half-eaten by
# the bare-token pattern and left recognisable.
# ---------------------------------------------------------------------------

_PATTERNS: tuple[tuple[str, re.Pattern[str], str], ...] = (
    # A PEM block, whole. First because it spans lines and contains base64
    # that several patterns below would otherwise chew through.
    (
        "private-key",
        re.compile(
            r"-----BEGIN [A-Z ]*PRIVATE KEY-----.*?-----END [A-Z ]*PRIVATE KEY-----",
            re.DOTALL,
        ),
        _redaction("private-key"),
    ),
    # `Authorization: Bearer …` / `Authorization: Basic …`, in a header, a
    # curl `-H` argument, or a logged request. The scheme survives; the
    # credential does not.
    (
        "authorization-header",
        re.compile(
            r"(?i)(authorization\s*[:=]\s*[\"']?\s*(?:bearer|basic|token)\s+)[A-Za-z0-9\-._~+/=]{8,}"
        ),
        r"\1" + _redaction("authorization-header"),
    ),
    # Any `X-…-Secret`/`X-…-Key`/`X-…-Token` header — `X-Internal-Secret` is
    # the one this estate actually passes around, but the family is the same.
    (
        "secret-header",
        re.compile(
            r"(?i)(x-[a-z0-9\-]*(?:secret|key|token|password)\s*[:=]\s*[\"']?)[^\s\"',;}\]]{6,}"
        ),
        r"\1" + _redaction("secret-header"),
    ),
    # A shell/env assignment whose NAME says credential. Covers
    # `NEO4J_AUTH=neo4j/hunter2` (user/pass in one value — the real shape, and
    # the reason the whole right-hand side goes, not just a `password=` tail),
    # `PGPASSWORD=…`, `export GITHUB_TOKEN=…`, `-e FOO_SECRET=…`.
    (
        "env-assignment",
        re.compile(
            r"(?i)\b([A-Z][A-Z0-9_]*(?:AUTH|PASSWORD|PASSWD|SECRET|TOKEN|APIKEY|API_KEY|KEY|CREDENTIALS?)\s*=\s*)"
            r"[\"']?[^\s\"'&;|]{4,}"
        ),
        r"\1" + _redaction("env-assignment"),
    ),
    # A JSON/YAML/kwargs field whose KEY says credential:
    # `"password": "hunter2"`, `service_secret='…'`, `api_key: abc`. Written
    # to keep the key and the quoting so the surrounding JSON stays readable.
    (
        "credential-field",
        re.compile(
            r"(?i)([\"']?\b(?:password|passwd|secret|service_secret|token|access_token|"
            r"refresh_token|api_key|apikey|auth_token|client_secret|private_key|"
            r"credential)\b[\"']?\s*[:=]\s*[\"']?)[^\s\"',;}\]]{4,}"
        ),
        r"\1" + _redaction("credential-field"),
    ),
    # URL userinfo — `https://user:token@host/…`. The host survives (it is
    # the useful half); the credential does not.
    #
    # The username part is `{0,256}`, not `{1,256}`: `redis://:password@host`
    # — no user, just a password — is the shape this estate actually uses
    # (see the Redis 2-arg-AUTH scars), and a `{1,256}` username requirement
    # MEASURED as missing it entirely. An empty-username URL is the easiest
    # one to get wrong and the one most likely to be in a run.
    (
        "url-userinfo",
        re.compile(r"(?i)\b([a-z][a-z0-9+.\-]*://)[^\s/:@]{0,256}:[^\s/@]{1,256}@"),
        r"\1" + _redaction("url-userinfo") + "@",
    ),
    # `curl -u user:password`. Two separate entries rather than one
    # alternation: a single pattern would need `(?i)` twice (illegal
    # mid-expression) or two optional groups whose replacement has to guess
    # which one matched — and a replacement that guesses wrong emits the
    # redaction with the credential still attached.
    (
        "cli-credential-flag",
        re.compile(r"(?i)(\s-u\s+[^\s:]{1,128}:)[^\s\"']{4,}"),
        r"\1" + _redaction("cli-credential-flag"),
    ),
    # `--password hunter2`, `--token=…`. Long forms only.
    (
        "cli-credential-flag",
        re.compile(
            r"(?i)(\s--(?:password|passwd|token|api-key|apikey|secret)[=\s]+)"
            r"[\"']?[^\s\"']{4,}"
        ),
        r"\1" + _redaction("cli-credential-flag"),
    ),
    # `-p <password>` — but ONLY after a command that actually takes a
    # password in that position. A bare `-p` pattern is unusable and was
    # MEASURED to be so before this was narrowed: it redacted
    # `docker run -p 7687:7687` and `ssh -p 2222`, two of the most common
    # shapes in this corpus. A scrubber that shreds ordinary text is one that
    # gets turned off, and then there is no scrubber at all — so the price of
    # covering this flag is naming the commands, and a command not named here
    # is covered by the exact-match pass instead (the real Neo4j password IS
    # in the secret store) rather than by a guess.
    (
        "cli-credential-flag",
        re.compile(
            r"(?i)\b((?:cypher-shell|neo4j-admin|mysql|mysqldump|redis-cli)"
            r"(?:\s+(?!-p\b)[^\s]+){0,12}\s+-p[=\s]*)[\"']?[^\s\"']{4,}"
        ),
        r"\1" + _redaction("cli-credential-flag"),
    ),
    # `basic auth: <base64>` quoted in prose or a log line — base64 of
    # `user:password`, which is a credential in full. Anchored on the word
    # `basic` and floored at 16 base64 characters, because an unanchored
    # base64 pattern would eat every hash, id and payload in the corpus.
    (
        "basic-credentials",
        re.compile(r"(?i)\b(basic(?:\s+auth)?\s*:?\s+)[A-Za-z0-9+/]{16,}={0,2}"),
        r"\1" + _redaction("basic-credentials"),
    ),
    # A bare `Bearer <token>` with no `Authorization:` in front of it — how
    # a token appears when an agent quotes one in prose or a log line echoes
    # it. Floored at 16 characters so `Bearer token` in English prose, or a
    # short placeholder like `Bearer abc.def.ghi`, is left alone: below that
    # length it is not a credential worth the false positives.
    (
        "bearer-token",
        re.compile(r"(?i)\b(bearer\s+)[A-Za-z0-9\-._~+/=]{16,}"),
        r"\1" + _redaction("bearer-token"),
    ),
    # A JWT, anywhere — three base64url segments. `eyJ` is the `{"` header
    # every one of them starts with.
    (
        "jwt",
        re.compile(r"\beyJ[A-Za-z0-9_\-]{8,}\.[A-Za-z0-9_\-]{8,}\.[A-Za-z0-9_\-]{8,}"),
        _redaction("jwt"),
    ),
    # Provider-prefixed tokens, bare. The prefix IS the shape, so these are
    # the highest-confidence patterns in the file and need no surrounding
    # context to be certain.
    (
        "provider-token",
        re.compile(
            r"\b("
            r"sk-ant-[A-Za-z0-9\-_]{16,}"
            r"|sk-proj-[A-Za-z0-9\-_]{16,}"
            r"|sk-[A-Za-z0-9]{20,}"
            r"|github_pat_[A-Za-z0-9_]{20,}"
            r"|gh[pousr]_[A-Za-z0-9]{20,}"
            r"|glpat-[A-Za-z0-9\-_]{16,}"
            r"|xox[baprs]-[A-Za-z0-9\-]{10,}"
            r"|AKIA[0-9A-Z]{16}"
            r"|ASIA[0-9A-Z]{16}"
            r"|AIza[0-9A-Za-z\-_]{30,}"
            r"|hf_[A-Za-z0-9]{20,}"
            r"|npm_[A-Za-z0-9]{30,}"
            r"|dop_v1_[a-f0-9]{32,}"
            r"|shpat_[a-f0-9]{24,}"
            r"|pypi-[A-Za-z0-9\-_]{32,}"
            r")\b"
        ),
        _redaction("provider-token"),
    ),
)


def _apply_patterns(text: str) -> str:
    for _kind, pattern, replacement in _PATTERNS:
        text = pattern.sub(replacement, text)
    return text


# ---------------------------------------------------------------------------
# Exact-match pass (§6 point 1a) — the strong half.
# ---------------------------------------------------------------------------


def _usable_exact_values(values: Iterable[str]) -> list[str]:
    """Filter a raw set of secret values down to the ones worth exact-matching,
    longest first.

    Longest first is not cosmetic: a secret store commonly holds both a token
    and a derived value containing it (a DSN holding a password, a combined
    ``user/pass``). Replacing the short one first leaves the long one
    partially redacted and still recognisable — and worse, still greppable
    for the half that survived.
    """
    usable: set[str] = set()
    for raw in values:
        if not isinstance(raw, str):
            continue
        value = raw.strip()
        if len(value) < MIN_EXACT_MATCH_LEN:
            continue
        if value.lower() in _EXACT_MATCH_DENYLIST:
            continue
        # A bare number or a version string is configuration, not a secret,
        # and it recurs in ordinary run text.
        if re.fullmatch(r"[\d.]+", value):
            continue
        usable.add(value)
    return sorted(usable, key=len, reverse=True)


def workspace_secret_values() -> tuple[list[str], str | None]:
    """Every value in the workspace secret store, for the redaction table.

    Returns ``(values, error)``. A failure here is **never** fatal and is
    **never** silent: the caller reports `exact_match_unavailable` on the sync
    status so a degraded scrub is visible as degraded, rather than running
    pattern-only while looking identical to a full pass. Silent degradation
    is this workspace's named failure mode; a scrubber is the last place to
    reproduce it.

    This reads ACROSS app namespaces, which the per-app ``ctx.secrets``
    facade deliberately does not allow, and that is a privileged read worth
    naming rather than burying:

    * the only purpose is REMOVAL — the values are compiled into a local
      replacement table and are never logged, never returned to a caller,
      never sent anywhere, and never written to disk;
    * no exception from this module ever carries a value (see
      :func:`Scrubber.scrub`'s contract and the tests next door, which assert
      the values do not appear in any output or in ``repr``);
    * scoping it to this app's own namespace would reduce the table to the
      connector's own two secrets, which would make §6 point 1a's "far
      stronger guarantee than patterns" false while still claiming it.
    """
    root = os.environ.get("AW_WORKSPACE_CONTAINER_DIR", "/opt/aw-workspace")
    if root not in sys.path:
        # Insert rather than setting PYTHONPATH — memory
        # container-pythonpath-breaks-throwaway-venvs: setting that env var
        # breaks site-packages resolution for venvs in this estate.
        sys.path.insert(0, root)
    try:
        from src.apps import paths  # noqa: PLC0415
        from src.apps.secret_store import SecretStore  # noqa: PLC0415
    except Exception as exc:  # pragma: no cover - import shape, not logic
        return [], f"secret store unavailable: {type(exc).__name__}"

    try:
        secrets_dir = paths.secrets_dir()
        store = SecretStore()
        values: list[str] = []
        namespaces = [
            name[: -len(".json")]
            for name in sorted(os.listdir(secrets_dir))
            if name.endswith(".json")
        ]
        for namespace in namespaces:
            for key in store.keys(namespace):
                try:
                    value = store.get(namespace, key)
                except Exception:
                    # One undecryptable entry (a key rotation, a hand-edited
                    # file) must not cost the whole table.
                    continue
                if value:
                    values.append(value)
    except Exception as exc:
        return [], f"secret store read failed: {type(exc).__name__}"
    return values, None


class Scrubber:
    """A compiled redaction table plus the pattern pass.

    Built once per sync tick, not per row: the exact-match table is the
    expensive half, and a tick scrubs thousands of rows.

    The instance holds secret values in memory, so it deliberately has no
    ``__repr__``/``__str__`` that could render them, and no accessor that
    returns them. A traceback through this object must not become the leak
    the object exists to prevent.
    """

    __slots__ = ("_exact", "_exact_error", "_exact_count")

    def __init__(
        self, secret_values: Iterable[str] | None = None, *, exact_error: str | None = None
    ) -> None:
        self._exact = _usable_exact_values(secret_values or [])
        self._exact_error = exact_error
        self._exact_count = len(self._exact)

    @classmethod
    def from_workspace(cls) -> "Scrubber":
        values, error = workspace_secret_values()
        if error:
            log.warning("exec scrub: exact-match pass degraded — %s", error)
        return cls(values, exact_error=error)

    def __repr__(self) -> str:
        # Count and status only — never a value, never a prefix of one.
        return f"Scrubber(exact_values={self._exact_count}, degraded={bool(self._exact_error)})"

    __str__ = __repr__

    @property
    def status(self) -> dict[str, Any]:
        """What the sync tick reports (§8's observability): how many values
        the exact pass is matching on, and whether it is degraded. A count,
        never a value."""
        return {
            "exact_values": self._exact_count,
            "exact_match_unavailable": self._exact_error,
            "pattern_families": len(_PATTERNS),
        }

    def scrub(self, text: Any) -> Any:
        """Redact ``text``. Non-strings pass through unchanged (``None`` is a
        legitimate empty run output, not a thing to stringify).

        Exact matches run FIRST so a known-live secret is labelled as such
        and cannot be half-eaten by a pattern. Replacement is irreversible in
        the graph; Postgres keeps the originals (§5).
        """
        if not isinstance(text, str) or not text:
            return text
        out = text
        for value in self._exact:
            if value in out:
                out = out.replace(value, _redaction("secret-store"))
        return _apply_patterns(out)

    def scrub_tool_args(self, *, name: str | None, args_text: Any) -> Any:
        """§6 point 3 — a secret-handling tool's arguments are dropped
        wholesale, not scrubbed. Everything else goes through
        :meth:`scrub`."""
        if name in SECRET_TOOL_NAMES:
            return None
        return self.scrub(args_text)
