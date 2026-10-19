"""The §13.6 event-kind registry (docs/design/execution-history-in-
production-neo4j.md §13.1 / §13.6 / §13.12).

Three things this file proves, and each one is a different way the registry
could rot into review that never happened:

* **A structured row's field list cannot drift from the real payload.** Every
  `fields`/`count_fields` key is checked against a fixture whose SHAPE is the
  one measured in production on 2026-10-19 — so a field renamed upstream
  stops being projected loudly, here, instead of silently becoming `None` on
  113k nodes.
* **Every `text`-verdict row actually redacts.** A planted, invented
  credential in each measured text shape must come back redacted, through
  the same `build_event_row` the sync uses — not through the scrubber
  directly, because a scrubber the build path does not call is the same as no
  scrubber.
* **An unregistered kind degrades visibly and is not projected.** The
  open-set contract: DISTINCT kinds (§13.12 point 8), never row counts, and
  a kind with a row — of ANY verdict, `excluded` included — is a decision,
  not a gap.

**No real credential appears anywhere in this file** (§13.12 point 9). Every
value below is invented for the test, exactly the discipline
`test_exec_scrub.py` and `test_vault_coverage.py` already hold: a real value
in a fixture is precisely the leak this module exists to prevent, and it
would land in git, in CI logs, and in every clone.

**Payload SHAPES, however, are measured, not invented.** They were read off
the live `run_events.payload` key sets, tenant-wide, on 2026-10-19 — a
fixture whose shape is guessed would make the first assertion above a
tautology.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from knowledgeable_app import event_kinds, exec_scrub, exec_sync  # noqa: E402

#: An invented secret-store value, long enough to clear
#: `MIN_EXACT_MATCH_LEN` so the exact-match pass actually engages on it.
PLANTED_STORE_SECRET = "invented-store-value-for-the-event-kind-test"


def _scrubber() -> exec_scrub.Scrubber:
    return exec_scrub.Scrubber([PLANTED_STORE_SECRET])


# ---------------------------------------------------------------------------
# Measured payload shapes, invented values. One per kind that has a row.
#
# The key sets are the ones production actually carries (2026-10-19,
# tenant-wide, `json_object_keys` grouped by kind) INCLUDING keys the
# registry deliberately does not project — `node_start`'s `run_id`,
# `system.init`'s `tools`, `connector.timing`'s epochs. Those extras are not
# noise in the fixture: they are what makes the allowlist test mean
# something.
# ---------------------------------------------------------------------------

SAMPLED_PAYLOADS: dict[str, dict] = {
    "node_start": {
        "label": "Coder - Opus",
        "agent": "coder-opus",
        "provider": "runner",
        "model": "claude-runner-opus",
        "model_slug": "claude-runner-opus",
        "run_id": "0000000000000000000000000000beef",
        "parent_run_id": None,
    },
    "finalizing": {"tokens_in": 729420, "tokens_out": 6345, "cost_usd": 5.443619},
    "cli.first_byte": {},
    "system.init": {
        "session_id": "00000000-0000-0000-0000-000000000001",
        "model": "claude-opus-5",
        "cwd": "/opt/aw-workspace",
        "tools": ["Task", "Bash", "Read", "Edit", "Write"],
        "permission_mode": "bypassPermissions",
    },
    "done": {"status": "success"},
    "cli.error": {"subtype": "error_during_execution", "is_error": True},
    "error": {"error": "cancelled by user"},
    "auto_compact.blind": {
        "session_id": "00000000-0000-0000-0000-000000000002",
        "threshold": 500000,
        "completed_runs": 1,
    },
    "auto_compact.ineffective": {
        "session_id": "00000000-0000-0000-0000-000000000003",
        "threshold": 500000,
        "current_tokens": 652417,
        "post_compact_tokens": 639001,
        "seconds_since_compact": 1530,
        "cooldown_s": 1800,
    },
    "connector.timing": {
        "phase": "spawn_to_first_line",
        "ready_epoch": 1790190325.01533,
        "first_line_epoch": 1790190326.5393455,
        "duration_s": 1.524,
    },
    "container.started": {},
    "log": {"msg": "start workflow TEST · Echo parallel (smoke) (parallel)"},
    "cli.timeout": {"timeout_s": 600},
    # Reviewed, not shipped by this slice — shapes still pinned, so slice 2/3
    # starts from a measured fixture rather than from a fresh guess.
    "thinking": {"text": "I should read the design doc before editing."},
    "node_end": {
        "text": "done — the change is in graph.py",
        "tokens_in": 1000,
        "tokens_out": 2000,
        "cost_usd": 0.5,
        "run_id": "0000000000000000000000000000beef",
    },
    "tool_call": {"id": "toolu_invented", "name": "Read", "input": {"file_path": "/x"}},
    "tool_result": {"tool_use_id": "toolu_invented", "content": "file contents"},
    "llm_token": {"delta": "he"},
}


def _record(kind: str, payload: dict, *, event_id="ev-1", run_id="run-1"):
    """One row in `SQL_EVENTS`' column order. Positional, like the real
    cursor — the column order IS the contract between the SQL and
    `build_event_row`, and a keyword fixture would stop checking it."""
    return (event_id, run_id, kind, "2026-10-19T10:00:00+00:00", "node-a", payload)


# ---------------------------------------------------------------------------
# The registry cannot drift from the payloads it claims to describe.
# ---------------------------------------------------------------------------


def test_every_measured_kind_has_a_registry_row():
    """The 18 kinds measured in production on 2026-10-19. A kind with no row
    is not projected and turns the doctor check red — which is the designed
    behaviour for a NEW kind, and would be a bug for one already measured."""
    assert set(SAMPLED_PAYLOADS) == set(event_kinds.EVENT_KIND_REGISTRY), (
        "registry and sampled payloads disagree: "
        f"unsampled={sorted(set(event_kinds.EVENT_KIND_REGISTRY) - set(SAMPLED_PAYLOADS))}, "
        f"unregistered={sorted(set(SAMPLED_PAYLOADS) - set(event_kinds.EVENT_KIND_REGISTRY))}"
    )


def test_the_shipped_set_is_exactly_slice_2():
    """Pinned by name. Slice 2 flips `thinking`/`node_end` to shipped (this
    change); slice 3 adds `tool_result` on another label — each a deliberate
    edit here plus its own scrub test, never a widening nothing pinned."""
    assert event_kinds.shipped_kinds() == (
        "auto_compact.blind",
        "auto_compact.ineffective",
        "cli.error",
        "cli.first_byte",
        "cli.timeout",
        "connector.timing",
        "container.started",
        "done",
        "error",
        "finalizing",
        "log",
        "node_end",
        "node_start",
        "system.init",
        "thinking",
    )


@pytest.mark.parametrize("kind", sorted(event_kinds.EVENT_KIND_REGISTRY))
def test_every_registry_field_exists_in_the_sampled_payload(kind):
    """§13.6's "every field list pins names that exist in sampled payloads".

    The failure this catches is quiet by nature: a field renamed upstream
    does not error, it projects `None` onto every event of that kind forever,
    and the graph looks populated.
    """
    spec = event_kinds.EVENT_KIND_REGISTRY[kind]
    payload = SAMPLED_PAYLOADS[kind]
    for prop, payload_key in spec.fields.items():
        assert payload_key in payload, (
            f"{kind}: registry reads {prop!r} from payload[{payload_key!r}], which is "
            f"not in the measured payload shape {sorted(payload)}"
        )
    for prop, payload_key in spec.count_fields.items():
        assert payload_key in payload, (
            f"{kind}: registry counts payload[{payload_key!r}] into {prop!r}, which is "
            f"not in the measured payload shape {sorted(payload)}"
        )
    if spec.text_field:
        assert spec.text_field in payload, (
            f"{kind}: registry reads text from payload[{spec.text_field!r}], which is "
            f"not in the measured payload shape {sorted(payload)}"
        )


@pytest.mark.parametrize("kind", sorted(event_kinds.EVENT_KIND_REGISTRY))
def test_every_registry_row_is_internally_coherent(kind):
    """The shape rules, so a hand-edited row cannot be half-written: a
    verdict from the three named states, a reason that actually says
    something, a `text` row with a text field and an `excluded`/`structured`
    row without one, and every property it names carrying a declared type."""
    spec = event_kinds.EVENT_KIND_REGISTRY[kind]
    assert spec.verdict in ("structured", "text", "excluded"), spec.verdict
    assert len(spec.reason) > 40, f"{kind}: a one-word reason is not a review"
    if spec.verdict == "text":
        assert spec.text_field, f"{kind}: verdict 'text' with no text_field"
    else:
        assert not spec.text_field or not spec.shipped, (
            f"{kind}: a shipped non-text verdict must not carry a text_field — that "
            f"is how unreviewed free text reaches the graph through the structured path"
        )
    if spec.verdict == "excluded":
        assert not spec.shipped, f"{kind}: excluded kinds are never shipped"
        assert not spec.fields and not spec.count_fields
    for prop in list(spec.fields) + list(spec.count_fields):
        assert prop in event_kinds.EVENT_FIELD_TYPES, (
            f"{kind}: property {prop!r} has no declared type in EVENT_FIELD_TYPES, so "
            f"`coerce` would drop it on every single event"
        )


def test_no_declared_type_is_unused():
    """The other direction: a type declared for a property no row fills is a
    leftover from a reverted edit, and it makes the type table read as a
    larger allowlist than the registry actually grants."""
    used = {
        prop
        for spec in event_kinds.EVENT_KIND_REGISTRY.values()
        for prop in list(spec.fields) + list(spec.count_fields)
    }
    assert set(event_kinds.EVENT_FIELD_TYPES) == used, (
        f"declared but unused: {sorted(set(event_kinds.EVENT_FIELD_TYPES) - used)}"
    )


# ---------------------------------------------------------------------------
# §13.6's open-set alarm.
# ---------------------------------------------------------------------------


def test_an_unregistered_kind_is_reported_as_unreviewed():
    seen = list(event_kinds.shipped_kinds()) + ["some.kind.invented.next.year"]
    assert event_kinds.unreviewed_kinds(seen) == ["some.kind.invented.next.year"]


def test_a_kind_with_any_verdict_is_not_unreviewed():
    """`excluded` is a DECISION, not a gap. If `llm_token` showed up as
    unreviewed the alarm would be permanently red over the single highest-
    volume kind in the table — and a permanently red doctor detects nothing,
    which is the silent degradation this whole mechanism exists to avoid."""
    assert event_kinds.unreviewed_kinds(["llm_token", "thinking", "tool_result"]) == []


def test_unreviewed_kinds_counts_distinct_kinds_not_rows():
    """§13.12 point 8 — a burst of ten thousand events of one new kind is ONE
    decision owed."""
    assert event_kinds.unreviewed_kinds(["new.kind"] * 10_000) == ["new.kind"]
    assert event_kinds.unreviewed_kinds([None, "", "new.kind"]) == ["new.kind"]


# ---------------------------------------------------------------------------
# §13.4's cap, and the ordering rule that makes it safe.
# ---------------------------------------------------------------------------


def test_cap_text_reports_the_true_length_and_the_cut():
    long = "y" * (event_kinds.EVENT_TEXT_MAX + 11)
    text, length, truncated = event_kinds.cap_text(long)
    assert len(text) == event_kinds.EVENT_TEXT_MAX
    assert length == len(long), "the TRUE length must survive, or a cut reads as short text"
    assert truncated is True


def test_cap_text_leaves_short_text_alone():
    text, length, truncated = event_kinds.cap_text("short")
    assert (text, length, truncated) == ("short", 5, False)
    assert event_kinds.cap_text(None) == (None, None, None)


@pytest.mark.parametrize("kind,text_field", [("error", "error"), ("thinking", "text"), ("node_end", "text")])
def test_a_planted_credential_straddling_the_cap_is_still_redacted(kind, text_field):
    """§13.12 point 6, as a test rather than a comment: the build path scrubs
    and THEN caps, so a credential lying ACROSS the cut cannot be bisected
    into a fragment that both passes then miss.

    **The planted value has to straddle the boundary, and the first version
    of this test did not** — found by QA on 2026-10-19, reversing the order
    in `build_event_row` and watching this test stay green. It planted the
    secret twice, once at index 0 and once past the cap, so the surviving
    prefix always held a WHOLE copy: the exact-match pass caught it whichever
    order ran, and a secret entirely beyond the cut is simply thrown away by
    either order too. Neither shape can distinguish them. Recorded here
    because the tautology was invisible from reading the test — it asserted
    something true for the wrong reason, which is the failure mode §13.12
    point 9's sibling rule exists for.

    The distinguishing case is a value cut IN HALF:

    * scrub-then-cap (correct) — the value becomes `<REDACTED:secret-store>`
      before any cut, so the truncation can only ever bisect the redaction
      MARKER. No part of the credential is in the graph.
    * cap-then-scrub (the bug) — the cut leaves a leading fragment of the
      real value. That fragment is not the stored value, so the exact-match
      pass does not match it, and a fragment has no shape, so no pattern
      fires either. It stays in the graph, and it stays greppable.

    So the assertion that carries this test is on the FRAGMENT, not on the
    whole value. MUTATION, run and measured rather than claimed: swapping
    `cap_text(scrubber.scrub(...))` to `scrubber.scrub(cap_text(...))` in
    `build_event_row` turns exactly this test red.

    Parametrised over `error` (slice 1, the original find) plus `thinking`
    and `node_end` (slice 2, this change) — the exact same straddle pattern,
    reused rather than re-derived, per the Architect's instruction that the
    cap-then-scrub bug is a property of `build_event_row` itself and not of
    any one kind. This matters most for slice 3: `tool_result` text
    routinely exceeds the cap, so the boundary is where that slice's
    credentials will actually sit.
    """
    # Half the value before the cut, half after it.
    straddle = len(PLANTED_STORE_SECRET) // 2
    padding = "p" * (event_kinds.EVENT_TEXT_MAX - straddle)
    # A tail, so the scrubbed text still exceeds the cap even though the
    # redaction marker is shorter than the value it replaces — otherwise the
    # correct order would not truncate at all and the two orders would again
    # be indistinguishable.
    payload = {text_field: f"{padding}{PLANTED_STORE_SECRET}{'t' * 100}"}
    row, dropped = exec_sync.build_event_row(_record(kind, payload), _scrubber())
    assert dropped == 0

    fragment = PLANTED_STORE_SECRET[:straddle]
    assert len(fragment) >= 8, "the fragment must be long enough to be a real leak"
    assert fragment not in row["text"], (
        "a leading FRAGMENT of the planted credential survived the cut — the cap "
        "ran before the scrubber, which is exactly the §13.12 point 6 bug: the "
        "half that survives matches no stored value and has no pattern shape"
    )
    assert PLANTED_STORE_SECRET not in row["text"]
    assert row["text_truncated"] is True
    assert row["text_len"] > event_kinds.EVENT_TEXT_MAX
    assert len(row["text"]) == event_kinds.EVENT_TEXT_MAX


def test_documented_residual_vault_style_shapeless_secret_survives_in_thinking():
    """§13.3's stated residual, honestly demonstrated rather than papered over.

    `exec_scrub.py`'s own docstring: the Scrubber's exact-match pass reads
    the per-app FILE secret store only. aw-vault (written by `write_secret`)
    is a second store this module never reads — its read path is
    approval-gated by design. So a vault-only credential that ALSO has no
    recognisable shape is caught by neither pass, for `thinking` exactly as
    for every other text kind. §13.3 decided to ship `thinking` anyway, on
    the strength of an equivalence argument (§13.5), not a claim of complete
    coverage — this test proves the limitation it accepted rather than
    pretending it doesn't exist.

    If a future change starts catching this shape, this test goes red: that
    is the intended signal. Update it and say so plainly — do not delete it.
    """
    # Invented, shapeless (no pattern family matches it), and deliberately
    # NOT in `_scrubber()`'s exact-match table — standing in for a secret
    # that lives only in the vault, which this Scrubber cannot see.
    shapeless = "zq9mxk2vtr8qln4"
    payload = {"text": f"the value I just read was {shapeless}, noting it for later"}
    row, dropped = exec_sync.build_event_row(_record("thinking", payload), _scrubber())
    assert dropped == 0
    assert shapeless in row["text"], (
        "a shapeless, vault-only-style secret survived scrubbing — this IS the "
        "documented §13.3 residual, not a regression; if this now fails, the "
        "residual has shrunk and the comment above should be updated to say so"
    )


# ---------------------------------------------------------------------------
# Every `text`-verdict kind has a planted-credential scrub test (§13.6).
# Invented shapes only.
# ---------------------------------------------------------------------------

#: Per text-bearing SHIPPED kind, the credential shapes its text realistically
#: carries, all invented. `error` gets the shapes §13.1 argues for it —
#: connection strings and DSNs, which is what a failed connect prints.
_PLANTED_TEXT_SHAPES = {
    "error": {
        "secret-store": (
            f"could not connect: password {PLANTED_STORE_SECRET} rejected"
        ),
        "url-userinfo": (
            "connection refused: bolt://neo4j:invented-event-test-pw@"
            "aw-stack-aw-neo4j-1:7687"
        ),
        "env-assignment": "startup failed with NEO4J_AUTH=neo4j/invented-event-test-pw",
        "credential-field": 'ingest rejected {"service_secret": "invented-event-test-sec"}',
        "authorization-header": (
            "upstream 401 for Authorization: Bearer invented.event.test.token"
        ),
    },
    "log": {
        "secret-store": f"booting worker with {PLANTED_STORE_SECRET}",
        "env-assignment": "exporting PGPASSWORD=invented-event-test-pg",
        "url-userinfo": "syncing from redis://:invented-event-test-redis@aw-redis:6379/0",
    },
    # Slice 2 — §13.3's restatement risk: a model can restate a secret it
    # just read, so `thinking` gets the same shape coverage as `error`.
    "thinking": {
        "secret-store": f"I should remember this value: {PLANTED_STORE_SECRET}",
        "url-userinfo": (
            "let me connect via bolt://neo4j:invented-thinking-test-pw@"
            "aw-stack-aw-neo4j-1:7687 first"
        ),
        "env-assignment": "the env had NEO4J_AUTH=neo4j/invented-thinking-test-pw set",
        "credential-field": (
            'the response looked like {"api_key": "invented-thinking-test-key"}'
        ),
        "authorization-header": (
            "the failing request sent Authorization: Bearer invented.thinking.test.token"
        ),
    },
    # Slice 2 — §13.12 point 3: `node_end.text` is the node's final OUTPUT,
    # the same restatement-risk class `output_text` already carries.
    "node_end": {
        "secret-store": f"final answer: authenticated with {PLANTED_STORE_SECRET}",
        "url-userinfo": (
            "wrote the result to bolt://neo4j:invented-nodeend-test-pw@"
            "aw-stack-aw-neo4j-1:7687"
        ),
        "env-assignment": "set NEO4J_AUTH=neo4j/invented-nodeend-test-pw in the output",
        "credential-field": 'returned {"token": "invented-nodeend-test-token"}',
        "authorization-header": (
            "sent Authorization: Bearer invented.nodeend.test.token to finish"
        ),
    },
}


@pytest.mark.parametrize(
    "kind",
    sorted(
        k
        for k, spec in event_kinds.EVENT_KIND_REGISTRY.items()
        if spec.verdict == "text" and spec.shipped
    ),
)
def test_every_shipped_text_kind_has_planted_credential_coverage(kind):
    """The registry's own requirement, enforced: a `text` row that ships
    without a planted-credential case here is an unverified claim that the
    scrubber covers its content."""
    assert kind in _PLANTED_TEXT_SHAPES, (
        f"{kind}: verdict 'text', shipped, and no planted-credential shapes in "
        f"_PLANTED_TEXT_SHAPES — add them with INVENTED values before shipping it"
    )


@pytest.mark.parametrize(
    "kind,family",
    sorted((k, f) for k, shapes in _PLANTED_TEXT_SHAPES.items() for f in shapes),
)
def test_a_planted_credential_in_event_text_is_redacted(kind, family):
    """Through `build_event_row`, deliberately — not through `Scrubber.scrub`
    directly. What is being tested is that the BUILD PATH routes this kind's
    text through the scrubber; a scrubber nothing calls is the same as no
    scrubber, which is the standard `test_exec_sync.py` already holds the run
    and tool-call paths to."""
    spec = event_kinds.EVENT_KIND_REGISTRY[kind]
    raw = _PLANTED_TEXT_SHAPES[kind][family]
    row, _dropped = exec_sync.build_event_row(
        _record(kind, {spec.text_field: raw}), _scrubber()
    )
    assert "<REDACTED:" in row["text"], f"{kind}/{family}: nothing was redacted in {row['text']!r}"
    assert PLANTED_STORE_SECRET not in row["text"]
    for leaked in (
        "invented-event-test-pw",
        "invented-event-test-sec",
        "invented.event.test.token",
        "invented-event-test-pg",
        "invented-event-test-redis",
        "invented-thinking-test-pw",
        "invented-thinking-test-key",
        "invented.thinking.test.token",
        "invented-nodeend-test-pw",
        "invented-nodeend-test-token",
        "invented.nodeend.test.token",
    ):
        assert leaked not in row["text"], (
            f"{kind}/{family}: {leaked!r} survived into {row['text']!r}"
        )


# ---------------------------------------------------------------------------
# `build_event_row` — the registry, applied.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("kind", sorted(event_kinds.shipped_kinds()))
def test_a_shipped_kind_projects_only_its_allowlisted_properties(kind):
    """§13.3's "no `payload_json` catch-all", per kind: the built row carries
    the five identity/text columns plus exactly the properties that kind's
    row names, and nothing the payload also happened to contain."""
    spec = event_kinds.EVENT_KIND_REGISTRY[kind]
    payload = SAMPLED_PAYLOADS[kind]
    row, dropped = exec_sync.build_event_row(_record(kind, payload), _scrubber())
    assert dropped == 0, f"{kind}: a measured payload should coerce cleanly"

    expected = {"id", "run_id", "kind", "ts", "node_id"}
    expected |= {p for p in spec.fields if payload.get(spec.fields[p]) is not None}
    expected |= set(spec.count_fields)
    if spec.text_field:
        expected |= {"text", "text_len", "text_truncated"}
    assert set(row) == expected, (
        f"{kind}: projected {sorted(set(row) - expected)} beyond its allowlist / "
        f"missed {sorted(expected - set(row))}"
    )
    assert "payload" not in row and "payload_json" not in row


def test_system_init_projects_a_tool_COUNT_and_never_the_tool_names():
    """§13.1's allowlist for the one kind the card named explicitly. The
    names are ~250 strings per run and the count is the retrieval-useful
    part; projecting the list would also put every tool a session was
    granted into the graph as text."""
    row, _ = exec_sync.build_event_row(
        _record("system.init", SAMPLED_PAYLOADS["system.init"]), _scrubber()
    )
    assert row["tools_count"] == 5
    assert "tools" not in row
    assert row["cwd"] == "/opt/aw-workspace"
    assert row["permission_mode"] == "bypassPermissions"
    assert row["text"] if False else "text" not in row


def test_an_added_upstream_field_is_invisible_by_construction():
    """The `system.init` env-snapshot scenario the card told the design to
    look for. The payload shape is owned by the upstream CLI; a field it
    starts emitting must not reach the graph until someone adds it to a
    registry row — so the allowlist, not a denylist, is the mechanism."""
    payload = dict(SAMPLED_PAYLOADS["system.init"])
    payload["env"] = {"AWS_SECRET_ACCESS_KEY": "invented-env-snapshot-value"}
    row, _ = exec_sync.build_event_row(_record("system.init", payload), _scrubber())
    assert "env" not in row
    assert "invented-env-snapshot-value" not in str(row)


def test_an_unshipped_or_unregistered_kind_projects_nothing_but_identity():
    """The defensive branch. `SQL_EVENTS` cannot return these — it binds the
    registry's shipped set — so this is about a hand-built row: it projects
    identity and NO payload-derived property, rather than guessing a shape.
    `tool_result` is the one that matters now that `node_end`/`thinking`
    shipped in slice 2: it has a `text_field` too (slice 3), and leaking its
    text a slice early is the same §13.12 point 3 trap, one kind over."""
    for kind in ("tool_call", "tool_result", "llm_token", "some.kind.invented.next.year"):
        payload = SAMPLED_PAYLOADS.get(kind, {"anything": "at all"})
        row, dropped = exec_sync.build_event_row(_record(kind, payload), _scrubber())
        assert set(row) == {"id", "run_id", "kind", "ts", "node_id"}, f"{kind}: {sorted(row)}"
        assert dropped == 0


def test_shipped_gate_gatekeeps_text_field_not_just_verdict():
    """§13.12 point 2 as the Architect restated it for this slice:
    `build_event_row` only honours a registry row's `text_field` when
    `shipped` is True — that condition is load-bearing, not symmetry.
    Flipping `thinking`/`node_end` to `shipped=True` must make their text
    actually flow, and `tool_result` — same verdict, same shape, still
    `shipped=False` (slice 3) — must still get none through the identical
    function."""
    for kind in ("thinking", "node_end"):
        spec = event_kinds.EVENT_KIND_REGISTRY[kind]
        assert spec.shipped is True, f"{kind}: slice 2 must flip shipped=True"
        row, dropped = exec_sync.build_event_row(
            _record(kind, SAMPLED_PAYLOADS[kind]), _scrubber()
        )
        assert dropped == 0
        assert row.get("text"), f"{kind}: text did not flow despite shipped=True"

    assert event_kinds.EVENT_KIND_REGISTRY["tool_result"].shipped is False
    row, dropped = exec_sync.build_event_row(
        _record("tool_result", SAMPLED_PAYLOADS["tool_result"]), _scrubber()
    )
    assert "text" not in row, "tool_result is shipped=False — its text must not land yet"
    assert dropped == 0


def test_nul_bytes_are_stripped_from_event_text_and_properties():
    """§13.12 point 5. A literal NUL is legal in the client-side-parsed
    payload and illegal as a Neo4j property, and it arrives when it arrives —
    the first N ticks passing proves nothing. So it is stripped from the text
    AND from every string property, not only from the one that looked risky.
    """
    row, _ = exec_sync.build_event_row(
        _record("error", {"error": "boom\x00 at the end"}), _scrubber()
    )
    assert "\x00" not in row["text"]
    row, _ = exec_sync.build_event_row(
        _record("node_start", {"agent": "coder\x00opus", "label": "L"}), _scrubber()
    )
    assert row["agent"] == "coderopus"


def test_an_uncoercible_field_is_dropped_and_counted_not_raised():
    """A shape change upstream must degrade visibly (one missing property,
    counted) and never fatally: raising here 400s the whole batch
    server-side, which parks the watermark behind it on every subsequent
    tick — a silent stall traded for a visible gap, the wrong way round."""
    row, dropped = exec_sync.build_event_row(
        _record("auto_compact.blind", {"threshold": {"not": "a number"}, "completed_runs": 2}),
        _scrubber(),
    )
    assert dropped == 1
    assert "threshold" not in row
    assert row["completed_runs"] == 2


def test_coerce_never_turns_a_boolean_into_a_number_or_back():
    """`bool` is a subclass of `int` in Python, so `int(True)` is 1 and a
    flag would quietly become a count. Both directions are a shape change
    and both are dropped."""
    assert event_kinds.coerce("is_error", True) is True
    assert event_kinds.coerce("threshold", True) is None
    assert event_kinds.coerce("tokens_in", "1000") == 1000
    assert event_kinds.coerce("cost_usd", "1.25") == 1.25
    assert event_kinds.coerce("cwd", {"a": 1}) is None
    assert event_kinds.coerce("not_a_declared_prop", "x") is None
    assert event_kinds.coerce("cwd", None) is None
