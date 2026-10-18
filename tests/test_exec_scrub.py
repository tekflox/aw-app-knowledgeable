"""The scrubber, tested adversarially against REAL-SHAPED run payloads
(docs/design/execution-history-in-production-neo4j.md §6).

**The standard this file is held to, and why it is higher than the rest of
the suite.** This is the one module in the feature that can write a live
credential into a production graph that backs a publicly reachable app, whose
nightly dumps leave the box. A scrubber that passes on ``"password=hunter2"``
and misses the real shapes is worse than no scrubber at all, because it
manufactures confidence: the delivery report says "scrubbing is tested", the
graph says otherwise, and nobody looks again.

So every fixture below is shaped like something that is actually in the
corpus — a ``NEO4J_AUTH=user/pass`` inline in a Bash command, a bearer token
in a ``curl -H``, a token pasted into prose inside ``runs.input``, a
secret-store tool call's arguments, a DSN with URL userinfo, a PEM block —
not a toy string. The three "does it mangle ordinary text" tests at the
bottom are part of the same standard: a scrubber nobody can leave enabled
because it shreds the corpus is a scrubber that gets turned off.

**No real credential appears anywhere in this file.** Every secret value here
is invented for the test. The workspace secret store is the only sanctioned
source of a real one, and `exec_scrub` reads it in exactly one function that
is itself exercised here with a fake store on a temp dir.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from knowledgeable_app import exec_scrub  # noqa: E402

#: Invented, not real. Long and distinctive enough that a partial redaction
#: leaves an obvious, greppable remnant — which is what several assertions
#: below check for, rather than only checking the whole string is gone.
STORE_SECRET = "Zq7-neo4j-prod-password-invented-for-this-test"
STORE_TOKEN = "invented-store-token-aaaaaaaaaaaaaaaaaaaaaa"
#: A derived value that CONTAINS another stored value — the longest-first
#: ordering trap.
STORE_DSN = f"postgresql://aw:{STORE_SECRET}@aw-remote-host-postgres:5432/agents"


def _scrubber(values=(STORE_SECRET, STORE_TOKEN, STORE_DSN)):
    return exec_scrub.Scrubber(values)


def _assert_fully_gone(out: str, *secrets: str) -> None:
    """A secret is gone only if no recognisable FRAGMENT of it survives.

    Checking `secret not in out` alone is the weak form: a pattern that eats
    the first half leaves the second half in the graph, still greppable and
    often still enough to narrow a brute force. So every window of 12
    characters is checked too — if any survives, the redaction is partial and
    this fails.
    """
    for secret in secrets:
        assert secret not in out, f"the whole secret survived: {out!r}"
        window = 12
        for i in range(0, max(len(secret) - window + 1, 1)):
            frag = secret[i : i + window]
            if len(frag) < window:
                continue
            assert frag not in out, (
                f"a {window}-char fragment of the secret survived redaction "
                f"({frag!r}) — a partially redacted credential is still a leak: {out!r}"
            )


# ---------------------------------------------------------------------------
# Pass (a) — exact match against the workspace secret store. The strong half:
# it catches a credential with no recognisable shape at all.
# ---------------------------------------------------------------------------


def test_a_store_secret_pasted_into_run_prose_is_redacted():
    """The shape the design calls out explicitly: a human pasted a password
    into a dispatch, so it is in `runs.input` as prose with no key, no `=`,
    no provider prefix — nothing for a pattern to recognise. This is the case
    exact matching exists for, and the only case nothing else covers."""
    text = (
        "the neo4j container is up; its password is "
        f"{STORE_SECRET} — use cypher-shell from inside the container and "
        "report the SHOW DATABASES output"
    )
    out = _scrubber().scrub(text)
    _assert_fully_gone(out, STORE_SECRET)
    assert "<REDACTED:secret-store>" in out
    # The surrounding instruction survives — a scrubbed run must stay useful.
    assert "SHOW DATABASES" in out and "cypher-shell" in out


def test_the_longest_stored_value_is_replaced_first():
    """A DSN holding a password is a real secret-store shape. Redacting the
    short value first would leave the DSN partially redacted and still
    recognisable; longest-first is what prevents that."""
    out = _scrubber().scrub(f"export AW_PG_DSN; psql '{STORE_DSN}' -c 'select 1'")
    _assert_fully_gone(out, STORE_DSN, STORE_SECRET)


def test_a_store_secret_is_redacted_everywhere_it_appears():
    out = _scrubber().scrub(f"{STORE_TOKEN} ... and again {STORE_TOKEN}")
    _assert_fully_gone(out, STORE_TOKEN)
    assert out.count("<REDACTED:secret-store>") == 2


def test_short_and_low_entropy_store_values_are_not_exact_matched():
    """The corpus-shredding trap, from the other direction.

    A secret store holds configuration too — `true`, `8090`, `neo4j`, a
    bucket slug. Exact-matching those would redact them out of every run that
    merely mentions a port or a container name, which destroys the corpus
    this feature exists to make searchable while LOOKING like thorough
    scrubbing. The length floor plus the denylist is what stops that, and
    this is the test that keeps someone from "improving" the floor to 1.
    """
    scrubber = exec_scrub.Scrubber(["true", "8090", "neo4j", "5432", "1.2.3", "default"])
    text = "neo4j on 8090 with default bucket, version 1.2.3, enabled=true"
    assert scrubber.scrub(text) == text


def test_the_scrubber_never_renders_a_secret_in_repr_or_status():
    """A traceback through this object must not become the leak the object
    exists to prevent."""
    scrubber = _scrubber()
    for rendered in (repr(scrubber), str(scrubber), str(scrubber.status)):
        _assert_fully_gone(rendered, STORE_SECRET, STORE_TOKEN)
    assert scrubber.status["exact_values"] == 3
    assert scrubber.status["exact_match_unavailable"] is None


# ---------------------------------------------------------------------------
# Pass (b) — pattern families, on real run shapes. Every fixture here is
# something an agent actually emits.
# ---------------------------------------------------------------------------


def test_neo4j_auth_inline_in_a_bash_command():
    """The exact shape the design names. Note it is `user/pass` in ONE value:
    a scrubber that only redacts a `password=` tail leaves the password in
    full here, which is why the whole right-hand side of a credential-named
    assignment goes."""
    text = (
        "docker run -d --name k7-neo4j -e NEO4J_AUTH=neo4j/Wb8xQ-invented-pw "
        "-e NEO4J_server_memory_heap_max__size=2G neo4j:5.26.4-community"
    )
    out = _scrubber().scrub(text)
    assert "Wb8xQ-invented-pw" not in out
    assert "neo4j/Wb8xQ" not in out
    assert "NEO4J_AUTH=<REDACTED:env-assignment>" in out
    # The non-credential env var and the image tag survive — this is the
    # difference between a scrub and a shred.
    assert "neo4j:5.26.4-community" in out
    assert "NEO4J_server_memory_heap_max__size=2G" in out


def test_pgpassword_and_export_token_assignments():
    out = _scrubber().scrub(
        "PGPASSWORD=invented-pg-password-here psql -h aw-remote-host-postgres -U postgres\n"
        "export GITHUB_TOKEN=invented_gh_token_value_here\n"
        "export AW_WORKSPACE_API_KEY='invented-api-key-value'"
    )
    for leaked in (
        "invented-pg-password-here",
        "invented_gh_token_value_here",
        "invented-api-key-value",
    ):
        assert leaked not in out, out
    assert "psql -h aw-remote-host-postgres -U postgres" in out


def test_bearer_token_in_a_curl_header():
    """The second shape the design names."""
    text = (
        "curl -sS -H 'Authorization: Bearer eyJhbGciOiJFZERTQSIsImtpZCI6InRlc3Qt"
        "a2lkLTEifQ.eyJzdWIiOiI3IiwiZXhwIjo5OTk5OTk5OTk5fQ.invented-signature-part' "
        "-H 'Content-Type: application/json' https://aw.tekflox.com/api/runs"
    )
    out = _scrubber().scrub(text)
    assert "eyJzdWIiOiI3" not in out
    assert "invented-signature-part" not in out
    # The scheme and the URL survive: "this run called /api/runs with a
    # bearer token" is exactly the fact worth keeping.
    assert "Authorization: Bearer <REDACTED:authorization-header>" in out
    assert "https://aw.tekflox.com/api/runs" in out
    assert "Content-Type: application/json" in out


def test_internal_secret_header():
    out = _scrubber().scrub(
        "curl -H 'X-Internal-Secret: invented-service-secret-value' "
        "http://aw-knowledgeable:8090/api/documents"
    )
    assert "invented-service-secret-value" not in out
    assert "http://aw-knowledgeable:8090/api/documents" in out


def test_a_bare_jwt_in_prose():
    out = _scrubber().scrub(
        "the cookie is eyJhbGciOiJFZERTQSJ9.eyJzdWIiOiI5MTAwMSJ9.invented_sig_aaaa "
        "and it expires in an hour"
    )
    assert "eyJzdWIiOiI5MTAwMSJ9" not in out
    assert "<REDACTED:jwt>" in out
    assert "expires in an hour" in out


@pytest.mark.parametrize(
    "token",
    [
        "sk-ant-api03-inventedinventedinventedinvented",
        "sk-projinventedinventedinventedinvented12345",
        "sk-inventedinventedinventedinvented12345",
        "ghp_inventedinventedinventedinvented1234",
        "github_pat_11INVENTEDinventedinventedinvented",
        "glpat-inventedinvented1234",
        "xoxb-1234567890-invented-token",
        "AKIAIOSFODNN7INVENTD",
        "AIzaSyInventedInventedInventedInvented12",
        "hf_inventedinventedinventedinvented",
        "dop_v1_" + "a" * 40,
    ],
)
def test_provider_prefixed_tokens_are_redacted_bare(token):
    """These need no surrounding context: the prefix IS the shape. Tested one
    per family rather than one representative, because a regex alternation is
    exactly the kind of thing that loses a branch in an edit and nothing goes
    red."""
    out = _scrubber().scrub(f"I found this in the env: {token} — rotate it")
    assert token not in out, out
    assert "<REDACTED:provider-token>" in out
    assert "rotate it" in out


def test_url_userinfo_in_a_git_remote():
    out = _scrubber().scrub(
        "git push https://fredericowu:ghp_inventedinventedinventedinvent@github.com/"
        "fredericowu/knowledge-base-private.git main"
    )
    assert "ghp_invented" not in out
    assert "fredericowu:ghp_" not in out
    # The host and path survive — which repo was pushed to is the useful part.
    assert "github.com/fredericowu/knowledge-base-private.git" in out


def test_url_userinfo_with_no_username():
    """`redis://:password@host` — the shape this estate actually uses, and
    the one a `{1,256}` username requirement MEASURED as missing entirely
    before the pattern was widened."""
    out = _scrubber().scrub(
        "REDIS_URL check: redis://:invented-redis-password@aw-remote-host-redis:6379/0"
    )
    assert "invented-redis-password" not in out, out
    assert "aw-remote-host-redis:6379/0" in out


def test_a_bare_bearer_token_without_the_authorization_prefix():
    """How a token appears when an agent quotes one in prose rather than in a
    header — MEASURED as surviving before this family was added."""
    out = _scrubber().scrub(
        "it answered 401 until I passed Bearer aw_invented_bearer_token_value_here"
    )
    assert "aw_invented_bearer_token_value_here" not in out, out
    assert "401" in out


def test_the_word_bearer_in_ordinary_prose_is_left_alone():
    """The other half of the floor: `Bearer` followed by a short word is
    English, not a credential."""
    for text in (
        "the request needs a Bearer token",
        "Bearer abc.def.ghi is a placeholder",
    ):
        assert _scrubber().scrub(text) == text


def test_json_credential_fields():
    out = _scrubber().scrub(
        '{"service_secret": "invented-secret-field-value", '
        '"base_url": "http://aw-knowledgeable:8090", '
        '"api_key": "invented-api-key-field", "password":"invented-pw-field"}'
    )
    for leaked in (
        "invented-secret-field-value",
        "invented-api-key-field",
        "invented-pw-field",
    ):
        assert leaked not in out, out
    assert "http://aw-knowledgeable:8090" in out


def test_cli_credential_flags():
    out = _scrubber().scrub(
        "curl -u admin:invented-basic-password https://x/y\n"
        "cypher-shell -u neo4j --password invented-cypher-password 'RETURN 1'"
    )
    assert "invented-basic-password" not in out, out
    assert "invented-cypher-password" not in out, out
    assert "RETURN 1" in out


def test_a_pem_private_key_block():
    out = _scrubber().scrub(
        "wrote the deploy key:\n"
        "-----BEGIN OPENSSH PRIVATE KEY-----\n"
        "b3BlbnNzaC1rZXktdjEAAAAABG5vbmUAAAAEbm9uZQAAAAAAAAABAAAAMwAAAAtzc2gtZW\n"
        "INVENTEDKEYMATERIALINVENTEDKEYMATERIALINVENTEDKEYMATERIAL\n"
        "-----END OPENSSH PRIVATE KEY-----\n"
        "then ran ssh-add"
    )
    assert "INVENTEDKEYMATERIAL" not in out
    assert "b3BlbnNzaC1rZXktdjEA" not in out
    assert "<REDACTED:private-key>" in out
    assert "then ran ssh-add" in out


# ---------------------------------------------------------------------------
# §6 point 3 — secret-tool arguments are DROPPED, not scrubbed.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("name", sorted(exec_scrub.SECRET_TOOL_NAMES))
def test_secret_tool_arguments_are_dropped_wholesale(name):
    """Not scrubbed — dropped. These arguments are a credential by
    definition rather than by accident, so there is nothing for a pattern to
    have to recognise, and `collect_secret`'s own arguments are only a request
    id while its RESULT is the value — which is why it is in the set too."""
    args = '{"name": "neo4j_password", "value": "invented-value-that-must-not-land"}'
    assert _scrubber().scrub_tool_args(name=name, args_text=args) is None


def test_ordinary_tool_arguments_are_scrubbed_not_dropped():
    """The drop must not spread: a `Bash` call's arguments are most of the
    measurement's retrieval value, so they are scrubbed and kept."""
    args = '{"command": "docker run -e NEO4J_AUTH=neo4j/invented-pw neo4j:5"}'
    out = _scrubber().scrub_tool_args(name="Bash", args_text=args)
    assert out is not None
    assert "invented-pw" not in out
    assert "docker run" in out


def test_a_store_secret_inside_tool_args_is_exact_matched_too():
    """The two passes compose on tool arguments exactly as they do on run
    text — a stored secret pasted into a Bash command has no pattern shape
    once it is a bare positional argument."""
    args = f'{{"command": "cypher-shell -p {STORE_SECRET} \'MATCH (n) RETURN count(n)\'"}}'
    out = _scrubber().scrub_tool_args(name="Bash", args_text=args)
    _assert_fully_gone(out, STORE_SECRET)
    assert "MATCH (n) RETURN count(n)" in out


# ---------------------------------------------------------------------------
# Composition on a whole, realistically-shaped run payload — the thing the
# sync actually hands over, not a single field.
# ---------------------------------------------------------------------------


def test_a_whole_run_payload_is_scrubbed_across_every_text_field():
    scrubber = _scrubber()
    run = {
        "id": "run-abc",
        "input_text": (
            "the prod neo4j password is "
            f"{STORE_SECRET}; verify SHOW DATABASES from inside the container"
        ),
        "output_text": (
            "ran: docker exec aw-stack-aw-neo4j-1 cypher-shell -u neo4j "
            "--password invented-inline-password 'SHOW DATABASES'\n"
            "also pushed with Authorization: Bearer eyJhbGciOiJFZERTQSJ9."
            "eyJzdWIiOiI3In0.invented_sig"
        ),
    }
    toolcalls = [
        {"name": "read_secret", "args_text": '{"name": "neo4j_password"}'},
        {
            "name": "Bash",
            "args_text": '{"command": "PGPASSWORD=invented-pg psql -c \'select 1\'"}',
        },
    ]

    scrubbed_run = {k: scrubber.scrub(v) for k, v in run.items()}
    scrubbed_calls = [
        {**tc, "args_text": scrubber.scrub_tool_args(name=tc["name"], args_text=tc["args_text"])}
        for tc in toolcalls
    ]

    blob = str(scrubbed_run) + str(scrubbed_calls)
    _assert_fully_gone(blob, STORE_SECRET)
    for leaked in ("invented-inline-password", "invented_sig", "invented-pg", "eyJzdWIiOiI3In0"):
        assert leaked not in blob, blob
    # Retrieval value preserved: every one of these is what a later question
    # would be asked in terms of.
    assert "SHOW DATABASES" in blob
    assert "aw-stack-aw-neo4j-1" in blob
    assert "read_secret" in blob  # the NAME still loads
    assert scrubbed_calls[0]["args_text"] is None


# ---------------------------------------------------------------------------
# It must not shred ordinary text. A scrubber that does gets turned off, and
# then there is no scrubber at all.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "text",
    [
        "git log --oneline -5 && git status --short",
        "the run failed with HTTP 404 at /api/exec/runs/abc-123",
        "set EXTRACTION_ENABLED=false in the compose file",
        "MATCH (d:Document {tenant: $tenant_id}) RETURN count(d)",
        "docker inspect -f '{{.NetworkSettings.IPAddress}}' aw-stack-aw-neo4j-1",
        "the chunk_embedding_index has dimension 1024 and the model produces 1024",
        "pytest backend/tests/test_tenant_isolation.py -q -k k5",
        # The `-p` false positives, MEASURED before the pattern was narrowed:
        # a bare `-p` rule redacted all three of these. They are among the
        # most common shapes in this corpus, so they are pinned rather than
        # left to the next author's judgement.
        "docker run -d -p 7687:7687 neo4j:5.26.4-community",
        "ssh -p 2222 ubuntu@aw-remote-host",
        "git log -p -- backend/app/core/graph.py",
        "kubectl port-forward svc/neo4j -p 7474:7474",
    ],
)
def test_ordinary_run_text_passes_through_untouched(text):
    assert _scrubber().scrub(text) == text, "the scrubber mangled ordinary run text"


def test_the_short_p_flag_is_only_redacted_after_a_command_that_takes_a_password():
    """Both halves of the narrowing, so neither can be lost in an edit: the
    flag IS redacted after `cypher-shell`, and is NOT after `docker run`."""
    scrubber = _scrubber()
    redacted = scrubber.scrub("cypher-shell -u neo4j -p invented-cypher-pw 'RETURN 1'")
    assert "invented-cypher-pw" not in redacted, redacted
    assert "RETURN 1" in redacted

    kept = "docker run -d -p 7687:7687 -e FOO=bar neo4j:5"
    assert scrubber.scrub(kept) == kept


def test_a_flag_assignment_that_is_not_a_credential_survives():
    """`EXTRACTION_ENABLED=false` and `UI_V2=1` are assignments whose names do
    not say credential. The `env-assignment` pattern keys on the NAME for
    exactly this reason — keying on "has an `=`" would redact half the
    corpus."""
    text = "UI_V2=1 EXTRACTION_ENABLED=false WORKERS=10 aw-workspace-cli restart core"
    assert _scrubber().scrub(text) == text


def test_non_strings_pass_through():
    scrubber = _scrubber()
    assert scrubber.scrub(None) is None
    assert scrubber.scrub("") == ""
    assert scrubber.scrub(17) == 17


def test_base64_basic_credentials_quoted_in_prose():
    out = _scrubber().scrub("it worked with basic auth: aW52ZW50ZWQ6cGFzc3dvcmQxMjM0")
    assert "aW52ZW50ZWQ6cGFzc3dvcmQxMjM0" not in out, out


def test_a_base64_looking_id_is_not_redacted_just_for_being_base64ish():
    """The other half of anchoring on the word `basic`: an unanchored base64
    pattern would eat every hash, run id and payload in the corpus."""
    for text in (
        "AW_RUN_ID=ad80f065d600497a8a373417d9cb2640",
        "content_hash a3f1c92b4d5e6f708192a3b4c5d6e7f8",
        "page_id=3fd5bf3b-9510-814c-be85-c2f9723dca01",
    ):
        assert _scrubber().scrub(text) == text


# ---------------------------------------------------------------------------
# The residual, RECORDED rather than hoped away (§6 point 2).
#
# These are the shapes pattern scrubbing does not catch, asserted as the
# current behaviour so the gap is visible in the test file instead of being
# discovered later by someone who read "scrubbing is tested" and assumed it
# meant complete. Each one is covered by the exact-match pass IF the value is
# in the workspace secret store — which is the whole reason that pass exists
# and is the stronger of the two.
#
# If a future change catches one of these, this test goes red. That is the
# intended signal: update it, and say so — do not delete it.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "text",
    [
        # A credential in prose with no key, no delimiter and no prefix.
        "the password is hunter2",
        # `pass:` deliberately NOT in the credential-field key list — in run
        # output it is overwhelmingly a test verdict ("pass: 3 of 5"), and
        # redacting those would shred the corpus for a shape the exact-match
        # pass already covers.
        "pass: s3cr3t-value-here",
        # An invented, shapeless token with no surrounding context at all.
        "try zq7tbx4n9k2m with the api",
    ],
)
def test_documented_residual_prose_secrets_survive_pattern_scrubbing(text):
    """A shapeless credential with no context survives the PATTERN pass.

    That is §6 point 2's stated residual, and the layers that contain it are
    structural, not textual: the reserved unregistered bucket (no
    human-reachable read path), service-caller-only routes, disjoint labels
    with zero cross edges, and no published Neo4j port. The actual safety
    argument is an equivalence — a miss is exposed only to callers who
    already hold the service secret, the same population that can read the
    unscrubbed original in Postgres.
    """
    assert _scrubber(values=[]).scrub(text) == text


@pytest.mark.parametrize(
    "secret",
    ["hunter2-but-in-the-store", "s3cr3t-value-here-in-store", "zq7tbx4n9k2m-stored"],
)
def test_the_same_shapeless_secrets_ARE_caught_when_they_are_in_the_store(secret):
    """The point of the pass above: shapelessness is irrelevant to exact
    matching. This is why §6 point 1a calls the store pass a far stronger
    guarantee than patterns, and why `workspace_secret_values` reads across
    app namespaces rather than just this app's own."""
    out = exec_scrub.Scrubber([secret]).scrub(f"the value is {secret}, use it")
    _assert_fully_gone(out, secret)
    assert "<REDACTED:secret-store>" in out


# ---------------------------------------------------------------------------
# The store read itself, and its degraded path. Exercised against a FAKE
# store on a temp dir — never the real one.
# ---------------------------------------------------------------------------


def test_workspace_secret_values_reads_every_namespace(tmp_path, monkeypatch):
    """The cross-namespace read, proven to actually enumerate rather than
    returning this app's own two secrets (which is what would quietly make
    §6 point 1a's "far stronger guarantee than patterns" false)."""
    from src.apps import paths
    from src.apps.secret_store import SecretStore

    secrets_dir = tmp_path / "secrets"
    secrets_dir.mkdir()
    monkeypatch.setattr(paths, "secrets_dir", lambda: str(secrets_dir))
    monkeypatch.setenv("AW_WORKSPACE_SECRET_KEY", "lQ4u5Y1xqQ8PmKZ2nQ6rF3hT7bV9wX0cD1eG2jL5aM8=")

    store = SecretStore()
    store.put("knowledgeable", "service_secret", "invented-connector-secret-value")
    store.put("git", "github_token", "invented-git-token-value-here")

    values, error = exec_scrub.workspace_secret_values()
    assert error is None
    assert "invented-connector-secret-value" in values
    assert "invented-git-token-value-here" in values, (
        "the read is scoped to one namespace — the exact-match table would then "
        "hold only this app's own secrets"
    )


def test_a_broken_secret_store_degrades_visibly_and_never_fatally(monkeypatch):
    """Silent degradation is this workspace's named failure mode, and a
    scrubber is the last place to reproduce it. A store that cannot be read
    must still scrub by pattern, and must SAY it is degraded."""
    monkeypatch.setattr(
        exec_scrub, "workspace_secret_values", lambda: ([], "secret store read failed: OSError")
    )
    scrubber = exec_scrub.Scrubber.from_workspace()
    assert scrubber.status["exact_match_unavailable"] == "secret store read failed: OSError"
    assert scrubber.status["exact_values"] == 0
    # Still scrubs what it can.
    out = scrubber.scrub("NEO4J_AUTH=neo4j/invented-pw")
    assert "invented-pw" not in out
