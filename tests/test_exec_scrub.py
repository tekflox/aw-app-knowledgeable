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


def test_aws_sdk_access_key_and_secret_key_field_names():
    """QA run 4c86d34ebf4e4857a6a92086608861ec: the literal AWS SDK field
    names, in the two separator styles that actually appear — a bare
    `access_key=` assignment and a JSON-style `"secret_key": …` field. The
    design's own §6 names S3 as where the nightly dumps go, which is what
    makes this shape directly relevant rather than generic."""
    out = _scrubber().scrub(
        "access_key=AKIAIOSFODNN7INVENTED\n"
        '{"secret_key": "wJalrXUtnFEMIinventedinventedBPxRfiCYINVENTEDKEY"}'
    )
    for leaked in ("AKIAIOSFODNN7INVENTED", "wJalrXUtnFEMIinventedinventedBPxRfiCYINVENTEDKEY"):
        assert leaked not in out, out


def test_oauth_client_secret_key_field_name():
    """`client_secret_key` — the OAuth field name QA found passing through.
    `client_secret` already matched; the `_key` suffix defeated the `\\b`
    word boundary that family relies on."""
    out = _scrubber().scrub('client_secret_key=invented-oauth-client-secret-key-value')
    assert "invented-oauth-client-secret-key-value" not in out, out


def test_access_key_and_secret_key_field_names_do_not_catch_db_column_names():
    """The companion trap check for the family above: `primary_key`,
    `foreign_key` and `cache_key` are column names, not AWS field names, and
    must survive untouched — these alternatives are literal matches on
    `access_key`/`secret_key`/`client_secret_key`, not a generic `key`
    suffix, for exactly this reason."""
    text = "primary_key: id, foreign_key: user_id, cache_key: abcde12345, api version 1.2"
    assert _scrubber().scrub(text) == text


def test_abbreviated_db_pass_assignment():
    """`DB_PASS=` — the abbreviated form QA found missing; the existing
    `PASSWORD`/`PASSWD` suffixes only cover the spelled-out names."""
    out = _scrubber().scrub(
        "docker run -d -e DB_PASS=invented-db-pass-value -e DB_HOST=db neo4j:5"
    )
    assert "invented-db-pass-value" not in out, out
    assert "DB_PASS=<REDACTED:env-assignment>" in out
    assert "DB_HOST=db" in out


def test_db_pass_does_not_catch_words_that_merely_end_in_pass():
    """The companion trap check, and the same shape as the bare `-p` trap one
    level up: a plain `PASS` suffix would also fire on `BYPASS=`/`COMPASS=`/
    `SURPASS=`, none of which are credentials. `DB_PASS` is matched as a
    literal name, not a generic suffix, for exactly this reason."""
    text = "BYPASS=true SURPASS=1 COMPASS_HEADING=270 ENCOMPASS_ALL=false"
    assert _scrubber().scrub(text) == text


def test_slack_webhook_url():
    """The URL itself is the credential — anyone holding it can post as the
    integration — so a pattern that only strips URL userinfo (the
    `url-userinfo` family) does nothing here; the whole URL must go."""
    out = _scrubber().scrub(
        "alerting posts to https://hooks.slack.com/services/T000INVENTED/"
        "B000INVENTED/invented1234567890ABCDEFinventedXYZ when the sync fails"
    )
    assert "T000INVENTED" not in out
    assert "invented1234567890ABCDEFinventedXYZ" not in out
    assert "<REDACTED:webhook-url>" in out
    assert "when the sync fails" in out


def test_discord_webhook_url():
    out = _scrubber().scrub(
        "mirrored to https://discord.com/api/webhooks/123456789012345678/"
        "inventedABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890invented for the on-call channel"
    )
    assert "123456789012345678" not in out
    assert "inventedABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890invented" not in out
    assert "<REDACTED:webhook-url>" in out
    assert "on-call channel" in out


def test_ordinary_urls_are_not_mistaken_for_webhooks():
    """The companion trap check: an ordinary GitHub/Slack-domain URL with no
    webhook path must survive — the families above anchor on the specific
    `/services/|/workflows/|/triggers/` and `/api/webhooks/` paths, not on
    the bare hostname."""
    for text in (
        "see https://github.com/fredericowu/aw-workspace for details",
        "join the workspace at https://aw-workspace.slack.com/archives/C0123",
        "https://discord.com/invite/abcdef is the support server",
    ):
        assert _scrubber().scrub(text) == text


def test_stripe_underscore_style_secret_keys():
    """`sk_live_`/`sk_test_` — only the hyphenated `sk-` form was covered
    before this. Pasted bare in prose, the shape an agent actually emits.

    The prefix and suffix are concatenated rather than one literal: a
    contiguous fixture in this exact shape trips GitHub push protection's
    Stripe-key scanner on the source text itself, invented value or not —
    which is the same "don't write anything secret-shaped" discipline this
    module's own docstring asks for, just enforced one layer up.
    """
    for token in (
        "sk_live_" + "xk2mQpT9Zc4bWnJ8VhRu2Lsa",
        "sk_test_" + "qN7bYd3FtR8mWx2VhLp9KcZa",
    ):
        out = _scrubber().scrub(f"the Stripe key in this env is {token} — rotate it")
        assert token not in out, out
        assert "<REDACTED:provider-token>" in out
        assert "rotate it" in out


def test_ordinary_mentions_of_live_and_test_are_not_mistaken_for_stripe_keys():
    """The companion trap check: `sk_live_`/`sk_test_` must stay anchored to
    the actual prefix — ordinary prose that merely contains the words `live`
    and `test`, or a short/incomplete prefix, must survive."""
    for text in (
        "this build is live, that other one is just a test run",
        "sk_test_ on its own is not a key",
        "desk_testing_fixtures.py failed on CI",
    ):
        assert _scrubber().scrub(text) == text


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


#: The gateway prefixes measured in the production graph, 2026-10-19, via
#: `search_execution_graph(search_in='name')` — the three real spellings the
#: old `if name in SECRET_TOOL_NAMES` silently failed to match. Pinned as
#: data rather than prose: a fourth spelling appearing is a one-line change
#: here, and the row that proves the fix covers it.
_MEASURED_PREFIXES = (
    "mcp__aw-gateway__aw__aw_secrets__",
    "aw-gateway.aw__aw_secrets__",
    "mcp__workspace-gateway__aw__aw_secrets__",
)


@pytest.mark.parametrize("prefix", _MEASURED_PREFIXES)
@pytest.mark.parametrize("bare", sorted(exec_scrub.SECRET_TOOL_NAMES))
def test_a_gateway_prefixed_secret_tool_is_dropped_too(prefix, bare):
    """**§13.2 — the hole this test exists for was live, not hypothetical.**

    The drop was `if name in SECRET_TOOL_NAMES`, an exact match on the BARE
    name, and every real MCP-routed call arrives prefixed. So prefixed
    `write_secret` arguments landed in the production graph with `args_text`
    intact — and because `write_secret` writes to the VAULT, those values
    were vault-only (the exact pass cannot see them, §6 point 1) and
    shapeless (no pattern fires). Three defences, all missed, on the one
    channel whose arguments are a credential by definition.

    MUTATION: restore `if name in SECRET_TOOL_NAMES` in `scrub_tool_args`
    and all 12 of these go red while the bare-name test above stays green —
    which is exactly how the bug shipped.
    """
    args = '{"name": "invented_token_name", "value": "invented-value-that-must-not-land"}'
    assert _scrubber().scrub_tool_args(name=prefix + bare, args_text=args) is None


@pytest.mark.parametrize(
    "name",
    [
        "spread_secret",
        "misread_secret",
        "mcp__aw-gateway__aw__aw_secrets__spread_secret",
        "aw-gateway.aw__misread_secret",
        "unwrite_secret",
        "precollect_secret",
        "undelete_secret",
    ],
)
def test_a_tool_whose_name_merely_ends_with_a_secret_tool_is_not_dropped(name):
    """The rule is terminal-segment EQUALITY, not `endswith` on the bare
    name — `spread_secret` ends with `read_secret` and has nothing to do
    with the secret store.

    This is the other half of the fix, and it is not pedantry: dropping the
    arguments of an unrelated tool destroys retrieval value silently, and
    the §13.2 lesson ("a tool's identity is its terminal segment") is a
    statement about identity in both directions. The sloppy
    `name.endswith(bare)` form passes every test above and fails every one
    here.

    MUTATION: change `is_secret_tool` to
    `any(name.endswith(b) for b in SECRET_TOOL_NAMES)` -> red here, green
    everywhere else.
    """
    args = '{"command": "echo hello"}'
    out = _scrubber().scrub_tool_args(name=name, args_text=args)
    assert out is not None, f"{name} is not a secret-store tool — its args must be kept"
    assert "echo hello" in out


@pytest.mark.parametrize(
    ("name", "expected"),
    [
        ("write_secret", "write_secret"),
        ("mcp__aw-gateway__aw__aw_secrets__write_secret", "write_secret"),
        ("aw-gateway.aw__aw_secrets__write_secret", "write_secret"),
        ("mcp__workspace-gateway__aw__aw_secrets__write_secret", "write_secret"),
        # Either separator can be the last one, in either order — the two
        # rsplit passes have to land after whichever came last.
        ("mcp__gw__aw.read_secret", "read_secret"),
        ("a.b__c.delete_secret", "delete_secret"),
        ("a__b.c__collect_secret", "collect_secret"),
        ("", ""),
        (None, ""),
    ],
)
def test_tool_name_segment_is_the_text_after_the_last_separator(name, expected):
    """The helper on its own, because `scrub_tool_args` can only show it
    through a boolean and a wrong segment that happens not to be in the set
    looks identical to a right one."""
    assert exec_scrub.tool_name_segment(name) == expected


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


def _install_fake_secret_store(monkeypatch, tmp_path, store: dict[str, dict[str, str]]):
    """Inject a fake `src.apps.paths` / `src.apps.secret_store` into
    `sys.modules`.

    `src` is **aw-workspace core**, which lives OUTSIDE this repo — this app
    is Tier-1 and imports it at runtime from the server process, but the
    release workflow checks out only this repo and installs only
    `pytest jsonschema fastapi httpx uvicorn`. A test that imported `src`
    for real therefore passed locally and went red in CI on the first push
    (`ModuleNotFoundError: No module named 'src'`) — the sibling-import trap
    this estate has hit before.

    Skipping the test in CI was the wrong fix: `workspace_secret_values` is
    the privileged cross-namespace read, and an untested privileged read is
    the thing to avoid most here. So the dependency is faked instead, which
    also tests the part that actually matters — that it ENUMERATES every
    namespace rather than returning this app's own secrets.
    """
    import sys
    import types

    secrets_dir = tmp_path / "secrets"
    secrets_dir.mkdir(exist_ok=True)
    for namespace, entries in store.items():
        (secrets_dir / f"{namespace}.json").write_text("{}")

    paths_mod = types.ModuleType("src.apps.paths")
    paths_mod.secrets_dir = lambda: str(secrets_dir)

    class _FakeStore:
        def keys(self, namespace):
            return list(store.get(namespace, {}))

        def get(self, namespace, key):
            value = store.get(namespace, {}).get(key)
            if value == "__undecryptable__":
                raise ValueError("fernet: invalid token")
            return value

    store_mod = types.ModuleType("src.apps.secret_store")
    store_mod.SecretStore = _FakeStore

    src_pkg = types.ModuleType("src")
    src_pkg.__path__ = []  # a package, so `src.apps` is a legal submodule
    apps_pkg = types.ModuleType("src.apps")
    apps_pkg.__path__ = []
    # Set the submodules as ATTRIBUTES of their parent as well as in
    # sys.modules. `from src.apps import paths` resolves by getattr on the
    # parent package first, and a pre-seeded sys.modules entry does NOT get
    # that attribute set for it (the import machinery returns early from the
    # cache without touching the parent) — so without these two lines the
    # fake only works when the REAL `src.apps` happens to be importable too,
    # which is the exact condition this fake exists to remove.
    src_pkg.apps = apps_pkg
    apps_pkg.paths = paths_mod
    apps_pkg.secret_store = store_mod
    for name, mod in (
        ("src", src_pkg),
        ("src.apps", apps_pkg),
        ("src.apps.paths", paths_mod),
        ("src.apps.secret_store", store_mod),
    ):
        monkeypatch.setitem(sys.modules, name, mod)


def test_workspace_secret_values_reads_every_namespace(tmp_path, monkeypatch):
    """The cross-namespace read, proven to actually enumerate rather than
    returning this app's own secrets — which is what would quietly make §6
    point 1a's "far stronger guarantee than patterns" false while still
    claiming it."""
    _install_fake_secret_store(
        monkeypatch,
        tmp_path,
        {
            "knowledgeable": {"service_secret": "invented-connector-secret-value"},
            "git": {"github_token": "invented-git-token-value-here"},
            "aws": {"access_key": "invented-aws-value-here"},
        },
    )
    values, error = exec_scrub.workspace_secret_values()
    assert error is None
    assert "invented-connector-secret-value" in values
    assert "invented-git-token-value-here" in values, (
        "the read is scoped to one namespace — the exact-match table would then "
        "hold only this app's own secrets"
    )
    assert "invented-aws-value-here" in values


def test_one_undecryptable_entry_does_not_cost_the_whole_table(tmp_path, monkeypatch):
    """A key rotation or a hand-edited secrets file leaves one entry that
    cannot be decrypted. Losing the whole redaction table over it would turn
    a cosmetic problem into a pattern-only scrub, silently."""
    _install_fake_secret_store(
        monkeypatch,
        tmp_path,
        {
            "git": {"broken": "__undecryptable__", "github_token": "invented-still-readable"},
        },
    )
    values, error = exec_scrub.workspace_secret_values()
    assert error is None
    assert "invented-still-readable" in values


def test_the_real_workspace_secret_store_satisfies_the_contract_this_fake_assumes(tmp_path):
    """The fake above is only honest if the real class has the shape it
    imitates. Run against aw-workspace core when it IS importable (a local
    run in the workspace container) and skipped when it is not (this repo's
    own CI, which checks out only this repo) — so the fake can never drift
    from `SecretStore`'s real `keys`/`get` contract without someone noticing
    in the place where core is actually present.
    """
    pytest.importorskip(
        "src.apps.secret_store",
        reason="aw-workspace core is not on the path — this app's CI checks out only this repo",
    )
    from src.apps import paths  # noqa: PLC0415
    from src.apps.secret_store import SecretStore  # noqa: PLC0415

    assert callable(paths.secrets_dir)
    for method in ("keys", "get"):
        assert callable(getattr(SecretStore, method, None)), (
            f"SecretStore.{method} is gone — exec_scrub.workspace_secret_values "
            f"and its test fake both depend on it"
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
