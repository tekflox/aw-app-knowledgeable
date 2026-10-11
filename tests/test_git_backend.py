"""docs/design/aw-knowledgeable-fs-sync.md §16 — the git backend engine,
exercised directly against a real local `git init --bare` repo (a tmp
directory, never the live `.aw-workspace/knowledge_base/` — see
`_workspace_home` below). No network, no GitHub: the remote is a plain
filesystem path, so credential injection never engages (that's
`_classify_push_failure`'s and `_run`'s job, covered separately with no
git subprocess involved for the former)."""
from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from knowledgeable_app import bulk_ingest, git_backend  # noqa: E402


@pytest.fixture(autouse=True)
def _workspace_home(tmp_path, monkeypatch):
    """Every test gets its own throwaway `AW_WORKSPACE_HOME` — never the
    real, in-use KB mirror. See skill guidance: an agent wiped the live
    tree this same week trying to "prove" a fix this way."""
    monkeypatch.setenv("AW_WORKSPACE_HOME", str(tmp_path / "workspace_home"))
    return tmp_path


def _sync(remote_url: str, branch: str, token: str, workspace_slug: str) -> dict | None:
    """Wraps `sync_tick` with the exact three values `bulk_ingest.py`'s own
    call site threads in — `kb_root()`, `BUCKET`, `SKIP_PREFIXES` — so this
    file never hand-copies them (the one place that would actually matter:
    §16.10 risk 4)."""
    return git_backend.sync_tick(
        bulk_ingest.kb_root(), remote_url, branch, token, workspace_slug,
        bulk_ingest.BUCKET, bulk_ingest.SKIP_PREFIXES,
    )


def _bare_remote(tmp_path) -> str:
    remote = tmp_path / "remote.git"
    subprocess.run(["git", "init", "-q", "--bare", str(remote)], check=True)
    return str(remote)


def _write_kb_file(relpath: str, content: str) -> None:
    path = bulk_ingest.kb_root() / relpath
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def _git(args: list[str], cwd) -> subprocess.CompletedProcess:
    return subprocess.run(["git", *args], cwd=str(cwd), capture_output=True, text=True, check=True)


# ---------------------------------------------------------------------------
# gitignore ≡ SKIP_PREFIXES — §16.10 risk 4
# ---------------------------------------------------------------------------


def test_gitignore_body_matches_skip_prefixes_exactly():
    """MUTATION: add a new entry to SKIP_PREFIXES without regenerating
    this and the next sync silently swallows the new code-map tree — this
    test is the drift tripwire."""
    body = git_backend.gitignore_body(bulk_ingest.SKIP_PREFIXES)
    ignored = {
        line[1:] for line in body.splitlines() if line.startswith("/") and not line.startswith("//")
    }
    assert ignored == set(bulk_ingest.SKIP_PREFIXES)


# ---------------------------------------------------------------------------
# remote URL validation — §16.3
# ---------------------------------------------------------------------------


def test_validate_remote_url_rejects_userinfo():
    with pytest.raises(git_backend.GitBackendError) as exc:
        git_backend._validate_remote_url("https://user:ghp_token@github.com/f/k.git")
    assert exc.value.state == git_backend.STATE_AUTH_FAILED


def test_validate_remote_url_accepts_plain_https():
    git_backend._validate_remote_url("https://github.com/fredericowu/knowledge-base-private.git")


# ---------------------------------------------------------------------------
# push-failure classification — pure function, no subprocess (§16.4)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "stderr,expected",
    [
        (
            "remote: Invalid username or token. Password authentication is not supported.\n"
            "fatal: Authentication failed for 'https://github.com/f/k.git/'\n",
            git_backend.STATE_AUTH_FAILED,
        ),
        (
            "! [rejected]        main -> main (fetch first)\n"
            "error: failed to push some refs to 'https://github.com/f/k.git'\n"
            "hint: Updates were rejected because the remote contains work that you do\n"
            "hint: not have locally.\n",
            git_backend.STATE_DIVERGED,
        ),
        (
            "fatal: unable to access 'https://github.com/f/k.git/': "
            "Could not resolve host: github.com\n",
            git_backend.STATE_PUSH_FAILED,
        ),
    ],
)
def test_classify_push_failure(stderr, expected):
    assert git_backend._classify_push_failure(stderr) == expected


# ---------------------------------------------------------------------------
# sync_tick — disabled / no_secret
# ---------------------------------------------------------------------------


def test_sync_tick_returns_none_when_remote_url_is_blank():
    assert _sync("", "main", "", "aw") is None


def test_sync_tick_reports_no_secret_and_keeps_the_commit_local(tmp_path):
    remote = _bare_remote(tmp_path)
    _write_kb_file("crispal/a.md", "hello")

    result = _sync(remote, "main", "", "aw")

    assert result["state"] == git_backend.STATE_NO_SECRET
    assert result["committed"] is True
    # The commit happened locally even though nothing was pushed.
    root = bulk_ingest.kb_root()
    log = _git(["log", "--oneline"], root)
    assert log.stdout.strip() != ""
    # But the bare remote never received it.
    remote_log = subprocess.run(
        ["git", "log", "--oneline", "main"], cwd=remote, capture_output=True, text=True
    )
    assert remote_log.returncode != 0 or remote_log.stdout.strip() == ""


# ---------------------------------------------------------------------------
# sync_tick — happy path against a real local bare repo
# ---------------------------------------------------------------------------


def test_sync_tick_first_run_inits_commits_and_pushes(tmp_path):
    remote = _bare_remote(tmp_path)
    _write_kb_file("crispal/a.md", "hello")
    _write_kb_file("mapped_folders/repos/some-repo/code.md", "generated code map — must be ignored")

    result = _sync(remote, "main", "dummy-token", "aw")

    assert result == {"state": git_backend.STATE_OK, "detail": "pushed", "committed": True}

    root = bulk_ingest.kb_root()
    assert (root / ".git").is_dir()
    assert (root / ".gitignore").is_file()
    marker = json.loads((root / git_backend.MARKER_FILENAME).read_text())
    assert marker["workspace_slug"] == "aw"
    assert marker["bucket"] == bulk_ingest.BUCKET

    tracked = _git(["ls-tree", "-r", "--name-only", "HEAD"], root).stdout.splitlines()
    assert "crispal/a.md" in tracked
    assert ".gitignore" in tracked
    assert git_backend.MARKER_FILENAME in tracked
    assert not any(p.startswith("mapped_folders/repos/") for p in tracked)

    subject = _git(["log", "-1", "--format=%s"], root).stdout.strip()
    assert subject.startswith("kb-sync: +")
    assert "[aw " in subject

    remote_tracked = subprocess.run(
        ["git", "show", "main:crispal/a.md"], cwd=remote, capture_output=True, text=True
    )
    assert remote_tracked.returncode == 0
    assert remote_tracked.stdout == "hello"


def test_sync_tick_second_run_with_no_changes_is_a_cheap_noop_push(tmp_path):
    remote = _bare_remote(tmp_path)
    _write_kb_file("crispal/a.md", "hello")
    first = _sync(remote, "main", "dummy-token", "aw")
    assert first["state"] == git_backend.STATE_OK

    second = _sync(remote, "main", "dummy-token", "aw")
    assert second["state"] == git_backend.STATE_OK
    assert second["committed"] is False


def test_sync_tick_picks_up_a_new_file_on_the_next_tick(tmp_path):
    remote = _bare_remote(tmp_path)
    _write_kb_file("crispal/a.md", "hello")
    _sync(remote, "main", "dummy-token", "aw")

    _write_kb_file("memory/b.md", "world")
    result = _sync(remote, "main", "dummy-token", "aw")

    assert result["state"] == git_backend.STATE_OK
    assert result["committed"] is True
    remote_tracked = subprocess.run(
        ["git", "show", "main:memory/b.md"], cwd=remote, capture_output=True, text=True
    )
    assert remote_tracked.stdout == "world"


# ---------------------------------------------------------------------------
# diverged — foreign non-empty repo, and a marker from another workspace
# ---------------------------------------------------------------------------


def test_sync_tick_refuses_a_foreign_non_empty_remote(tmp_path):
    """§16.5 risk 2 — a remote with existing, unrelated history (e.g. a
    human created it with a README) rejects our first push as
    non-fast-forward; classified as diverged, never forced."""
    remote = _bare_remote(tmp_path)
    clone_dir = tmp_path / "external_clone"
    subprocess.run(["git", "clone", "-q", remote, str(clone_dir)], check=True)
    (clone_dir / "README.md").write_text("a human's existing project", encoding="utf-8")
    subprocess.run(["git", "add", "README.md"], cwd=clone_dir, check=True)
    subprocess.run(
        ["git", "-c", "user.name=human", "-c", "user.email=human@example.com", "commit", "-q", "-m", "init"],
        cwd=clone_dir,
        check=True,
    )
    subprocess.run(["git", "push", "-q", "origin", "HEAD:main"], cwd=clone_dir, check=True)

    _write_kb_file("crispal/a.md", "hello")
    result = _sync(remote, "main", "dummy-token", "aw")

    assert result["state"] == git_backend.STATE_DIVERGED
    assert result["committed"] is True  # the local snapshot commit still happens (§16.6)


def test_sync_tick_refuses_when_the_local_marker_belongs_to_another_workspace(tmp_path):
    """§16.5 risk 1 — two workspaces (a copy-pasted config, a cloned
    workspace) pointed at the same local knowledge_base/ tree."""
    remote = _bare_remote(tmp_path)
    _write_kb_file("crispal/a.md", "hello")
    _sync(remote, "main", "dummy-token", "other-workspace")

    _write_kb_file("memory/b.md", "world")
    result = _sync(remote, "main", "dummy-token", "aw")

    assert result["state"] == git_backend.STATE_DIVERGED
    assert "other-workspace" in result["detail"]
    assert result["committed"] is False
    # Nothing new got staged/committed for the mismatched caller.
    root = bulk_ingest.kb_root()
    log = _git(["log", "--oneline"], root).stdout.splitlines()
    assert len(log) == 1
