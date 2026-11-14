"""Tests for the row-level change feed (dataChange=true replay + paging)."""

from __future__ import annotations

import os
from collections import Counter
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from fastapi.testclient import TestClient

from app import change_feed, delta
from app.external_writer import append_rows, delete_files
from app.sample_data import create_sample_table


# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------


@pytest.fixture()
def table(tmp_path: Path) -> Path:
    t = tmp_path / "water_level"
    create_sample_table(t)
    return t


def _all_pages(table: Path, start: int, page_size: int, *, cursor: str | None = None):
    """Drain a round page by page, asserting the pinned bounds never move."""
    pages = []
    current = cursor
    while True:
        if current is None:
            page = change_feed.read_change_page(
                table, start_version=start, page_size=page_size
            )
        else:
            page = change_feed.read_change_page(table, cursor=current)
        pages.append(page)
        if not page["has_more"]:
            break
        current = page["next_cursor"]
    return pages


def _ids(page: dict) -> list[str]:
    return [r["record_id"] for r in page["records"]]


# ---------------------------------------------------------------------------
# Basic feed semantics
# ---------------------------------------------------------------------------


def test_feed_emits_only_data_change_actions(table: Path) -> None:
    page = change_feed.read_change_page(table, start_version=0, page_size=1000)
    assert page["start_version"] == 0
    assert page["end_version"] == 4
    assert page["count"] == 24
    assert page["has_more"] is False
    assert {r["change_type"] for r in page["records"]} == {"insert"}
    # Version 0 carries only protocol/metaData, so nothing for the feed.
    assert {r["version"] for r in page["records"]} == {1, 2, 3, 4}


def test_compaction_is_invisible_to_the_feed(table: Path) -> None:
    before = change_feed.read_change_page(table, start_version=0, page_size=1000)

    chosen = sorted(delta.load_snapshot(table).active)[:2]
    result = delta.compact(table, chosen)
    assert result["version"] == 5

    after = change_feed.read_change_page(table, start_version=0, page_size=1000)
    # Same records, same order -> the compaction replayed nothing.
    assert _ids(after) == _ids(before)
    assert after["records"] == before["records"]

    # Reading only the compaction version yields an empty page, not an error.
    only = change_feed.read_change_page(table, start_version=5, page_size=100)
    assert only["records"] == [] and only["has_more"] is False
    assert only["next_start_version"] == 6


def test_inserts_and_deletes_are_both_reported(table: Path) -> None:
    rows = [
        {"station": "ST-100", "utc_seconds": 1, "level_mm": 10},
        {"station": "ST-100", "utc_seconds": 2, "level_mm": 11},
    ]
    appended = append_rows(table, rows)
    victim = appended["file"]
    delete_files(table, [victim])

    page = change_feed.read_change_page(
        table, start_version=appended["version"], page_size=100
    )
    assert page["count"] == 4
    inserts = [r for r in page["records"] if r["change_type"] == "insert"]
    deletes = [r for r in page["records"] if r["change_type"] == "delete"]
    assert len(inserts) == 2 and len(deletes) == 2
    assert all(r["path"] == victim for r in page["records"])
    assert all(r["action"] == "add" for r in inserts)
    assert all(r["action"] == "remove" for r in deletes)
    # Same rows, opposite direction.
    assert [r["row"] for r in inserts] == rows
    assert [r["row"] for r in deletes] == rows


def test_delete_of_compaction_output_reads_removed_file(table: Path) -> None:
    """The compacted file is added with dataChange=false, then deleted later.

    That later delete *is* a data change, so its rows must be read back from
    the (still present) compacted parquet file.
    """
    chosen = sorted(delta.load_snapshot(table).active)[:2]
    compacted = delta.compact(table, chosen)

    # Feed replay up to and including the compaction adds nothing.
    page = change_feed.read_change_page(table, start_version=5, page_size=100)
    assert page["records"] == []

    deleted = delete_files(table, [compacted["output_file"]])
    page = change_feed.read_change_page(table, start_version=deleted["version"], page_size=100)
    assert page["count"] == compacted["rows"] == 12
    assert {r["change_type"] for r in page["records"]} == {"delete"}
    assert all(r["path"] == compacted["output_file"] for r in page["records"])
    assert {r["action_index"] for r in page["records"]} == {1}
    assert [r["row_index"] for r in page["records"]] == list(range(12))


def test_duplicate_rows_are_preserved(table: Path) -> None:
    dup = {"station": "ST-DUP", "utc_seconds": 7, "level_mm": 9}
    append_rows(table, [dup, dup, dup])

    page = change_feed.read_change_page(table, start_version=5, page_size=100)
    rows = [r["row"] for r in page["records"]]
    assert rows == [dup, dup, dup]
    # Three distinct record identities for the same content.
    assert len(set(_ids(page))) == 3


def test_record_identity_matches_log_position(table: Path) -> None:
    page = change_feed.read_change_page(table, start_version=0, page_size=1000)
    first = page["records"][0]
    actions = delta.read_log(table, first["version"])
    assert actions[first["action_index"]]["add"]["path"] == first["path"]
    assert first["record_id"] == (
        f"v{first['version']}-a{first['action_index']}-r{first['row_index']}"
    )

    # One fragmented file: per-row identities are consecutive and unique.
    frag = append_rows(
        table, [{"station": "ST-F", "utc_seconds": i, "level_mm": i} for i in range(5)]
    )
    page = change_feed.read_change_page(
        table, start_version=frag["version"], page_size=100
    )
    assert [r["row_index"] for r in page["records"]] == [0, 1, 2, 3, 4]
    assert len(set(_ids(page))) == 5


def test_missing_data_file_fails_instead_of_skipping(table: Path) -> None:
    appended = append_rows(
        table, [{"station": "ST-X", "utc_seconds": 1, "level_mm": 1}]
    )
    (table / appended["file"]).unlink()

    with pytest.raises(change_feed.DataFileUnavailableError):
        change_feed.read_change_page(table, start_version=0, page_size=100)
    # The page must not be reported as complete.
    with pytest.raises(change_feed.DataFileUnavailableError):
        change_feed.read_change_page(
            table, start_version=appended["version"], page_size=100
        )


def test_missing_removed_file_fails(table: Path) -> None:
    appended = append_rows(
        table, [{"station": "ST-X", "utc_seconds": 1, "level_mm": 1}]
    )
    delete_files(table, [appended["file"]])
    (table / appended["file"]).unlink()
    with pytest.raises(change_feed.DataFileUnavailableError):
        change_feed.read_change_page(
            table, start_version=appended["version"], page_size=100
        )


# ---------------------------------------------------------------------------
# Paging invariants
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("page_size", [1, 2, 5, 7, 24, 1000])
def test_paging_covers_every_row_exactly_once(table: Path, page_size: int) -> None:
    pages = _all_pages(table, 0, page_size)
    assert all(p["end_version"] == 4 for p in pages)
    ids = [rid for p in pages for rid in _ids(p)]
    assert len(ids) == 24
    assert len(set(ids)) == 24  # no row sent twice
    assert all(p["count"] <= page_size for p in pages)
    # has_more is exact, and next_start_version is stable per round.
    assert [p["has_more"] for p in pages[:-1]] == [True] * (len(pages) - 1)
    assert pages[-1]["has_more"] is False
    assert {p["next_start_version"] for p in pages} == {5}


def test_page_boundaries_split_files_and_rows(table: Path) -> None:
    pages = _all_pages(table, 0, 4)
    records = [r for p in pages for r in p["records"]]
    for page in pages:
        # A page may not repeat a record from the previous page.
        assert page["count"] <= 4
    by_version = Counter(r["version"] for r in records)
    assert by_version == {1: 6, 2: 6, 3: 6, 4: 6}
    # Row indexes for the same (version, action) form a complete 0..N-1 range.
    for (version, action_index), group in _group(records).items():
        assert [r["row_index"] for r in group] == list(range(len(group)))


def _group(records):
    grouped: dict[tuple[int, int], list[dict]] = {}
    for r in records:
        grouped.setdefault((r["version"], r["action_index"]), []).append(r)
    return grouped


def test_new_commits_wait_for_next_round(table: Path) -> None:
    first = change_feed.read_change_page(table, start_version=0, page_size=5)
    end = first["end_version"]
    assert first["has_more"]

    appended = append_rows(
        table, [{"station": "ST-NEW", "utc_seconds": 1, "level_mm": 1}]
    )
    assert appended["version"] > end

    pages = _all_pages(table, 0, 5, cursor=first["next_cursor"])
    ids = _ids(first) + [rid for p in pages for rid in _ids(p)]
    assert len(ids) == 24  # the new file is *not* in this round
    assert all(p["end_version"] == end for p in pages)

    # The next round starts right after the pinned bound and sees the append.
    nxt = change_feed.read_change_page(
        table, start_version=first["next_start_version"], page_size=100
    )
    assert nxt["end_version"] == appended["version"]
    assert [r["path"] for r in nxt["records"]] == [appended["file"]]


def test_cursor_is_replica_agnostic_and_restart_safe(tmp_path: Path) -> None:
    """A cursor carries all state: another process/instance resumes it."""
    table = tmp_path / "water_level"
    create_sample_table(table)
    first = change_feed.read_change_page(table, start_version=0, page_size=6)
    token = first["next_cursor"]

    # Simulate a different process: fresh import-free module state, same token.
    # (No in-memory session exists to lose - decode + read from disk only.)
    resumed = change_feed.read_change_page(table, cursor=token)
    assert resumed["start_version"] == 0 and resumed["end_version"] == 4
    assert resumed["records"][0]["version"] == 2  # continues at v2 row 0

    # Draining via a different page size keeps the exact same position.
    second = change_feed.read_change_page(table, cursor=token, page_size=2)
    third = change_feed.read_change_page(table, cursor=second["next_cursor"])
    assert _ids(resumed)[:2] == _ids(second)
    assert _ids(third) == _ids(resumed)[2:4]


def test_cursor_round_trip_is_deterministic(table: Path) -> None:
    page = change_feed.read_change_page(table, start_version=0, page_size=3)
    token = page["next_cursor"]
    assert change_feed.decode_cursor(token)["version"] == page["records"][-1]["version"]
    again = change_feed.read_change_page(table, cursor=token)
    assert again["records"] == change_feed.read_change_page(table, cursor=token)["records"]


def test_tampered_cursor_is_rejected(table: Path) -> None:
    token = change_feed.read_change_page(table, start_version=0, page_size=3)["next_cursor"]
    prefix, body, signature = token.split(".")

    with pytest.raises(delta.InvalidRequestError):
        change_feed.read_change_page(table, cursor="not-a-cursor")
    with pytest.raises(delta.InvalidRequestError):
        change_feed.read_change_page(table, cursor=f"{prefix}.{body}.{signature[:-2]}AA")
    with pytest.raises(delta.InvalidRequestError):
        change_feed.read_change_page(table, cursor=f"cf9.{body}.{signature}")

    other = change_feed.encode_cursor(
        {"start_version": 0, "end_version": 4, "page_size": 3,
         "version": 99, "action_index": 0, "row_index": 0}
    )
    with pytest.raises(delta.InvalidRequestError):
        change_feed.read_change_page(table, cursor=other)


def test_cursor_secret_env_must_match(table: Path, monkeypatch) -> None:
    token = change_feed.read_change_page(table, start_version=0, page_size=3)["next_cursor"]
    monkeypatch.setenv(change_feed.CURSOR_SECRET_ENV, "another-deployment")
    with pytest.raises(delta.InvalidRequestError):
        change_feed.read_change_page(table, cursor=token)
    # The same secret still works on the other replica.
    monkeypatch.delenv(change_feed.CURSOR_SECRET_ENV)
    assert change_feed.read_change_page(table, cursor=token)["count"] == 3


def test_first_request_validation(table: Path) -> None:
    with pytest.raises(delta.InvalidRequestError):
        change_feed.read_change_page(table)
    with pytest.raises(delta.InvalidRequestError):
        change_feed.read_change_page(table, start_version=-1)
    with pytest.raises(delta.InvalidRequestError):
        change_feed.read_change_page(table, start_version=0, page_size=0)
    with pytest.raises(delta.InvalidRequestError):
        change_feed.read_change_page(table, start_version=0, page_size=10**9)
    token = change_feed.read_change_page(table, start_version=0, page_size=2)["next_cursor"]
    with pytest.raises(delta.InvalidRequestError):
        change_feed.read_change_page(table, cursor=token, start_version=3)


def test_start_beyond_latest_is_an_empty_round(table: Path) -> None:
    page = change_feed.read_change_page(table, start_version=10, page_size=5)
    assert page["records"] == [] and page["has_more"] is False
    assert page["next_cursor"] is None
    assert page["latest_version"] == 4
    assert page["next_start_version"] == 10

def test_empty_delete_of_multiple_files(table: Path) -> None:
    a = append_rows(table, [{"station": "A", "utc_seconds": 1, "level_mm": 1}])
    b = append_rows(table, [{"station": "B", "utc_seconds": 2, "level_mm": 2}])
    deleted = delete_files(table, [a["file"], b["file"]])
    page = change_feed.read_change_page(
        table, start_version=deleted["version"], page_size=1
    )
    pages = _all_pages(table, deleted["version"], 1, cursor=page["next_cursor"])
    records = page["records"] + [r for p in pages for r in p["records"]]
    assert [r["path"] for r in records] == sorted([a["file"], b["file"]])
    assert all(r["version"] == deleted["version"] for r in records)
    assert [r["action_index"] for r in records] == [1, 2]


def test_paging_reads_only_needed_row_groups(
    table: Path, monkeypatch
) -> None:
    """A small page over a multi-row-group file must not decode the whole file."""
    rows = [
        {"station": "ST-RG", "utc_seconds": i, "level_mm": i} for i in range(10)
    ]
    rel = delta._new_data_filename()
    data = pa.Table.from_pylist(rows, schema=delta.TABLE_SCHEMA)
    pq.write_table(data, table / rel, row_group_size=3)
    now = 1
    delta._commit_actions(
        table,
        base_version=delta.load_snapshot(table).version,
        actions=[
            {"add": {"path": rel, "size": (table / rel).stat().st_size,
                     "modificationTime": now, "dataChange": True}},
        ],
    )
    version = delta.load_snapshot(table).version
    total_groups = pq.ParquetFile(str(table / rel)).metadata.num_row_groups
    assert total_groups >= 3

    seen_groups: list[list[int]] = []
    original = pq.ParquetFile.read_row_groups

    def spy(self, row_groups, columns=None):
        seen_groups.append(list(row_groups))
        return original(self, row_groups, columns)

    monkeypatch.setattr(pq.ParquetFile, "read_row_groups", spy)
    page = change_feed.read_change_page(table, start_version=version, page_size=2)
    assert page["count"] == 2
    assert [r["row_index"] for r in page["records"]] == [0, 1]
    # Only the first (3-row) group is decoded to serve two rows.
    assert seen_groups == [[0]]

    # The page straddling a row-group boundary reads exactly the two groups.
    token = page["next_cursor"]
    seen_groups.clear()
    page2 = change_feed.read_change_page(table, cursor=token, page_size=2)
    assert [r["row_index"] for r in page2["records"]] == [2, 3]
    assert seen_groups == [[0, 1]]


def test_missing_intermediate_log_fails_the_page(table: Path) -> None:
    token = change_feed.read_change_page(table, start_version=0, page_size=6)["next_cursor"]
    # Removing a log segment inside the pinned round is corruption, not "done".
    (table / "_delta_log" / f"{2:020d}.json").unlink()
    with pytest.raises(delta.DeltaError):
        change_feed.read_change_page(table, cursor=token)


# ---------------------------------------------------------------------------
# HTTP layer
# ---------------------------------------------------------------------------


@pytest.fixture()
def client(tmp_path: Path, monkeypatch) -> TestClient:
    data_root = tmp_path / "data"
    monkeypatch.setenv("DATA_ROOT", str(data_root))
    import app.main as main

    monkeypatch.setattr(main, "DATA_ROOT", data_root)
    monkeypatch.setattr(main, "TABLE_DIR", data_root / "water_level")
    with TestClient(main.app) as c:
        yield c


def test_api_changes_first_page_and_continuation(client: TestClient) -> None:
    r = client.get("/table/changes", params={"start_version": 0, "page_size": 5})
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["table"] == "water_level"
    assert body["start_version"] == 0 and body["end_version"] == 4
    assert body["count"] == 5 and body["has_more"] is True
    assert body["next_cursor"] == body["cursor"]
    assert body["records"][0]["record_id"] == "v1-a1-r0"

    seen = [rec["record_id"] for rec in body["records"]]
    token = body["next_cursor"]
    while token:
        page = client.get("/table/changes", params={"cursor": token}).json()
        assert page["end_version"] == 4
        seen += [rec["record_id"] for rec in page["records"]]
        token = page["next_cursor"]
    assert len(seen) == 24 and len(set(seen)) == 24


def test_api_changes_compaction_replay_is_empty(client: TestClient) -> None:
    files = client.get("/table/snapshot").json()["active_files"][:2]
    assert client.post("/compact", json={"files": files}).status_code == 200
    body = client.get(
        "/table/changes", params={"start_version": 5, "page_size": 10}
    ).json()
    assert body["records"] == [] and body["has_more"] is False
    assert body["next_start_version"] == 6


def test_api_changes_error_mapping(client: TestClient) -> None:
    assert client.get("/table/changes").status_code == 400
    assert (
        client.get("/table/changes", params={"start_version": 0, "page_size": 0}).status_code
        == 422
    )
    assert (
        client.get("/table/changes", params={"cursor": "garbage"}).status_code == 400
    )
    token = client.get(
        "/table/changes", params={"start_version": 0, "page_size": 2}
    ).json()["next_cursor"]
    assert (
        client.get(
            "/table/changes", params={"cursor": token, "start_version": 1}
        ).status_code
        == 400
    )


def test_api_missing_file_is_500_not_200(client: TestClient) -> None:
    from app import main

    appended = append_rows(
        main.TABLE_DIR, [{"station": "Z", "utc_seconds": 1, "level_mm": 1}]
    )
    (main.TABLE_DIR / appended["file"]).unlink()
    r = client.get(
        "/table/changes",
        params={"start_version": 0, "page_size": change_feed.MAX_PAGE_SIZE},
    )
    assert r.status_code == 500
    assert "missing on disk" in r.json()["detail"]


def test_api_swagger_lists_change_feed(client: TestClient) -> None:
    schema = client.get("/openapi.json").json()
    assert "/table/changes" in schema["paths"]
    params = {
        p["name"] for p in schema["paths"]["/table/changes"]["get"]["parameters"]
    }
    assert params == {"start_version", "page_size", "cursor"}
