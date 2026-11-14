"""Demo 4 - row-level change feed across a compaction and a deletion.

The alert pipeline must see exactly the observations that were inserted or
retracted, in commit order, and nothing from compaction.  This demo:

1. pages through the change feed with a small page size and checks that every
   parquet row appears exactly once (duplicates included);
2. compacts files, then verifies a replay of the compaction version is empty;
3. deletes a compaction output (a file that was itself added by compaction)
   and verifies its rows come back as ``delete`` records;
4. makes a new append while a round is in flight and checks the pinned upper
   bound hides it until the next round.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts._demo_common import API_1, API_2, TABLE_DIR, banner, api, show_snapshot  # noqa: E402
from app.external_writer import append_rows, delete_files  # noqa: E402


def drain(client, start_version: int, page_size: int) -> list[dict]:
    page = client.get(
        "/table/changes", params={"start_version": start_version, "page_size": page_size}
    )
    page.raise_for_status()
    body = page.json()
    print(
        f"  first request start_version={body['start_version']} -> "
        f"pinned end_version={body['end_version']}, page_size={body['page_size']}, "
        f"count={body['count']}, has_more={body['has_more']}"
    )
    records = list(body["records"])
    token = body["next_cursor"]
    pages = 1
    while token:
        body = client.get("/table/changes", params={"cursor": token}).json()
        records.extend(body["records"])
        token = body["next_cursor"]
        pages += 1
    print(f"  drained {len(records)} records in {pages} page(s)")
    return records


def main() -> None:
    client = api(API_1)
    other = api(API_2)
    banner("Demo 4: row-level change feed (dataChange only, resumable paging)")

    versions = client.get("/table/versions").json()["versions"]
    start = versions[0]
    before_snapshot = show_snapshot(client)

    banner("1. Page through the whole log with page_size=5")
    records = drain(client, start, 5)
    ids = [r["record_id"] for r in records]

    # Cross-check against the table: inserts minus deletes is the active row
    # count (earlier demos may already have deleted files).  Compaction adds
    # and removes are dataChange=false, so they contribute nothing here.
    inserts = [r for r in records if r["change_type"] == "insert"]
    deletes = [r for r in records if r["change_type"] == "delete"]
    assert len(ids) == len(set(ids)), "a record was sent twice"
    assert len(inserts) - len(deletes) == before_snapshot["row_count"], (
        f"{len(inserts)} inserts - {len(deletes)} deletes != "
        f"{before_snapshot['row_count']} active rows"
    )
    print(
        f"  record ids are unique; {len(inserts)} inserts - {len(deletes)} "
        f"deletes = {before_snapshot['row_count']} active rows"
    )
    print(f"  example record: {records[0]}")
    next_round = client.get(
        "/table/changes", params={"start_version": start, "page_size": 5}
    ).json()["next_start_version"]
    assert next_round == before_snapshot["version"] + 1
    print(f"  next round starts at version {next_round}")

    banner("2. Compact two files: the compaction replay must be empty")
    chosen = before_snapshot["active_files"][:2]
    compaction = client.post("/compact", json={"files": chosen}).json()
    compacted_v = compaction["committed_version"]
    print(f"  compaction committed at v{compacted_v} (dataChange=false)")
    page = client.get(
        "/table/changes", params={"start_version": compacted_v, "page_size": 100}
    ).json()
    assert page["records"] == [], page
    print("  /table/changes for that version returned 0 records (old rows not re-alerted)")

    banner("3. Delete the compaction output: rows must surface as deletes")
    # Use the table directory directly, exactly like the external alert writer.
    deleted = delete_files(TABLE_DIR, [compaction["output_file"]])
    print(f"  external delete committed at v{deleted['version']}")
    del_records = drain(client, deleted["version"], 5)
    assert len(del_records) == compaction["rows"], del_records
    assert {r["change_type"] for r in del_records} == {"delete"}
    assert {r["path"] for r in del_records} == {compaction["output_file"]}
    print(
        f"  {len(del_records)} delete records, each tagged with "
        f"version/action_index/row_index"
    )

    banner("4. Commits during a round wait for the next round")
    first = client.get(
        "/table/changes", params={"start_version": start, "page_size": 10}
    ).json()
    pinned = first["end_version"]
    appended = append_rows(
        TABLE_DIR,
        [{"station": "ST-STREAM", "utc_seconds": 1, "level_mm": 42}],
    )
    print(f"  new append committed at v{appended['version']} after the round was pinned")
    assert appended["version"] > pinned

    # Continue on the *other replica*: the cursor is stateless and pinned.
    rest = other.get("/table/changes", params={"cursor": first["next_cursor"]})
    rest.raise_for_status()
    rest = rest.json()
    assert rest["end_version"] == pinned
    assert all(r["version"] <= pinned for r in first["records"] + rest["records"])
    print(f"  replica #2 ({API_2}) continued at end_version={rest['end_version']} (pinned)")
    print(f"  latest_version is already {rest['latest_version']}; it is excluded")

    nxt = other.get(
        "/table/changes", params={"start_version": first["next_start_version"],
                                  "page_size": 100}
    ).json()
    assert any(r["path"] == appended["file"] for r in nxt["records"])
    print(
        f"  next round (start_version={first['next_start_version']}) picks it up "
        f"at v{nxt['end_version']}"
    )
    print("\nOK: compaction is invisible, deletes are read from removed files,")
    print("    paging is exact and crosses replicas without losing or repeating rows.")


if __name__ == "__main__":
    main()
