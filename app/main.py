"""FastAPI service exposing Delta Lake compaction for the water-level table.

Two replicas of this service run in docker-compose against one shared table
directory, which is how the "two processes" requirement is demonstrated:
either process can compact, and the Delta OCC protocol serialises them.

The same replicas serve ``/table/changes``, a resumable row-level change feed
over ``dataChange=true`` actions so the alert pipeline can catch up on newly
inserted and retracted observations without ever being re-alerted by a
compaction.
"""

from __future__ import annotations

import os
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Literal

from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, Field

from . import change_feed, delta
from .sample_data import create_sample_table

DATA_ROOT = Path(os.environ.get("DATA_ROOT", "/data"))
TABLE_NAME = os.environ.get("TABLE_NAME", "water_level")
TABLE_DIR = DATA_ROOT / TABLE_NAME


# ---------------------------------------------------------------------------
# Request / response models
# ---------------------------------------------------------------------------


class CompactRequest(BaseModel):
    version: int | None = Field(
        default=None,
        ge=0,
        description="Base snapshot version. Defaults to the latest committed version.",
    )
    files: list[str] = Field(
        ...,
        min_length=2,
        description=(
            "At least two parquet file paths (relative to the table root) "
            "that are all active at the base version."
        ),
    )


class CompactResponse(BaseModel):
    table: str
    committed_version: int
    base_version: int
    files_added: int
    files_removed: int
    output_file: str
    input_files: list[str]
    rows: int
    input_bytes: int
    output_bytes: int
    attempts: int


class SnapshotResponse(BaseModel):
    table: str
    version: int
    latest_version: int
    protocol: dict[str, int]
    partition_columns: list[str]
    active_file_count: int
    active_files: list[str]
    row_count: int


class VersionsResponse(BaseModel):
    table: str
    latest_version: int
    versions: list[int]


class TableStatsResponse(BaseModel):
    table: str
    path: str
    latest_version: int
    active_file_count: int
    row_count: int
    columns: list[dict]
    staging_orphans_cleaned: list[str]


class ChangeRow(BaseModel):
    station: str = Field(description="Station identifier as stored in the parquet file.")
    utc_seconds: int = Field(description="Observation time, UTC seconds since epoch.")
    level_mm: int = Field(description="Water level in millimetres.")


class ChangeRecord(BaseModel):
    record_id: str = Field(
        description=(
            "Stable identity of this row change: "
            "'v{version}-a{action_index}-r{row_index}'."
        ),
    )
    version: int = Field(description="Log version that contains the dataChange action.")
    action_index: int = Field(
        description="0-based position of the add/remove action inside that log file.",
    )
    row_index: int = Field(
        description="0-based position of the row inside the action's parquet file.",
    )
    action: Literal["add", "remove"] = Field(description="Raw Delta action name.")
    change_type: Literal["insert", "delete"] = Field(
        description="Row-level effect: insert for add, delete for remove.",
    )
    path: str = Field(description="Parquet file (relative to the table root).")
    row: ChangeRow


class ChangeFeedResponse(BaseModel):
    table: str
    start_version: int = Field(description="First version covered by this round.")
    end_version: int = Field(
        description=(
            "Pinned upper bound of this round (latest version at the first "
            "request). Commits after this bound belong to the next round."
        ),
    )
    page_size: int
    records: list[ChangeRecord]
    count: int = Field(description="Number of records in this page.")
    has_more: bool = Field(
        description="True when another page of the same pinned round exists.",
    )
    next_cursor: str | None = Field(
        description=(
            "Opaque continuation token for the next page; null when the "
            "pinned round is exhausted. Pass it back as ?cursor=."
        ),
    )
    cursor: str | None = Field(
        description="Alias of next_cursor.",
    )
    next_start_version: int = Field(
        description=(
            "Version to pass as start_version for the next round. Always "
            "end_version + 1; independent of how many pages remain."
        ),
    )
    latest_version: int = Field(
        description="Latest committed version observed while serving this page.",
    )


# ---------------------------------------------------------------------------
# Lifespan
# ---------------------------------------------------------------------------


@asynccontextmanager
async def lifespan(app: FastAPI):
    DATA_ROOT.mkdir(parents=True, exist_ok=True)
    # Idempotent; both replicas may run it concurrently - init + OCC make it safe.
    create_sample_table(TABLE_DIR)
    # Best-effort sweep of files left by previously crashed processes.
    delta.cleanup_staging(TABLE_DIR)
    yield


app = FastAPI(
    title="Delta Lake Compaction & Change Feed API",
    description=(
        "Compact small parquet files of the unpartitioned water-level "
        "observation Delta table (reader protocol v1, writer protocol v2, "
        "no deletion vectors) and replay its row-level changes. Compaction "
        "commits use add/remove actions with dataChange=false and preserve "
        "every value, duplicate row, the schema and all old snapshots, so "
        "they never appear in the change feed. Optimistic concurrency "
        "control lets external writers keep committing; the compaction only "
        "replaces its selected files while all of them are still active."
    ),
    version="1.1.0",
    lifespan=lifespan,
)


def _delta_http(exc: delta.DeltaError) -> HTTPException:
    status = {
        delta.TableNotFoundError: 404,
        delta.VersionNotFoundError: 404,
        delta.FileNotActiveError: 404,
        delta.InvalidRequestError: 400,
        delta.ConflictError: 409,
        delta.TableCorruptError: 500,
        change_feed.DataFileUnavailableError: 500,
    }
    for cls, code in status.items():
        if isinstance(exc, cls):
            return HTTPException(status_code=code, detail=str(exc))
    return HTTPException(status_code=500, detail=str(exc))


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------


@app.get("/health", tags=["ops"])
def health() -> dict:
    try:
        versions = delta.list_versions(TABLE_DIR)
        return {"status": "ok", "table": TABLE_NAME, "latest_version": versions[-1]}
    except delta.DeltaError as exc:
        raise HTTPException(status_code=503, detail=str(exc))


@app.post("/compact", response_model=CompactResponse, tags=["compaction"])
def compact(req: CompactRequest) -> CompactResponse:
    """Merge >= 2 active parquet files of a chosen version into one file."""
    try:
        result = delta.compact(TABLE_DIR, req.files, base_version=req.version)
    except delta.DeltaError as exc:
        raise _delta_http(exc)
    return CompactResponse(
        table=TABLE_NAME,
        committed_version=result["version"],
        base_version=result["base_version"],
        files_added=result["added_files"],
        files_removed=result["removed_files"],
        output_file=result["output_file"],
        input_files=result["input_files"],
        rows=result["rows"],
        input_bytes=result["input_bytes"],
        output_bytes=result["output_bytes"],
        attempts=result["attempts"],
    )


@app.get("/table/versions", response_model=VersionsResponse, tags=["table"])
def versions() -> VersionsResponse:
    vs = delta.list_versions(TABLE_DIR)
    return VersionsResponse(table=TABLE_NAME, latest_version=vs[-1], versions=vs)


@app.get("/table/snapshot", response_model=SnapshotResponse, tags=["table"])
def snapshot(version: int | None = None) -> SnapshotResponse:
    """Describe a snapshot; pass ?version=N for time travel."""
    try:
        snap = delta.load_snapshot(TABLE_DIR, version)
        latest = delta.list_versions(TABLE_DIR)[-1]
        table = delta.read_table(TABLE_DIR, snap.version)
    except delta.DeltaError as exc:
        raise _delta_http(exc)
    return SnapshotResponse(
        table=TABLE_NAME,
        version=snap.version,
        latest_version=latest,
        protocol={
            "minReaderVersion": snap.protocol[0],
            "minWriterVersion": snap.protocol[1],
        },
        partition_columns=list(snap.metadata.get("partitionColumns", [])),
        active_file_count=len(snap.active),
        active_files=sorted(snap.active),
        row_count=table.num_rows,
    )


@app.get(
    "/table/changes",
    response_model=ChangeFeedResponse,
    tags=["change-feed"],
    summary="Read row-level data changes (resumable)",
)
@app.get(
    "/changes",
    response_model=ChangeFeedResponse,
    tags=["change-feed"],
    include_in_schema=False,
)
def changes(
    start_version: int | None = Query(
        default=None,
        ge=0,
        description=(
            "First log version of the round. Required on the first request; "
            "omit it when passing a cursor."
        ),
    ),
    page_size: int | None = Query(
        default=None,
        ge=1,
        le=change_feed.MAX_PAGE_SIZE,
        description=(
            f"Maximum records per page (default {change_feed.DEFAULT_PAGE_SIZE}). "
            "A continuation reuses the value pinned in the cursor unless "
            "overridden explicitly."
        ),
    ),
    cursor: str | None = Query(
        default=None,
        description=(
            "Opaque continuation token returned as next_cursor. Continues the "
            "same pinned round on any replica and across restarts."
        ),
    ),
) -> ChangeFeedResponse:
    """Read inserted/deleted rows from ``dataChange=true`` actions, in log order.

    Compaction commits add/remove parquet files with ``dataChange=false`` and
    are therefore skipped: merging small files never replays old observations.

    Each record is identified by version, action position and in-file row
    index. Duplicate rows are preserved. Deletes are produced by reading the
    removed parquet file, including a compaction output that was deleted
    later; a missing data file fails the request instead of being skipped.

    The first request pins the current latest version as the round's upper
    bound. Later pages are served only from the cursor, so new commits during
    the round wait for the next one (``next_start_version``).
    """
    try:
        result = change_feed.read_change_page(
            TABLE_DIR,
            start_version=start_version,
            page_size=page_size,
            cursor=cursor,
        )
    except delta.DeltaError as exc:
        raise _delta_http(exc)
    return ChangeFeedResponse(table=TABLE_NAME, **result)


@app.get("/table/stats", response_model=TableStatsResponse, tags=["ops"])
def stats() -> TableStatsResponse:
    snap = delta.load_snapshot(TABLE_DIR)
    table = delta.read_table(TABLE_DIR)
    cleaned = delta.cleanup_staging(TABLE_DIR)
    return TableStatsResponse(
        table=TABLE_NAME,
        path=str(TABLE_DIR),
        latest_version=snap.version,
        active_file_count=len(snap.active),
        row_count=table.num_rows,
        columns=[
            {"name": f.name, "type": str(f.type), "nullable": f.nullable}
            for f in table.schema
        ],
        staging_orphans_cleaned=cleaned,
    )
