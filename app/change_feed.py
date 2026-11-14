"""Row-level change feed over the Delta transaction log.

The water-level alert pipeline needs to replay every observation that was
added or retracted, in commit order, exactly once.  This module turns the
line-delimited Delta log into such a stream:

* only actions with ``dataChange=true`` are considered; compaction commits
  (``dataChange=false`` adds/removes) are invisible to the feed, so merging
  small files never re-triggers an alert for an old observation
* an ``add`` yields one ``insert`` record per row of its parquet file, a
  ``remove`` yields one ``delete`` record per row of the removed file - the
  file content is read even though the file is no longer active, which also
  covers a compaction output that is deleted later
* rows are never deduplicated; two identical rows stay two records
* a record is identified by ``(version, action_index, row_index)`` where
  ``action_index`` is the 0-based position of the action inside the log file
  and ``row_index`` is the 0-based position of the row inside that parquet
  file

Paging is stateless: the first request passes ``start_version`` and
``page_size``, the server pins the current latest version as the round's
upper bound and returns a signed continuation cursor.  Every later page is
served from that cursor alone (no server-side session), so either replica -
or the same replica after a restart - continues at exactly the same position.
Versions committed after the round started are deliberately not included;
``next_start_version`` tells the client where the next round begins.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from .delta import (
    TABLE_SCHEMA,
    DeltaError,
    InvalidRequestError,
    TableCorruptError,
    VersionNotFoundError,
    _relative_data_path,
    list_versions,
    read_log,
)

CURSOR_VERSION = 1
DEFAULT_PAGE_SIZE = 100
MAX_PAGE_SIZE = 100_000

# Both replicas must accept cursors issued by the other one.  The default is
# shared by every container built from this image; set the env var to rotate
# it deliberately (existing cursors then become invalid).
CURSOR_SECRET_ENV = "CHANGE_FEED_CURSOR_SECRET"
_DEFAULT_CURSOR_SECRET = b"delta-change-feed-cursor-v1"

ROW_COLUMNS = ("station", "utc_seconds", "level_mm")


# ---------------------------------------------------------------------------
# Errors
# ---------------------------------------------------------------------------


class DataFileUnavailableError(DeltaError):
    """A data file referenced by a dataChange action is missing on disk.

    The feed refuses to skip it: the client would silently miss inserted or
    deleted observations, so the page is an error, never "complete".
    """


# ---------------------------------------------------------------------------
# Cursor encoding
# ---------------------------------------------------------------------------


def _cursor_secret() -> bytes:
    raw = os.environ.get(CURSOR_SECRET_ENV)
    return raw.encode("utf-8") if raw else _DEFAULT_CURSOR_SECRET


def _b64encode(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")


def _b64decode(text: str) -> bytes:
    padding = "=" * (-len(text) % 4)
    try:
        return base64.urlsafe_b64decode(text + padding)
    except (ValueError, UnicodeEncodeError) as exc:
        raise InvalidRequestError("cursor is not valid base64url") from exc


def _sign(body: str) -> str:
    digest = hmac.new(
        _cursor_secret(), body.encode("ascii"), hashlib.sha256
    ).digest()[:16]
    return _b64encode(digest)


def encode_cursor(state: dict[str, int]) -> str:
    """Encode the pinned round + next position into an opaque token."""
    body = _b64encode(
        json.dumps(state, separators=(",", ":"), sort_keys=True).encode("utf-8")
    )
    return f"cf{CURSOR_VERSION}.{body}.{_sign(body)}"


def _require_int(state: dict[str, Any], key: str, *, minimum: int = 0) -> int:
    value = state.get(key)
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise InvalidRequestError(
            f"cursor field '{key}' must be an integer >= {minimum}"
        )
    return value


def decode_cursor(token: str) -> dict[str, int]:
    """Verify and decode a cursor, rejecting tampered or foreign tokens."""
    try:
        prefix, body, signature = token.split(".")
    except ValueError as exc:
        raise InvalidRequestError("malformed cursor") from exc
    if prefix != f"cf{CURSOR_VERSION}":
        raise InvalidRequestError("unsupported cursor version")
    if not hmac.compare_digest(signature, _sign(body)):
        raise InvalidRequestError(
            "cursor signature mismatch; it was not issued by this deployment "
            f"(check {CURSOR_SECRET_ENV})"
        )
    try:
        state = json.loads(_b64decode(body))
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise InvalidRequestError("cursor payload is not valid JSON") from exc
    if not isinstance(state, dict):
        raise InvalidRequestError("cursor payload must be an object")

    start = _require_int(state, "start_version")
    end = _require_int(state, "end_version")
    page_size = _require_int(state, "page_size", minimum=1)
    version = _require_int(state, "version")
    action_index = _require_int(state, "action_index")
    row_index = _require_int(state, "row_index")
    if end < start:
        raise InvalidRequestError("cursor end_version precedes start_version")
    if not start <= version <= end:
        raise InvalidRequestError(
            "cursor position lies outside its pinned round "
            f"[{start}, {end}]"
        )
    if page_size > MAX_PAGE_SIZE:
        raise InvalidRequestError(f"cursor page_size exceeds {MAX_PAGE_SIZE}")
    return {
        "start_version": start,
        "end_version": end,
        "page_size": page_size,
        "version": version,
        "action_index": action_index,
        "row_index": row_index,
    }


# ---------------------------------------------------------------------------
# Reading data files
# ---------------------------------------------------------------------------


def _validate_schema(schema: pa.Schema, rel: str, version: int, action_index: int) -> None:
    for field in TABLE_SCHEMA:
        idx = schema.get_field_index(field.name)
        if idx < 0:
            raise TableCorruptError(
                f"data file '{rel}' at version {version} action_index "
                f"{action_index} is missing column '{field.name}'"
            )
        if schema.field(idx).type != field.type:
            raise TableCorruptError(
                f"data file '{rel}' at version {version} action_index "
                f"{action_index} has column '{field.name}' of type "
                f"{schema.field(idx).type}, expected {field.type}"
            )


def _open_change_file(
    table: Path, rel: str, version: int, action_index: int
) -> tuple[pq.ParquetFile, int]:
    """Open a parquet file referenced by a dataChange action.

    Missing files are a hard error: the caller is replaying alerts and must
    not silently lose the rows of that file.
    """
    abs_path = _relative_data_path(table, rel)
    if not abs_path.is_file():
        raise DataFileUnavailableError(
            f"data file '{rel}' referenced by a dataChange action at version "
            f"{version}, action_index {action_index} is missing on disk"
        )
    try:
        parquet = pq.ParquetFile(str(abs_path))
        metadata = parquet.metadata
    except FileNotFoundError as exc:  # raced with an external cleanup
        raise DataFileUnavailableError(
            f"data file '{rel}' referenced by a dataChange action at version "
            f"{version}, action_index {action_index} disappeared while being read"
        ) from exc
    except (OSError, pa.ArrowException) as exc:
        raise TableCorruptError(
            f"cannot read data file '{rel}' at version {version}, "
            f"action_index {action_index}: {exc}"
        ) from exc
    _validate_schema(parquet.schema_arrow, rel, version, action_index)
    if metadata is None:  # pragma: no cover - parquet footer always has it
        raise TableCorruptError(
            f"data file '{rel}' at version {version} has no parquet metadata"
        )
    return parquet, metadata.num_rows


def _read_window(
    parquet: pq.ParquetFile,
    rel: str,
    *,
    start: int,
    count: int,
) -> pa.Table:
    """Read ``count`` rows starting at ``start``, touching only needed groups.

    Paging must be exact *and* cheap: re-reading a multi-megabyte file for
    every small page would make a replay quadratic.  Only the row groups that
    intersect the requested window are decoded.
    """
    if count <= 0:
        return TABLE_SCHEMA.empty_table()
    metadata = parquet.metadata
    assert metadata is not None
    groups: list[int] = []
    rows_before_first = 0
    covered = 0
    for group_index in range(metadata.num_row_groups):
        group_rows = metadata.row_group(group_index).num_rows
        group_start = covered
        covered += group_rows
        if covered <= start:
            rows_before_first = covered
            continue
        if group_start >= start + count:
            break
        groups.append(group_index)
    if not groups:
        return TABLE_SCHEMA.empty_table()
    try:
        window = parquet.read_row_groups(
            groups, columns=list(ROW_COLUMNS)
        )
    except (OSError, pa.ArrowException) as exc:
        raise TableCorruptError(f"cannot read data file '{rel}': {exc}") from exc
    offset = start - rows_before_first
    return window.slice(offset, count)


# ---------------------------------------------------------------------------
# Log inspection
# ---------------------------------------------------------------------------


def _change_action(action: dict[str, Any]) -> tuple[str, str, str] | None:
    """Return ``(change_type, action_name, path)`` for a dataChange action.

    ``None`` means the action carries no row change (commitInfo, protocol,
    metaData, a compaction add/remove, or a null add/remove placeholder).
    """
    for action_name, change_type in (("add", "insert"), ("remove", "delete")):
        payload = action.get(action_name)
        if payload is None:
            continue
        if not isinstance(payload, dict) or not isinstance(payload.get("path"), str):
            raise TableCorruptError(
                f"malformed {action_name} action in the transaction log"
            )
        if not payload.get("dataChange", True):
            return None
        return change_type, action_name, payload["path"]
    return None


def _record(
    *,
    change_type: str,
    action_name: str,
    path: str,
    version: int,
    action_index: int,
    row_index: int,
    row: dict[str, Any],
) -> dict[str, Any]:
    values = {
        name: row.get(name) for name in ROW_COLUMNS
    }
    return {
        "record_id": f"v{version}-a{action_index}-r{row_index}",
        "version": version,
        "action_index": action_index,
        "row_index": row_index,
        "action": action_name,
        "change_type": change_type,
        "path": path,
        "row": values,
    }


def _collect(
    table: Path,
    *,
    start_version: int,
    end_version: int,
    position: tuple[int, int, int],
    page_size: int,
) -> tuple[list[dict[str, Any]], list[tuple[int, int, int]]]:
    """Collect up to ``page_size + 1`` records starting at *position*.

    The one extra record proves that the round has more data without having to
    re-scan after the page is built; its position becomes the next cursor.
    """
    limit = page_size + 1
    records: list[dict[str, Any]] = []
    positions: list[tuple[int, int, int]] = []

    version, action_index, row_index = position
    if not start_version <= version <= end_version:
        raise InvalidRequestError(
            "cursor position lies outside its pinned round "
            f"[{start_version}, {end_version}]"
        )

    while version <= end_version and len(records) < limit:
        try:
            actions = read_log(table, version)
        except VersionNotFoundError as exc:
            raise TableCorruptError(
                f"log version {version} disappeared while serving the pinned "
                f"round [{start_version}, {end_version}]"
            ) from exc
        if action_index > len(actions):
            raise InvalidRequestError(
                f"cursor action_index {action_index} is beyond version "
                f"{version} which has {len(actions)} actions"
            )
        while action_index < len(actions) and len(records) < limit:
            action = actions[action_index]
            change = _change_action(action)
            if change is None:
                action_index += 1
                row_index = 0
                continue
            change_type, action_name, rel = change
            parquet, total_rows = _open_change_file(
                table, rel, version, action_index
            )
            if row_index > total_rows:
                raise InvalidRequestError(
                    f"cursor row_index {row_index} is beyond data file '{rel}' "
                    f"which has {total_rows} rows"
                )
            if row_index == total_rows:
                # A cursor never points past the end, but tolerate a position
                # that exactly finished the file.
                action_index += 1
                row_index = 0
                continue
            take = min(total_rows - row_index, limit - len(records))
            rows = _read_window(
                parquet, rel, start=row_index, count=take
            ).to_pylist()
            if len(rows) != take:  # pragma: no cover - parquet metadata lie
                raise TableCorruptError(
                    f"data file '{rel}' at version {version} declared "
                    f"{total_rows} rows but only {row_index + len(rows)} "
                    f"could be read"
                )
            for offset, values in enumerate(rows):
                current = row_index + offset
                records.append(
                    _record(
                        change_type=change_type,
                        action_name=action_name,
                        path=rel,
                        version=version,
                        action_index=action_index,
                        row_index=current,
                        row=values,
                    )
                )
                positions.append((version, action_index, current))
            row_index += take
            if row_index >= total_rows:
                action_index += 1
                row_index = 0
        version += 1
        action_index = 0
        row_index = 0

    return records, positions


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


def _validate_page_size(page_size: int) -> int:
    if isinstance(page_size, bool) or not isinstance(page_size, int):
        raise InvalidRequestError("page_size must be an integer")
    if page_size < 1 or page_size > MAX_PAGE_SIZE:
        raise InvalidRequestError(
            f"page_size must be between 1 and {MAX_PAGE_SIZE}"
        )
    return page_size


def read_change_page(
    table: Path,
    *,
    start_version: int | None = None,
    page_size: int | None = None,
    cursor: str | None = None,
) -> dict[str, Any]:
    """Return one page of the row-level change feed.

    Either ``cursor`` (continuation) or ``start_version`` (first request) is
    required.  On the first request the latest committed version is pinned as
    the round's ``end_version``; a continuation always honours the bounds
    stored in the cursor, even if new commits have landed since.
    """
    table = Path(table)
    versions = list_versions(table)
    latest_version = versions[-1]

    if cursor is not None:
        if start_version is not None:
            raise InvalidRequestError(
                "pass either cursor (continuation) or start_version (first "
                "request), not both"
            )
        state = decode_cursor(cursor)
        start_version = state["start_version"]
        end_version = state["end_version"]
        position = (state["version"], state["action_index"], state["row_index"])
        if page_size is None:
            page_size = state["page_size"]
        page_size = _validate_page_size(page_size)
        if end_version > latest_version:
            raise TableCorruptError(
                f"pinned end_version {end_version} is beyond the latest "
                f"committed version {latest_version}; the log was truncated"
            )
    else:
        if start_version is None:
            raise InvalidRequestError(
                "the first request must pass start_version (or a cursor)"
            )
        if isinstance(start_version, bool) or not isinstance(start_version, int):
            raise InvalidRequestError("start_version must be an integer")
        if start_version < 0:
            raise InvalidRequestError("start_version must be >= 0")
        if page_size is None:
            page_size = DEFAULT_PAGE_SIZE
        page_size = _validate_page_size(page_size)
        end_version = latest_version
        if start_version > latest_version:
            # Nothing has been committed at/after the requested start yet
            # (e.g. a poller asking for latest+1).  This is an empty round,
            # not an error, and next_start_version stays put so the commit
            # that eventually lands at start_version is not skipped.
            return {
                "start_version": start_version,
                "end_version": start_version,
                "page_size": page_size,
                "records": [],
                "count": 0,
                "has_more": False,
                "next_cursor": None,
                "cursor": None,
                "next_start_version": start_version,
                "latest_version": latest_version,
            }
        position = (start_version, 0, 0)

    records, positions = _collect(
        table,
        start_version=start_version,
        end_version=end_version,
        position=position,
        page_size=page_size,
    )

    if len(records) <= page_size:
        has_more = False
        next_cursor = None
        records = records[:page_size]
    else:
        has_more = True
        # The (page_size+1)-th record is not returned; its own position is
        # exactly where the next page must resume.
        extra = positions[page_size]
        next_cursor = encode_cursor(
            {
                "start_version": start_version,
                "end_version": end_version,
                "page_size": page_size,
                "version": extra[0],
                "action_index": extra[1],
                "row_index": extra[2],
            }
        )
        records = records[:page_size]

    return {
        "start_version": start_version,
        "end_version": end_version,
        "page_size": page_size,
        "records": records,
        "count": len(records),
        "has_more": has_more,
        "next_cursor": next_cursor,
        # Alias: callers that just read ``cursor`` see the same token.
        "cursor": next_cursor,
        "next_start_version": end_version + 1,
        "latest_version": latest_version,
    }
