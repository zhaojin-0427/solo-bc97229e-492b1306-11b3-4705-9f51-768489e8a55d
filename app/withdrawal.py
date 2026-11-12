"""Batch withdrawal service and ownership-safe object-file cleanup.

Withdrawal semantics
--------------------
A center operator submits 1-20 *ingested* instance ids of one center. Within a
single ``BEGIN IMMEDIATE`` transaction the service:

1. Verifies every id exists, is live (not withdrawn earlier) and belongs to the
   center. Because the writer lock is held for the whole check-and-delete, a
   concurrent ingest transaction cannot land in between, so the check always
   runs against the latest committed data.
2. Finds references from instances *outside* the submitted set to instances
   *inside* the set. Any such reference blocks the whole request, which is
   rejected atomically (nothing is deleted). References whose source is also in
   the set are mutual references and are removed together.
3. On commit, hard-deletes the instance rows, snapshots them into
   ``withdrawal_items`` (the file-cleanup work queue), flips the retained
   ``uid_mappings.present`` flag back to 0 so new references to withdrawn
   targets stay unresolved, and marks surviving references to deleted targets
   unresolved.

Hard deletion is what makes withdrawn instances unqueryable / un-downloadable
and allows same-UID/same-bytes re-upload. UID mapping rows themselves are never
deleted.

File cleanup
------------
Object files are unlinked *after* the DB commit, asynchronously and retried
forever (a new attempt runs at startup and on a background interval). Every
unlink is preceded by an ownership re-check inside a writer transaction: the
snapshot path is deleted only if no live instance still owns it. A re-upload
that re-creates the same path therefore makes the late cleanup skip the file,
so neither re-uploaded nor unrelated objects are ever removed by stale work.
"""
from __future__ import annotations

import json
import sqlite3
import threading
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

from . import config
from .config import MAX_WITHDRAWAL_IDS
from .db import get_connection, immediate_transaction


class WithdrawalError(Exception):
    """Client-side rejection of a withdrawal request.

    ``http_status`` 422 = malformed payload; 404 = unknown / foreign / already
    withdrawn instance; 409 = blocked by references from surviving instances.
    """

    def __init__(self, message: str, http_status: int = 409) -> None:
        super().__init__(message)
        self.http_status = http_status


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%fZ")


def request_withdrawal(center_id: str, instance_ids: list[int]) -> dict:
    """Check and apply a batch withdrawal atomically.

    Returns a summary dict (status ``committed`` or ``rejected``). Raises
    :class:`WithdrawalError` for malformed input or unknown instances; a blocked
    request is *not* raised but returned with status ``rejected`` and the
    blocker list, and is itself persisted for auditing.
    """
    center_id = (center_id or "").strip()
    if not center_id:
        raise WithdrawalError("center_id is required", http_status=422)
    if not instance_ids:
        raise WithdrawalError(
            "at least one instance id is required", http_status=422
        )
    if len(instance_ids) > MAX_WITHDRAWAL_IDS:
        raise WithdrawalError(
            f"a withdrawal may target at most {MAX_WITHDRAWAL_IDS} instances, "
            f"got {len(instance_ids)}",
            http_status=422,
        )
    # Deduplicate while keeping order; duplicates would otherwise look like
    # missing ids once the first row is processed.
    unique_ids = list(dict.fromkeys(instance_ids))

    withdrawal_id = uuid.uuid4().hex

    with immediate_transaction() as conn:
        # ---- Validate every id against the latest committed state. --------
        placeholders = ",".join("?" for _ in unique_ids)
        rows = conn.execute(
            f"SELECT id, center_id, stored_path FROM instances "
            f"WHERE id IN ({placeholders})",
            unique_ids,
        ).fetchall()
        found = {row["id"]: row for row in rows}

        missing = [i for i in unique_ids if i not in found]
        if missing:
            raise WithdrawalError(
                f"instance ids not found or already withdrawn: {missing}",
                http_status=404,
            )
        foreign = sorted(
            i for i in unique_ids if found[i]["center_id"] != center_id
        )
        if foreign:
            raise WithdrawalError(
                f"instance ids do not belong to center {center_id!r}: {foreign}",
                http_status=404,
            )

        # ---- Blocking references: surviving instance -> withdrawn target. -
        # References from a source that is itself in the set are mutual and
        # may be removed together; only sources outside the set block.
        blocker_rows = conn.execute(
            f"""
            SELECT r.id AS reference_id,
                   r.center_id,
                   r.source_instance_id,
                   r.original_referenced_uid,
                   r.new_referenced_uid,
                   i.new_sop_uid AS source_new_sop_uid,
                   i.original_sop_uid AS source_original_sop_uid
            FROM instance_references AS r
            JOIN instances AS i ON i.id = r.source_instance_id
            WHERE r.source_instance_id NOT IN ({placeholders})
              AND r.new_referenced_uid IN (
                  SELECT new_sop_uid FROM instances
                  WHERE id IN ({placeholders})
              )
            ORDER BY r.id
            """,
            unique_ids + unique_ids,
        ).fetchall()

        if blocker_rows:
            blockers = [
                {
                    "reference_id": row["reference_id"],
                    "center_id": row["center_id"],
                    "source_instance_id": row["source_instance_id"],
                    "source_original_sop_uid": row["source_original_sop_uid"],
                    "source_new_sop_uid": row["source_new_sop_uid"],
                    "blocked_referenced_uid": row["new_referenced_uid"],
                    "blocked_original_uid": row["original_referenced_uid"],
                }
                for row in blocker_rows
            ]
            conn.execute(
                """
                INSERT INTO withdrawal_requests
                    (withdrawal_id, center_id, status, requested_count,
                     removed_count, reason, blockers)
                VALUES (?, ?, 'rejected', ?, 0, ?, ?)
                """,
                (
                    withdrawal_id,
                    center_id,
                    len(unique_ids),
                    "blocked by references from instances outside the "
                    "withdrawal set",
                    json.dumps(blockers),
                ),
            )
            return {
                "withdrawal_id": withdrawal_id,
                "center_id": center_id,
                "status": "rejected",
                "requested_count": len(unique_ids),
                "removed_count": 0,
                "reason": "blocked by references from instances outside the "
                          "withdrawal set",
                "blockers": blockers,
            }

        # ---- Snapshot the doomed rows before hard-deleting them. ----------
        targets = conn.execute(
            f"""
            SELECT id, center_id, original_sop_uid, new_sop_uid, sha256,
                   stored_path
            FROM instances WHERE id IN ({placeholders})
            ORDER BY id
            """,
            unique_ids,
        ).fetchall()

        conn.execute(
            """
            INSERT INTO withdrawal_requests
                (withdrawal_id, center_id, status, requested_count,
                 removed_count, reason, blockers)
            VALUES (?, ?, 'committed', ?, ?, NULL, NULL)
            """,
            (withdrawal_id, center_id, len(unique_ids), len(targets)),
        )
        conn.executemany(
            """
            INSERT INTO withdrawal_items
                (withdrawal_id, center_id, instance_id, original_sop_uid,
                 new_sop_uid, sha256, stored_path)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    withdrawal_id,
                    row["center_id"],
                    row["id"],
                    row["original_sop_uid"],
                    row["new_sop_uid"],
                    row["sha256"],
                    row["stored_path"],
                )
                for row in targets
            ],
        )

        # References carried by withdrawn instances disappear with them.
        conn.execute(
            f"DELETE FROM instance_references "
            f"WHERE source_instance_id IN ({placeholders})",
            unique_ids,
        )
        # Surviving references that pointed at a now-deleted target become
        # unresolved; re-ingestion of the target flips them back to resolved.
        conn.execute(
            f"""
            UPDATE instance_references SET resolved = 0
            WHERE new_referenced_uid IN (
                SELECT new_sop_uid FROM withdrawal_items
                WHERE withdrawal_id = ?
            )
            """,
            (withdrawal_id,),
        )
        # Hard delete: withdrawn instances immediately leave every query and
        # cannot be downloaded.
        conn.execute(
            f"DELETE FROM instances WHERE id IN ({placeholders})",
            unique_ids,
        )
        # Retain uid_mappings rows, but SOP UIDs without any live instance are
        # no longer "present", so later references are recorded unresolved.
        conn.execute(
            """
            UPDATE uid_mappings SET present = 0
            WHERE center_id = ? AND uid_kind = 'sop'
              AND original_uid NOT IN (
                  SELECT original_sop_uid FROM instances WHERE center_id = ?
              )
            """,
            (center_id, center_id),
        )

    # DB commit succeeded: best-effort first cleanup pass; anything left over
    # is retried by the background sweeper / after restart.
    process_pending_cleanups()

    return {
        "withdrawal_id": withdrawal_id,
        "center_id": center_id,
        "status": "committed",
        "requested_count": len(unique_ids),
        "removed_count": len(targets),
        "reason": None,
        "blockers": [],
    }


# --------------------------------------------------------------------------- #
# Object-file cleanup
# --------------------------------------------------------------------------- #
def _resolve_within_storage(relative: str) -> Path | None:
    """Resolve a snapshot path, rejecting anything escaping the storage root."""
    if not relative:
        return None
    base = config.STORAGE_DIR.resolve()
    candidate = (base / relative).resolve()
    try:
        candidate.relative_to(base)
    except ValueError:
        return None
    return candidate


def _process_one_item(conn: sqlite3.Connection, item: sqlite3.Row) -> str:
    """Clean up one snapshot path. Must run inside a writer transaction.

    Returns the resulting cleanup status (``done`` or ``error``).
    """
    item_id = item["id"]
    relative = item["stored_path"]

    # Ownership re-check: only unlink when no live instance owns this path.
    # Re-upload (same UID + same bytes) recreates the identical path and makes
    # this query return a row, so a stale cleanup item skips it.
    owner = conn.execute(
        "SELECT id FROM instances WHERE stored_path = ? LIMIT 1",
        (relative,),
    ).fetchone()

    if owner is not None:
        conn.execute(
            """
            UPDATE withdrawal_items
            SET cleanup_status = 'done', skip_reason = ?,
                cleanup_attempts = cleanup_attempts + 1, cleaned_at = ?
            WHERE id = ?
            """,
            (
                f"path now owned by live instance id {owner['id']}; "
                "re-uploaded file kept",
                _now_iso(),
                item_id,
            ),
        )
        return "done"

    candidate = _resolve_within_storage(relative)
    if candidate is None:
        conn.execute(
            """
            UPDATE withdrawal_items
            SET cleanup_status = 'error', cleanup_error = ?,
                cleanup_attempts = cleanup_attempts + 1
            WHERE id = ?
            """,
            ("stored path escapes the storage root", item_id),
        )
        return "error"

    try:
        # missing_ok: an earlier attempt (or an external sweep) may already
        # have removed the bytes; that is a successful cleanup outcome.
        candidate.unlink(missing_ok=True)
    except OSError as exc:
        conn.execute(
            """
            UPDATE withdrawal_items
            SET cleanup_status = 'error', cleanup_error = ?,
                cleanup_attempts = cleanup_attempts + 1
            WHERE id = ?
            """,
            (f"{type(exc).__name__}: {exc}", item_id),
        )
        return "error"

    conn.execute(
        """
        UPDATE withdrawal_items
        SET cleanup_status = 'done', cleanup_error = NULL,
            cleanup_attempts = cleanup_attempts + 1,
            cleaned_at = ?
        WHERE id = ?
        """,
        (_now_iso(), item_id),
    )
    return "done"


def process_pending_cleanups(limit: int = 100) -> dict:
    """Attempt every pending/error cleanup item; retryable across restarts.

    Each item is handled in its own short writer transaction so one failing
    unlink never blocks the others. Returns a small summary dict.
    """
    summary = {"processed": 0, "done": 0, "error": 0, "remaining": 0}
    conn = get_connection()
    try:
        rows = conn.execute(
            """
            SELECT id, stored_path FROM withdrawal_items
            WHERE cleanup_status IN ('pending', 'error')
            ORDER BY id LIMIT ?
            """,
            (limit,),
        ).fetchall()
    finally:
        conn.close()

    for item in rows:
        summary["processed"] += 1
        with immediate_transaction() as tx:
            # Re-read under the writer lock; a concurrent sweep may have
            # finished this item first.
            fresh = tx.execute(
                "SELECT id, stored_path, cleanup_status "
                "FROM withdrawal_items WHERE id = ?",
                (item["id"],),
            ).fetchone()
            if fresh is None or fresh["cleanup_status"] == "done":
                continue
            status = _process_one_item(tx, fresh)
        if status == "done":
            summary["done"] += 1
        else:
            summary["error"] += 1

    conn = get_connection()
    try:
        summary["remaining"] = conn.execute(
            "SELECT COUNT(*) FROM withdrawal_items "
            "WHERE cleanup_status IN ('pending', 'error')"
        ).fetchone()[0]
    finally:
        conn.close()
    return summary


def start_cleanup_sweeper(stop_event: threading.Event) -> threading.Thread:
    """Background loop that retries failed cleanups until shutdown."""

    def _loop() -> None:
        while not stop_event.wait(config.CLEANUP_SWEEP_INTERVAL_S):
            try:
                process_pending_cleanups()
            except Exception:
                # A sweep must never kill the worker thread; the next interval
                # (or a restart) retries.
                pass

    thread = threading.Thread(
        target=_loop, name="withdrawal-cleanup-sweeper", daemon=True
    )
    thread.start()
    return thread


# --------------------------------------------------------------------------- #
# Status queries
# --------------------------------------------------------------------------- #
def get_withdrawal(withdrawal_id: str) -> dict:
    """Return one withdrawal request plus per-item cleanup state."""
    conn = get_connection()
    try:
        req = conn.execute(
            "SELECT * FROM withdrawal_requests WHERE withdrawal_id = ?",
            (withdrawal_id,),
        ).fetchone()
        if req is None:
            raise WithdrawalError("withdrawal request not found", http_status=404)
        items = conn.execute(
            "SELECT * FROM withdrawal_items WHERE withdrawal_id = ? ORDER BY id",
            (withdrawal_id,),
        ).fetchall()
        result = dict(req)
        result["blockers"] = json.loads(result["blockers"]) if result["blockers"] else []
        result["items"] = [dict(row) for row in items]
        return result
    finally:
        conn.close()


def list_withdrawals(center_id: str | None, limit: int, offset: int) -> list[dict]:
    conn = get_connection()
    try:
        if center_id:
            rows = conn.execute(
                "SELECT * FROM withdrawal_requests WHERE center_id = ? "
                "ORDER BY created_at DESC, withdrawal_id DESC "
                "LIMIT ? OFFSET ?",
                (center_id, limit, offset),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM withdrawal_requests "
                "ORDER BY created_at DESC, withdrawal_id DESC "
                "LIMIT ? OFFSET ?",
                (limit, offset),
            ).fetchall()
        result = []
        for row in rows:
            d = dict(row)
            d["blockers"] = json.loads(d["blockers"]) if d["blockers"] else []
            result.append(d)
        return result
    finally:
        conn.close()


def list_cleanup_errors(center_id: str | None) -> list[dict]:
    """Return cleanup items still in error state (and pending ones for ops)."""
    conn = get_connection()
    try:
        sql = (
            "SELECT * FROM withdrawal_items "
            "WHERE cleanup_status IN ('pending', 'error')"
        )
        params: list = []
        if center_id:
            sql += " AND center_id = ?"
            params.append(center_id)
        sql += " ORDER BY id"
        return [dict(row) for row in conn.execute(sql, params).fetchall()]
    finally:
        conn.close()
