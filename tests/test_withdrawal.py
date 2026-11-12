"""Requirement tests for per-center batch withdrawal.

Covers:
* happy-path withdrawal: instances vanish from queries/downloads, UID mappings
  stay in place, mutual references inside the set are removed together
* reference blocking from surviving instances, with a full blocker listing
* new references to withdrawn targets stay unresolved until re-ingestion
* same-UID/same-bytes re-upload after withdrawal restores resolution
* file-cleanup failure retry, restart continuation and late-cleanup protection
  of re-uploaded files
* payload validation and concurrent ingest/withdrawal safety
"""
from __future__ import annotations

import os
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import generate_samples as gen

from conftest import upload

from app import withdrawal
from app.db import get_connection

# Numeric-only DICOM UID components (letters are illegal in a UI value).
SOP_BASE = "1.9.84.20261112.90"


def sop(tag: int) -> str:
    return f"{SOP_BASE}.{tag}"


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #
def make_image(tmp: Path, name: str, sop_uid: str, *, pixel: int = 1100) -> Path:
    path = tmp / name
    gen.make_ct_image(
        path,
        study_uid=f"{sop_uid}.1",
        series_uid=f"{sop_uid}.2",
        sop_uid=sop_uid,
        pixel_value=pixel,
    )
    return path


def make_sr(tmp: Path, name: str, sop_uid: str, target_sop: str) -> Path:
    path = tmp / name
    gen.make_sr_with_reference(
        path,
        study_uid=f"{sop_uid}.1",
        series_uid=f"{sop_uid}.2",
        sop_uid=sop_uid,
        referenced_sop_uid=target_sop,
        referenced_series_uid=f"{target_sop}.2",
    )
    return path


def ingest(client, center: str, paths: list[Path]) -> dict:
    resp = upload(client, center, paths)
    assert resp.status_code == 201, resp.text
    return resp.json()


def instance_rows(client, center: str) -> list[dict]:
    return client.get(
        "/api/v1/instances", params={"center_id": center}
    ).json()


def instance_ids(client, center: str) -> list[int]:
    return [r["id"] for r in instance_rows(client, center)]


def withdraw(client, center: str, ids: list[int]):
    return client.post(
        "/api/v1/withdrawals",
        json={"center_id": center, "instance_ids": ids},
    )


def cleanup_row(item_id: int) -> dict:
    conn = get_connection()
    try:
        return dict(conn.execute(
            "SELECT * FROM withdrawal_items WHERE id = ?", (item_id,)
        ).fetchone())
    finally:
        conn.close()


# --------------------------------------------------------------------------- #
# Happy path
# --------------------------------------------------------------------------- #
def test_withdraw_makes_instance_unqueryable_and_undownloadable(client, tmp_path):
    target_sop = sop(1)
    img = make_image(tmp_path, "img.dcm", target_sop)
    ingest(client, "center-w", [img])
    inst_id = instance_ids(client, "center-w")[0]

    resp = withdraw(client, "center-w", [inst_id])
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["status"] == "committed"
    assert body["removed_count"] == 1

    # No longer queryable.
    assert instance_rows(client, "center-w") == []
    # No longer downloadable (hard-deleted row -> 404).
    assert client.get(
        f"/api/v1/instances/{inst_id}/download"
    ).status_code == 404

    # Withdrawal is recorded with its cleanup state.
    detail = client.get(
        f"/api/v1/withdrawals/{body['withdrawal_id']}"
    ).json()
    assert detail["status"] == "committed"
    assert len(detail["items"]) == 1
    assert detail["items"][0]["cleanup_status"] == "done"

    # The object file is eventually gone.
    objects = Path(os.environ["DICOM_INGEST_DATA_DIR"]) / "objects"
    assert list(objects.rglob("*.dcm")) == []


def test_uid_mapping_is_retained_after_withdrawal(client, tmp_path):
    target_sop = sop(2)
    img = make_image(tmp_path, "img.dcm", target_sop)
    ingest(client, "center-k", [img])

    conn = get_connection()
    try:
        before = conn.execute(
            "SELECT new_uid, present FROM uid_mappings "
            "WHERE center_id = ? AND original_uid = ?",
            ("center-k", target_sop),
        ).fetchone()
        assert before["present"] == 1
    finally:
        conn.close()

    withdraw(client, "center-k", instance_ids(client, "center-k"))

    conn = get_connection()
    try:
        # The mapping row survives; only the presence flag flips back to 0.
        after = conn.execute(
            "SELECT new_uid, present FROM uid_mappings "
            "WHERE center_id = ? AND original_uid = ?",
            ("center-k", target_sop),
        ).fetchone()
        assert after is not None
        assert after["new_uid"] == before["new_uid"]
        assert after["present"] == 0
    finally:
        conn.close()


def test_mutual_references_inside_set_are_removed_together(client, tmp_path):
    # Two SR objects referencing each other: both are in the withdrawal set, so
    # neither reference blocks the operation.
    sop_a, sop_b = sop(3), sop(4)
    a = make_sr(tmp_path, "a.dcm", sop_a, sop_b)
    b = make_sr(tmp_path, "b.dcm", sop_b, sop_a)
    ingest(client, "center-m", [a, b])
    ids = instance_ids(client, "center-m")
    assert len(ids) == 2

    resp = withdraw(client, "center-m", ids)
    assert resp.status_code == 200, resp.text
    assert resp.json()["removed_count"] == 2
    assert instance_rows(client, "center-m") == []

    conn = get_connection()
    try:
        assert conn.execute(
            "SELECT COUNT(*) FROM instance_references"
        ).fetchone()[0] == 0
    finally:
        conn.close()


# --------------------------------------------------------------------------- #
# Blocking references
# --------------------------------------------------------------------------- #
def test_external_reference_blocks_and_lists_blockers(client, tmp_path):
    target_sop = sop(5)
    sr_sop = sop(6)
    target = make_image(tmp_path, "target.dcm", target_sop)
    sr = make_sr(tmp_path, "sr.dcm", sr_sop, target_sop)
    ingest(client, "center-b", [target, sr])

    rows = instance_rows(client, "center-b")
    target_id = next(r["id"] for r in rows
                     if r["original_sop_uid"] == target_sop)
    sr_id = next(r["id"] for r in rows
                 if r["original_sop_uid"] == sr_sop)

    # Withdrawing only the target is blocked by the surviving SR.
    resp = withdraw(client, "center-b", [target_id])
    assert resp.status_code == 409
    payload = resp.json()
    wd = payload["detail"]["withdrawal"]
    assert wd["status"] == "rejected"
    assert len(wd["blockers"]) == 1
    blocker = wd["blockers"][0]
    assert blocker["source_instance_id"] == sr_id
    assert blocker["blocked_original_uid"] == target_sop

    # Nothing was deleted.
    assert len(instance_ids(client, "center-b")) == 2

    # The rejected request is auditable and appears in the withdrawal list.
    listed = client.get(
        "/api/v1/withdrawals", params={"center_id": "center-b"}
    ).json()
    assert any(w["withdrawal_id"] == wd["withdrawal_id"]
               and w["status"] == "rejected" for w in listed)

    # Withdrawing both together is allowed (mutual set).
    ok = withdraw(client, "center-b", [target_id, sr_id])
    assert ok.status_code == 200
    assert ok.json()["removed_count"] == 2


def test_blocking_check_uses_latest_data_under_concurrency(client, tmp_path):
    # An ingest racing a withdrawal must never be missed: either the new
    # reference lands first and blocks the withdrawal, or the withdrawal lands
    # first and the ingest keeps its reference unresolved. Both are consistent;
    # a resolved reference pointing at a deleted target row is the forbidden
    # outcome.
    target_sop = sop(7)
    sr_sop = sop(8)
    target = make_image(tmp_path, "target.dcm", target_sop)
    ingest(client, "center-r", [target])
    target_id = instance_ids(client, "center-r")[0]
    sr = make_sr(tmp_path, "sr.dcm", sr_sop, target_sop)

    barrier = threading.Barrier(2)
    outcomes: list = []

    def do_withdraw():
        barrier.wait()
        outcomes.append(("withdraw",
                         withdraw(client, "center-r", [target_id]).status_code))

    def do_ingest():
        barrier.wait()
        outcomes.append(("ingest", upload(client, "center-r", [sr]).status_code))

    with ThreadPoolExecutor(max_workers=2) as pool:
        futs = [pool.submit(do_withdraw), pool.submit(do_ingest)]
        for f in futs:
            f.result()

    statuses = dict(outcomes)
    assert statuses["ingest"] == 201

    conn = get_connection()
    try:
        live_target = conn.execute(
            "SELECT id FROM instances WHERE original_sop_uid = ?",
            (target_sop,),
        ).fetchall()
        ref_row = conn.execute(
            "SELECT resolved FROM instance_references "
            "WHERE original_referenced_uid = ?",
            (target_sop,),
        ).fetchone()
    finally:
        conn.close()

    if statuses["withdraw"] == 409:
        # Ingest landed first; its reference must be fully resolved and the
        # target must still exist.
        assert len(live_target) == 1
        assert ref_row is not None and ref_row["resolved"] == 1
    else:
        # Withdrawal landed first; target gone and the new reference unresolved.
        assert statuses["withdraw"] == 200
        assert live_target == []
        assert ref_row is not None and ref_row["resolved"] == 0


# --------------------------------------------------------------------------- #
# Re-ingestion semantics
# --------------------------------------------------------------------------- #
def test_new_reference_to_withdrawn_target_stays_unresolved_then_recovers(
    client, tmp_path
):
    target_sop = sop(9)
    sr_sop = sop(10)
    target = make_image(tmp_path, "target.dcm", target_sop)
    ingest(client, "center-r2", [target])
    withdraw(client, "center-r2", instance_ids(client, "center-r2"))

    # A brand-new SR referencing the withdrawn target is accepted but unresolved.
    sr = make_sr(tmp_path, "sr.dcm", sr_sop, target_sop)
    ingest(client, "center-r2", [sr])
    pending = client.get(
        "/api/v1/references/pending",
        params={"center_id": "center-r2", "resolved": False},
    ).json()
    assert len(pending) == 1
    assert pending[0]["original_referenced_uid"] == target_sop

    # Re-upload the same target (same UID, same bytes): reference resolves.
    ingest(client, "center-r2", [target])
    assert client.get(
        "/api/v1/references/pending",
        params={"center_id": "center-r2", "resolved": False},
    ).json() == []
    resolved = client.get(
        "/api/v1/references/pending",
        params={"center_id": "center-r2", "resolved": True},
    ).json()
    assert any(r["original_referenced_uid"] == target_sop for r in resolved)


def test_same_uid_same_bytes_reupload_after_withdrawal(client, tmp_path):
    target_sop = sop(11)
    target = make_image(tmp_path, "target.dcm", target_sop)
    first = ingest(client, "center-r3", [target])
    first_uid = first["files"][0]["new_sop_uid"]
    withdraw(client, "center-r3", instance_ids(client, "center-r3"))

    # Re-upload identical bytes: allowed, deterministic UID stays identical.
    again = ingest(client, "center-r3", [target])
    assert again["files"][0]["new_sop_uid"] == first_uid
    assert again["reused_count"] == 0  # previous instance was hard-deleted
    new_id = instance_ids(client, "center-r3")[0]
    assert client.get(
        f"/api/v1/instances/{new_id}/download"
    ).status_code == 200


# --------------------------------------------------------------------------- #
# File cleanup: failures, restart continuation and late-cleanup protection
# --------------------------------------------------------------------------- #
def test_cleanup_failure_is_retried_and_recorded(client, tmp_path, monkeypatch):
    target_sop = sop(12)
    img = make_image(tmp_path, "img.dcm", target_sop)
    ingest(client, "center-c", [img])
    inst_id = instance_ids(client, "center-c")[0]

    real_unlink = Path.unlink
    fail = {"on": True}

    def flaky_unlink(self, *args, **kwargs):
        if fail["on"]:
            raise OSError("simulated disk hiccup")
        return real_unlink(self, *args, **kwargs)

    # Patch only the unlink method; data-dir patches from the data_dir fixture
    # must stay in place for the whole test.
    monkeypatch.setattr("app.withdrawal.Path.unlink", flaky_unlink)

    body = withdraw(client, "center-c", [inst_id]).json()
    item_id = client.get(
        f"/api/v1/withdrawals/{body['withdrawal_id']}"
    ).json()["items"][0]["id"]

    row = cleanup_row(item_id)
    assert row["cleanup_status"] == "error"
    assert "simulated disk hiccup" in row["cleanup_error"]
    assert row["cleanup_attempts"] >= 1

    # The error is visible through the dedicated query endpoint.
    errors = client.get(
        "/api/v1/cleanup/errors", params={"center_id": "center-c"}
    ).json()
    assert any(e["id"] == item_id and e["cleanup_status"] == "error"
               for e in errors)

    # Recover and retry (simulating a later sweep / post-restart continuation).
    fail["on"] = False
    summary = client.post("/api/v1/cleanup/retry").json()
    assert summary["remaining"] == 0
    row = cleanup_row(item_id)
    assert row["cleanup_status"] == "done"
    assert row["cleanup_error"] is None


def test_restart_continues_pending_cleanup(client, tmp_path, monkeypatch):
    target_sop = sop(13)
    img = make_image(tmp_path, "img.dcm", target_sop)
    ingest(client, "center-c2", [img])
    inst_id = instance_ids(client, "center-c2")[0]

    real_unlink = Path.unlink
    fail = {"on": True}

    def flaky_unlink(self, *args, **kwargs):
        if fail["on"]:
            raise OSError("boom")
        return real_unlink(self, *args, **kwargs)

    monkeypatch.setattr("app.withdrawal.Path.unlink", flaky_unlink)
    body = withdraw(client, "center-c2", [inst_id]).json()
    item_id = client.get(
        f"/api/v1/withdrawals/{body['withdrawal_id']}"
    ).json()["items"][0]["id"]
    assert cleanup_row(item_id)["cleanup_status"] == "error"

    # Simulate a fresh process start: on startup the lifespan runs an initial
    # sweep over whatever pending/error rows the previous incarnation left.
    fail["on"] = False
    startup_summary = withdrawal.process_pending_cleanups()
    assert startup_summary["remaining"] == 0
    assert cleanup_row(item_id)["cleanup_status"] == "done"


def test_late_cleanup_never_deletes_reuploaded_file(client, tmp_path, monkeypatch):
    target_sop = sop(14)
    target = make_image(tmp_path, "target.dcm", target_sop)
    ingest(client, "center-c3", [target])
    inst_id = instance_ids(client, "center-c3")[0]

    real_unlink = Path.unlink
    fail = {"on": True}

    def flaky_unlink(self, *args, **kwargs):
        if fail["on"]:
            raise OSError("deferred")
        return real_unlink(self, *args, **kwargs)

    monkeypatch.setattr("app.withdrawal.Path.unlink", flaky_unlink)
    body = withdraw(client, "center-c3", [inst_id]).json()
    item_id = client.get(
        f"/api/v1/withdrawals/{body['withdrawal_id']}"
    ).json()["items"][0]["id"]
    stored_path = cleanup_row(item_id)["stored_path"]

    # Re-upload the identical object while cleanup is still failing: the
    # physical path is recreated and now belongs to a live instance.
    ingest(client, "center-c3", [target])
    reuploaded = withdrawal.config.STORAGE_DIR / stored_path
    assert reuploaded.is_file()

    # Now the deferred sweep runs. The ownership re-check must skip the
    # re-uploaded file instead of unlinking it.
    fail["on"] = False
    summary = client.post("/api/v1/cleanup/retry").json()
    assert summary["error"] == 0

    row = cleanup_row(item_id)
    assert row["cleanup_status"] == "done"
    assert row["skip_reason"] is not None  # ownership re-check skipped unlink
    # The re-uploaded file is untouched and still downloadable.
    assert reuploaded.is_file()
    new_id = instance_ids(client, "center-c3")[0]
    assert client.get(
        f"/api/v1/instances/{new_id}/download"
    ).status_code == 200


# --------------------------------------------------------------------------- #
# Validation
# --------------------------------------------------------------------------- #
def test_withdrawal_payload_validation(client, tmp_path):
    target_sop = sop(15)
    img = make_image(tmp_path, "img.dcm", target_sop)
    ingest(client, "center-v", [img])
    inst_id = instance_ids(client, "center-v")[0]

    # Empty id list.
    assert withdraw(client, "center-v", []).status_code == 422
    # Missing center.
    r = client.post("/api/v1/withdrawals",
                    json={"center_id": "", "instance_ids": [inst_id]})
    assert r.status_code == 422
    # More than 20 ids.
    assert withdraw(client, "center-v", list(range(1, 22))).status_code == 422
    # Unknown / already-withdrawn id.
    assert withdraw(client, "center-v", [999999]).status_code == 404

    # Foreign center cannot withdraw another center's instance.
    assert withdraw(client, "center-other", [inst_id]).status_code == 404


def test_duplicate_ids_in_request_are_deduplicated(client, tmp_path):
    target_sop = sop(16)
    img = make_image(tmp_path, "img.dcm", target_sop)
    ingest(client, "center-v2", [img])
    inst_id = instance_ids(client, "center-v2")[0]
    resp = withdraw(client, "center-v2", [inst_id, inst_id])
    assert resp.status_code == 200
    assert resp.json()["requested_count"] == 1


def test_withdrawing_twice_second_call_reports_missing(client, tmp_path):
    target_sop = sop(17)
    img = make_image(tmp_path, "img.dcm", target_sop)
    ingest(client, "center-v3", [img])
    inst_id = instance_ids(client, "center-v3")[0]
    assert withdraw(client, "center-v3", [inst_id]).status_code == 200
    # The id no longer exists: the second call is a clean 404, not a crash.
    assert withdraw(client, "center-v3", [inst_id]).status_code == 404
