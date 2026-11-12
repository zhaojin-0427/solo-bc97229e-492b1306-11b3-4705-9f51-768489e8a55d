#!/usr/bin/env bash
# End-to-end demo against a running ingest API (default: http://localhost:8000).
#
# It walks through every requirement using the synthetic samples under
# samples/generated/:
#   1. cross-center UID collision  (same original UID, two centers)
#   2. reference-first, then the referenced image arrives (same center)
#   3. reference must NOT resolve across centers
#   4. idempotent retransmit (same bytes) vs. byte conflict (whole-batch reject)
#   5. batch query + object download, verifying pixels / transfer syntax
set -euo pipefail

BASE_URL="${BASE_URL:-http://localhost:8000}"
SAMPLES="$(cd "$(dirname "$0")/../samples/generated" && pwd)"
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

say() { printf '\n\033[1;36m== %s ==\033[0m\n' "$*"; }
post_batch() { # center file...
  local center="$1"; shift
  curl -sS -X POST "$BASE_URL/api/v1/batches" \
    -F "center_id=$center" \
    $(printf -- '-F files=@%s ' "$@")
}

say "0. health check"
curl -sS "$BASE_URL/healthz"; echo

say "1. CROSS-CENTER UID COLLISION: both centers reuse the same original SOP UID"
post_batch center-a "$SAMPLES/cross_center_uid_collision/center-a/image_001.dcm" | tee "$WORK/a.json"; echo
post_batch center-b "$SAMPLES/cross_center_uid_collision/center-b/image_001.dcm" | tee "$WORK/b.json"; echo
UID_A=$(python3 -c 'import json;print(json.load(open("'$WORK/a.json'"))["files"][0]["new_sop_uid"])')
UID_B=$(python3 -c 'import json;print(json.load(open("'$WORK/b.json'"))["files"][0]["new_sop_uid"])')
[ "$UID_A" != "$UID_B" ] && echo "OK: same original UID -> distinct remapped UIDs per center"

say "2. REFERENCE-FIRST: upload the SR that points at an image not yet present"
post_batch center-x "$SAMPLES/reference_first/struct_001.dcm" | tee "$WORK/struct.json"; echo
echo "-- pending references for center-x (expect one unresolved):"
curl -sS "$BASE_URL/api/v1/references/pending?center_id=center-x&resolved=false"; echo

say "3. The SAME original target SOP uploaded by center-y must NOT resolve center-x's ref"
post_batch center-y "$SAMPLES/reference_first/image_001.dcm" > /dev/null
curl -sS "$BASE_URL/api/v1/references/pending?center_id=center-x&resolved=false"; echo
echo "  (still one pending row above: references never cross centers)"

say "4. Now center-x uploads its own image -> pending reference resolves"
post_batch center-x "$SAMPLES/reference_first/image_001.dcm" > /dev/null
curl -sS "$BASE_URL/api/v1/references/pending?center_id=center-x&resolved=false"; echo
echo "  (empty list above: reference resolved by same-center image)"

say "5. IDEMPOTENT RETRANSMIT: identical bytes reuse the stored object"
post_batch center-x "$SAMPLES/reference_first/image_001.dcm" | python3 -m json.tool

say "6. BYTE CONFLICT: same original SOP, changed bytes -> whole batch rejected (409)"
cp "$SAMPLES/reference_first/image_001.dcm" "$WORK/mutated.dcm"
printf '\001' | dd of="$WORK/mutated.dcm" bs=1 seek=$(( $(stat -c%s "$WORK/mutated.dcm") - 1 )) count=1 conv=notrunc 2>/dev/null
post_batch center-x "$WORK/mutated.dcm" "$SAMPLES/reference_first/struct_001.dcm"; echo

say "7. BATCH QUERY + DOWNLOAD: verify rewritten object keeps TS and pixel bytes"
BATCH=$(python3 -c 'import json;print(json.load(open("'$WORK/a.json'"))["batch_id"])')
curl -sS "$BASE_URL/api/v1/batches/$BATCH" | python3 -m json.tool
INSTANCE=$(curl -sS "$BASE_URL/api/v1/instances?center_id=center-a" | python3 -c 'import json,sys;print(json.load(sys.stdin)[0]["id"])')
curl -sS -D "$WORK/headers" "$BASE_URL/api/v1/instances/$INSTANCE/download" -o "$WORK/down.dcm"
grep -iE "content-type|x-new-sop|x-transfer" "$WORK/headers"
python3 - "$SAMPLES/cross_center_uid_collision/center-a/image_001.dcm" "$WORK/down.dcm" <<'PY'
import sys, pydicom
src, out = (pydicom.dcmread(p) for p in sys.argv[1:])
assert src.PixelData == out.PixelData, "pixel bytes changed!"
assert str(out.file_meta.TransferSyntaxUID) == "1.2.840.10008.1.2.1"
assert out.is_implicit_VR is False and out.is_little_endian is True
print("OK: pixel bytes identical; transfer syntax still Explicit VR Little Endian")
print("rewritten SOP:", out.SOPInstanceUID)
PY

say "8. INVALID OBJECT: whole batch rejected (422), nothing stored"
curl -sS -o "$WORK/inv.json" -w "HTTP %{http_code}\n" -X POST "$BASE_URL/api/v1/batches" \
  -F "center_id=center-z" \
  -F "files=@$SAMPLES/reference_first/image_001.dcm" \
  -F "files=@$SAMPLES/invalid/not_a_dicom.dcm"
cat "$WORK/inv.json"; echo
curl -sS "$BASE_URL/api/v1/batches?center_id=center-z"; echo "  (empty list above)"

# --------------------------------------------------------------------------- #
# Batch withdrawal: reference blocking, mutual removal, re-upload, cleanup.
# --------------------------------------------------------------------------- #
say "9. WITHDRAWAL SETUP: fresh center-w ingests an image plus an SR referencing it"
WORK_D="$WORK/withdraw"; mkdir -p "$WORK_D"
python3 - "$WORK_D" <<'PY'
import sys
from pathlib import Path
sys.path.insert(0, "scripts")
import generate_samples as g
root = Path(sys.argv[1])
g.make_ct_image(root / "image.dcm",
                study_uid="1.9.84.20261112.91.1",
                series_uid="1.9.84.20261112.91.2",
                sop_uid="1.9.84.20261112.91.100", pixel_value=1700)
g.make_sr_with_reference(root / "struct.dcm",
                         study_uid="1.9.84.20261112.91.3",
                         series_uid="1.9.84.20261112.91.4",
                         sop_uid="1.9.84.20261112.91.101",
                         referenced_sop_uid="1.9.84.20261112.91.100",
                         referenced_series_uid="1.9.84.20261112.91.2")
print("withdrawal samples written to", root)
PY
post_batch center-w "$WORK_D/image.dcm" "$WORK_D/struct.dcm" > "$WORK_D/ingest.json"
cat "$WORK_D/ingest.json" | python3 -m json.tool
read_img_id() { # <sop-uid> -> instance id
  curl -sS "$BASE_URL/api/v1/instances?center_id=center-w" \
    | python3 -c 'import json,sys;sop=sys.argv[1];print(next(r["id"] for r in json.load(sys.stdin) if r["original_sop_uid"]==sop))' "$1"
}
IMG_ID=$(read_img_id "1.9.84.20261112.91.100")
SR_ID=$(read_img_id "1.9.84.20261112.91.101")
echo "image instance id=$IMG_ID, struct instance id=$SR_ID"

say "10. REFERENCE BLOCKING: withdrawing only the image is rejected (409) with blockers"
curl -sS -w "\nHTTP %{http_code}\n" -X POST "$BASE_URL/api/v1/withdrawals" \
  -H "Content-Type: application/json" \
  -d "{\"center_id\":\"center-w\",\"instance_ids\":[$IMG_ID]}" | tee "$WORK_D/blocked.json"
echo "  (the surviving SR still references the image -> both instances remain live)"
curl -sS "$BASE_URL/api/v1/instances?center_id=center-w" \
  | python3 -c 'import json,sys;print("live instances:",len(json.load(sys.stdin)))'

say "11. MUTUAL SET: withdrawing image + SR together succeeds (200)"
curl -sS -o "$WORK_D/done.json" -w "HTTP %{http_code}\n" -X POST "$BASE_URL/api/v1/withdrawals" \
  -H "Content-Type: application/json" \
  -d "{\"center_id\":\"center-w\",\"instance_ids\":[$IMG_ID,$SR_ID]}"
cat "$WORK_D/done.json" | python3 -m json.tool
WD=$(python3 -c 'import json;print(json.load(open("'$WORK_D/done.json'"))["withdrawal_id"])')
echo "-- withdrawal status + per-file cleanup:"
curl -sS "$BASE_URL/api/v1/withdrawals/$WD" | python3 -m json.tool
echo "-- instances now gone from queries; download returns 404:"
curl -sS "$BASE_URL/api/v1/instances?center_id=center-w"; echo
curl -sS -o /dev/null -w "download old image id -> HTTP %{http_code}\n" \
  "$BASE_URL/api/v1/instances/$IMG_ID/download"
echo "-- cleanup errors (expect empty):"
curl -sS "$BASE_URL/api/v1/cleanup/errors?center_id=center-w"; echo

say "12. NEW REFERENCE TO A WITHDRAWN TARGET stays unresolved, then re-ingestion resolves it"
# Re-upload only the SR first: its target image is withdrawn, so it is pending.
post_batch center-w "$WORK_D/struct.dcm" > /dev/null
curl -sS "$BASE_URL/api/v1/references/pending?center_id=center-w&resolved=false" \
  | python3 -m json.tool
# Same UID + same bytes re-upload of the image is allowed and re-resolves it.
post_batch center-w "$WORK_D/image.dcm" | python3 -m json.tool
echo "-- pending after re-ingestion (expect empty):"
curl -sS "$BASE_URL/api/v1/references/pending?center_id=center-w&resolved=false"; echo

say "13. LATE CLEANUP PROTECTION: a stale failed cleanup never deletes a re-upload"
# Use a standalone image (no references) so the withdrawal itself is not blocked.
# Block its physical unlink by removing the write bit on the center directory,
# simulating a disk-level cleanup failure that must be retried after restart.
python3 - "$WORK_D" <<'PY'
import sys
from pathlib import Path
sys.path.insert(0, "scripts")
import generate_samples as g
root = Path(sys.argv[1])
g.make_ct_image(root / "cleanup_img.dcm",
                study_uid="1.9.84.20261112.92.1",
                series_uid="1.9.84.20261112.92.2",
                sop_uid="1.9.84.20261112.92.100", pixel_value=1800)
PY
post_batch center-w "$WORK_D/cleanup_img.dcm" > /dev/null
CLEAN_ID=$(read_img_id "1.9.84.20261112.92.100")
OBJ_DIR="$DICOM_INGEST_DATA_DIR/objects/center-w"
chmod -w "$OBJ_DIR" 2>/dev/null || true
curl -sS -o "$WORK_D/wd2.json" -w "withdraw HTTP %{http_code}\n" -X POST "$BASE_URL/api/v1/withdrawals" \
  -H "Content-Type: application/json" \
  -d "{\"center_id\":\"center-w\",\"instance_ids\":[$CLEAN_ID]}"
WD2=$(python3 -c 'import json;print(json.load(open("'$WORK_D/wd2.json'"))["withdrawal_id"])')
echo "-- cleanup error is recorded and queryable while the unlink keeps failing:"
curl -sS "$BASE_URL/api/v1/cleanup/errors?center_id=center-w" | python3 -m json.tool
chmod +w "$OBJ_DIR" 2>/dev/null || true
# Re-upload before the retry: the identical path is recreated and is live again.
post_batch center-w "$WORK_D/cleanup_img.dcm" > /dev/null
CLEAN2_ID=$(read_img_id "1.9.84.20261112.92.100")
echo "-- retry the stale cleanup; ownership re-check skips the re-uploaded file:"
curl -sS -X POST "$BASE_URL/api/v1/cleanup/retry" | python3 -m json.tool
curl -sS "$BASE_URL/api/v1/withdrawals/$WD2" \
  | python3 -c 'import json,sys;[print("item cleanup_status=",i["cleanup_status"],"| skip_reason=",i["skip_reason"]) for i in json.load(sys.stdin)["items"]]'
curl -sS -o /dev/null -w "re-uploaded image still downloadable -> HTTP %{http_code}\n" \
  "$BASE_URL/api/v1/instances/$CLEAN2_ID/download"
echo "-- cleanup errors after retry (expect empty):"
curl -sS "$BASE_URL/api/v1/cleanup/errors?center_id=center-w"; echo

echo
echo "Interactive docs: open $BASE_URL/docs in a browser."
