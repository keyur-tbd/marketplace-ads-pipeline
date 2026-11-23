"""Move old full-row copies out of mp_dedupe_log into Drive, then clear them.

Why: the dedupe guard (mp/dedupe.py) and the trashed-file sweep (mp/trashed.py)
write every row they delete into public.mp_dedupe_log, with the full row in
`row_data` so a wrong delete can be undone. Those copies are ~1 KB a row and a
single marketing re-export can retire a million Instamart rows, so the log grew
to 4 GB within two months (3.1 GB of it row_data). The first clean-up was done
by hand on 2026-11-23 (backup: Drive "Supabase Backups", ids <= 3124372).

What this does, in order, and it stops at the first failure:

  1. Fix the batch: rows with row_data, deleted more than KEEP_DAYS ago, id <= the
     current max id. Nothing written after this point is touched.
  2. Write them to a gzip JSON-lines file (every column, one row per line - the
     same format as the 2026-11-23 backup), paging by id so memory stays flat.
  3. Read the file back and count the lines.
  4. Upload it to the Drive folder and check Drive's size and md5 against ours.
  5. Only then set row_data = null for exactly that batch, in id chunks.
  6. Plain VACUUM, so the next month's copies reuse the freed space (no lock).

The log ROW stays (table, deleted id, file, date, reason, md5); only the full
copy moves. To undo a delete older than KEEP_DAYS, take row_data from the file.

    python -m mp.dedupe_archive            # dry run: counts + Drive folder check
    python -m mp.dedupe_archive --apply    # the real thing (monthly workflow)

Drive: the marketplace token (marketing@) must be able to add files to the
folder; MP_ARCHIVE_FOLDER_ID overrides the default folder.
"""

import argparse
import datetime as dt
import gzip
import hashlib
import json
import logging
import os
import sys
import tempfile
from typing import Optional, Sequence

from . import dbadmin

logger = logging.getLogger("mp.dedupe_archive")

LOG_TABLE = "mp_dedupe_log"
#: birbal@ Drive > Supabase Backups (made 2026-11-23, holds the first backup).
FOLDER_ID = os.environ.get("MP_ARCHIVE_FOLDER_ID", "1fRHsFPesG8sZYF6YhwO20mcoq_rs2hQY")
#: Recent copies stay in the table so an undo in the first two weeks is a plain insert.
KEEP_DAYS = int(os.environ.get("MP_ARCHIVE_KEEP_DAYS", "14"))
PAGE = 20000
CLEAR_CHUNK = 100000


def _batch(cur, cutoff: dt.datetime, max_id: int):
    cur.execute(f"""select count(*), min(id), max(id),
                           coalesce(sum(pg_column_size(row_data)), 0)
                      from public.{LOG_TABLE}
                     where id <= %s and deleted_at < %s and row_data is not null""",
                (max_id, cutoff))
    return cur.fetchone()


def _export(cur, path: str, cutoff: dt.datetime, max_id: int) -> int:
    cur.execute(f"""select column_name from information_schema.columns
                     where table_schema = 'public' and table_name = %s
                     order by ordinal_position""", (LOG_TABLE,))
    cols = [r[0] for r in cur.fetchall()]
    select = ", ".join(f'"{c}"' for c in cols)
    n, last = 0, 0
    with gzip.open(path, "wt", encoding="utf-8", compresslevel=6) as fh:
        while True:
            cur.execute(f"""select {select} from public.{LOG_TABLE}
                             where id > %s and id <= %s and deleted_at < %s
                               and row_data is not null
                             order by id limit {PAGE}""", (last, max_id, cutoff))
            rows = cur.fetchall()
            if not rows:
                break
            for r in rows:
                rec = dict(zip(cols, r))
                fh.write(json.dumps(rec, default=str, ensure_ascii=False) + "\n")
            n += len(rows)
            last = rows[-1][cols.index("id")]
            if n % 200000 < PAGE:
                logger.info("exported %d rows (id %d)", n, last)
    return n


def _readback(path: str) -> int:
    n = 0
    with gzip.open(path, "rt", encoding="utf-8") as fh:
        for line in fh:
            json.loads(line)
            n += 1
    return n


def _md5(path: str) -> str:
    h = hashlib.md5()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _check_folder(svc) -> str:
    from googleapiclient.errors import HttpError
    try:
        meta = svc.files().get(fileId=FOLDER_ID, supportsAllDrives=True,
                               fields="id,name,trashed,capabilities(canAddChildren)").execute()
    except HttpError as exc:
        if exc.resp.status == 404:
            raise SystemExit(f"Drive folder {FOLDER_ID} is not visible to this token - share it "
                             f"with the token's account (marketing@) as Editor.")
        raise
    if meta.get("trashed") or not meta.get("capabilities", {}).get("canAddChildren"):
        raise SystemExit(f"Drive folder {FOLDER_ID} ({meta.get('name')}) is trashed or not "
                         f"writable by this token - share it with the token's account as Editor.")
    return meta["name"]


def _upload(svc, path: str, name: str) -> dict:
    from googleapiclient.http import MediaFileUpload
    media = MediaFileUpload(path, mimetype="application/gzip", resumable=True,
                            chunksize=32 * 1024 * 1024)
    req = svc.files().create(body={"name": name, "parents": [FOLDER_ID]}, media_body=media,
                             fields="id,name,size,md5Checksum", supportsAllDrives=True)
    resp = None
    while resp is None:
        status, resp = req.next_chunk(num_retries=5)
        if status:
            logger.info("upload %d%%", int(status.progress() * 100))
    return resp


def _clear(conn, cutoff: dt.datetime, lo: int, hi: int) -> int:
    cur = conn.cursor()
    total = 0
    for start in range(lo, hi + 1, CLEAR_CHUNK):
        end = min(start + CLEAR_CHUNK - 1, hi)
        cur.execute(f"""update public.{LOG_TABLE} set row_data = null
                         where id between %s and %s and deleted_at < %s
                           and row_data is not null""", (start, end, cutoff))
        total += cur.rowcount
        conn.commit()
    return total


def run(apply: bool) -> int:
    from .drive import build_service

    svc = build_service()
    folder = _check_folder(svc)
    logger.info("Drive folder OK: %s (%s)", folder, FOLDER_ID)

    conn = dbadmin.connect(timeout=1800)
    cur = conn.cursor()
    cur.execute("set statement_timeout = '1800000'")
    cur.execute(f"select now(), coalesce(max(id), 0) from public.{LOG_TABLE}")
    now, max_id = cur.fetchone()
    cutoff = now - dt.timedelta(days=KEEP_DAYS)
    n, lo, hi, nbytes = _batch(cur, cutoff, max_id)
    logger.info("batch: %d rows with row_data deleted before %s (ids %s..%s, %.2f GB in row_data)",
                n, cutoff, lo, hi, nbytes / 1024 ** 3)
    if not n:
        logger.info("nothing to archive")
        return 0
    if not apply:
        logger.info("dry run: nothing written (use --apply)")
        return 0

    name = f"mp_dedupe_log_rowdata_{now:%Y-%m-%d}_ids_{lo}-{hi}.jsonl.gz"
    with tempfile.TemporaryDirectory() as tmp:
        path = os.path.join(tmp, name)
        written = _export(cur, path, cutoff, max_id)
        conn.commit()
        if written != n:
            raise SystemExit(f"exported {written} rows, expected {n}: stopping before upload")
        back = _readback(path)
        if back != n:
            raise SystemExit(f"file reads back {back} rows, expected {n}: stopping before upload")
        size, md5 = os.path.getsize(path), _md5(path)
        logger.info("file %s: %d rows, %.1f MB, md5 %s", name, n, size / 1024 ** 2, md5)
        up = _upload(svc, path, name)
        if int(up.get("size", -1)) != size or up.get("md5Checksum") != md5:
            raise SystemExit(f"Drive copy does not match (size {up.get('size')} vs {size}, "
                             f"md5 {up.get('md5Checksum')} vs {md5}): row_data left in place")
        logger.info("uploaded and verified: Drive file %s", up["id"])

    cleared = _clear(conn, cutoff, lo, hi)
    logger.info("cleared row_data on %d rows", cleared)
    if cleared != n:
        logger.warning("cleared %d rows but archived %d - check the batch", cleared, n)
    conn.autocommit = True
    conn.cursor().execute(f"vacuum (analyze) public.{LOG_TABLE}")
    conn.close()
    logger.info("vacuumed %s", LOG_TABLE)
    return 0


def main(argv: Optional[Sequence[str]] = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--apply", action="store_true", help="export, upload, then clear")
    args = ap.parse_args(argv)
    from .sink import load_dotenv
    load_dotenv(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env"))
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    return run(args.apply)


if __name__ == "__main__":
    sys.exit(main())
