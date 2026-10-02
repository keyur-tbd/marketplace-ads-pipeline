"""Files that left Drive after we loaded them: retire their rows once they are replaced.

    python -m mp.trashed              # report only
    python -m mp.trashed --apply      # also delete what is safely replaced

The marketing team works by REPLACING exports in Drive: an interim month-to-date
file is trashed when the next one lands (Instamart, 14 interim "Sep-26.csv"s before
the final Sep-26-1/-2 on 1 Oct), and a growing monthly file is re-uploaded as a new
file with the old one trashed (Blinkit's Sep-26.xls). The load ledger only ever adds,
so the trashed versions' rows stayed and every later version counted on top:
Instamart Sep 2026 read Rs 127 L in Birbal with ~Rs 105 L in the final files.

A loaded file is a CANDIDATE when the freshly built Drive index no longer has it.
Each candidate's Drive status is then checked (batched, 100 per call):

  * trashed -> its rows are deleted ONLY when every date it holds has rows from
    files of the same table that were loaded AFTER it was trashed (approximated by
    its Drive modifiedTime, which trashing moves, or its own load time if later).
    That is "a replacement has landed". Rows go to mp_dedupe_log in full first, and
    the ledger entry goes too, so restoring the file in Drive makes the next run load
    it again. A trashed file whose replacement has not loaded yet is left alone and
    reported: deleting Blinkit's old Sep-26.xls before the new one loaded would have
    emptied Blinkit's September.
  * not found / not shared -> reported only, never deleted. A 404 here is far more
    often a sharing change than a deletion.
  * alive but outside the indexed folders (the frozen 'Market Place Data' copy, a
    file moved elsewhere) -> nothing to do.
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
from typing import Dict, List, Optional

from . import dbadmin, dedupe, drive

logger = logging.getLogger("mp.trashed")


def _drive_status(svc, ids: List[str]) -> Dict[str, dict]:
    """id -> {'state': 'trashed'|'alive'|'missing', 'modified': iso, 'by': email}."""
    out: Dict[str, dict] = {}

    def cb(request_id, response, exception):
        if exception is not None:
            out[request_id] = {"state": "missing", "error": str(exception)[:120]}
        else:
            out[request_id] = {"state": "trashed" if response.get("trashed") else "alive",
                               "modified": response.get("modifiedTime"),
                               "parent": (response.get("parents") or [None])[0],
                               "by": (response.get("lastModifyingUser") or {}).get("emailAddress"),
                               "name": response.get("name")}

    for start in range(0, len(ids), 100):
        batch = svc.new_batch_http_request(callback=cb)
        for fid in ids[start:start + 100]:
            batch.add(svc.files().get(fileId=fid, supportsAllDrives=True,
                                      fields="name,trashed,parents,modifiedTime,lastModifyingUser(emailAddress)"),
                      request_id=fid)
        batch.execute()
    return out


def reconcile(svc, index: dict, apply: bool = False, tables: Optional[List[str]] = None) -> List[dict]:
    """One entry per candidate file that is trashed or missing, with what was done."""
    live = {f["id"] for f in index["files"]}
    conn = dbadmin.connect(timeout=1800)
    try:
        cur = conn.cursor()
        cur.execute("set statement_timeout = '30min'")
        cur.execute("""select table_name, drive_file_id, source_file, loaded_at
                       from public.mp_loaded_files where rows_written > 0""")
        everything = cur.fetchall()
        loaded_at_of = {(t, fid): at for t, fid, _, at in everything}
        # Live files by Drive folder: a replacement is uploaded where the old file was.
        folder_id = {f["path"]: f["id"] for f in index.get("folders", [])}
        live_in: Dict[str, List[str]] = {}
        for f in index["files"]:
            parent = folder_id.get(f["path"].rsplit("/", 1)[0])
            if parent:
                live_in.setdefault(parent, []).append(f["id"])
        ledger = [r for r in everything if r[1] not in live and (not tables or r[0] in tables)]
        if not ledger:
            return []
        status = _drive_status(svc, sorted({r[1] for r in ledger}))
        results: List[dict] = []
        by_table: Dict[str, List[tuple]] = {}
        for table, fid, name, loaded_at in ledger:
            st = status.get(fid, {"state": "missing"})
            if st["state"] == "alive":
                continue
            entry = {"table": table, "file": name, "id": fid, "state": st["state"],
                     "by": st.get("by"), "action": "reported"}
            results.append(entry)
            if st["state"] == "trashed":
                cut = max(str(loaded_at), str(st.get("modified") or ""))
                # Its replacements: live files in the SAME Drive folder, loaded into the
                # same table after it was trashed. Same table alone is not enough --
                # Zepto's KBA, PCA and PDA folders all load one table.
                covers = [x for x in live_in.get(st.get("parent"), [])
                          if (table, x) in loaded_at_of and str(loaded_at_of[(table, x)]) > cut]
                by_table.setdefault(table, []).append((fid, covers, entry))
        if by_table:
            dedupe.ensure_log_table(conn)
        for table, files in by_table.items():
            dcol = dedupe.date_column(cur, table)
            if not dcol:
                continue
            ids = [f[0] for f in files]
            # One pass over the table for every trashed file's dates; the replacement
            # check per (file, date) then rides the date index.
            cur.execute(f"""
                with dead as (select fid, string_to_array(covers, ',') covers
                              from unnest(%s::text[], %s::text[]) as u(fid, covers)),
                f as (select drive_file_id fid, {dcol} d, count(*) n from public.{table}
                      where drive_file_id = any(%s) group by 1, 2)
                select f.fid, sum(f.n), count(f.d), count(*) > count(f.d), min(f.d), max(f.d),
                       count(*) filter (where f.d is not null and exists (
                           select 1 from public.{table} x
                           where x.{dcol} = f.d and x.drive_file_id = any(dead.covers)))
                from f join dead using (fid) group by 1""",
                        (ids, [",".join(f[1]) for f in files], ids))
            cover = {r[0]: r[1:] for r in cur.fetchall()}
            for fid, covers, entry in files:
                rows, dated, undated, lo, hi, covered = cover.get(fid, (0, 0, False, None, None, 0))
                entry.update(rows=int(rows or 0), dates=f"{lo}..{hi}" if lo else "")
                if not rows:
                    entry["action"] = "no rows left"
                elif undated:
                    entry["action"] = "kept: it has rows with no date to check a replacement against"
                elif covered < dated:
                    entry["action"] = f"kept: replacement not loaded for {dated - covered} of {dated} dates"
                elif apply:
                    cur.execute(f"""insert into public.{dedupe.LOG_TABLE}
                                        (table_name, deleted_id, drive_file_id, source_file, row_date,
                                         content_md5, kept_file_id, reason, row_data)
                                    select %s, t.id, t.drive_file_id, t.source_file, t.{dcol},
                                           {dedupe._content()}, null, %s, to_jsonb(t)
                                    from public.{table} t where t.drive_file_id = %s""",
                                (table, f"trashed in Drive, replaced (by {entry['by']})"[:200], fid))
                    cur.execute(f"delete from public.{table} where drive_file_id = %s", (fid,))
                    gone = cur.rowcount
                    cur.execute("delete from public.mp_loaded_files where table_name = %s and drive_file_id = %s",
                                (table, fid))
                    conn.commit()
                    entry["action"] = f"deleted {gone:,} rows"
                else:
                    entry["action"] = "would delete (replacement loaded)"
        return results
    finally:
        conn.close()


def report_lines(results: List[dict]) -> List[str]:
    lines = []
    for e in sorted(results, key=lambda e: (e["table"], e.get("dates", ""), e["file"])):
        who = f", by {e['by']}" if e.get("by") else ""
        what = "trashed in Drive" if e["state"] == "trashed" else "no longer found in Drive (sharing?)"
        rows = f" {e['rows']:,} rows {e.get('dates', '')}" if e.get("rows") else ""
        lines.append(f"  {e['table']}: {e['file']} {what}{who};{rows} -> {e['action']}")
    return lines


def main(argv=None) -> int:
    from .sink import load_dotenv
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    load_dotenv(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env"))
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--table", action="append")
    ap.add_argument("--refresh-index", action="store_true")
    args = ap.parse_args(argv)
    svc = drive.build_service()
    index = drive.load_index(svc, refresh=args.refresh_index)
    results = reconcile(svc, index, apply=args.apply, tables=args.table)
    print("\n".join(report_lines(results)) or "nothing has left Drive")
    return 0


if __name__ == "__main__":
    sys.exit(main())
