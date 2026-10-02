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


def _campaign_key(cur, table: str) -> Optional[str]:
    """SQL for a row's campaign, whichever of these columns the export carries (Zepto's
    exports renamed campaign_name to campaignname, so both exist with one null)."""
    cur.execute("""select column_name from information_schema.columns
                   where table_schema = 'public' and table_name = %s
                     and column_name in ('campaign_id', 'campaign_name', 'campaignname')""", (table,))
    present = {r[0] for r in cur.fetchall()}
    cols = [f"t.{c}::text" for c in ("campaign_id", "campaign_name", "campaignname") if c in present]
    return f"coalesce({', '.join(cols)})" if cols else None


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
            key = _campaign_key(cur, table)
            if not dcol or not key:
                for _, _, entry in files:
                    entry["action"] = "kept: no date or campaign column to match a replacement on"
                continue
            # One scan of the table for every trashed file's rows (drive_file_id has no
            # index; Instamart is 17M rows). Everything after works on this copy and
            # reaches back into the table only by date, which is indexed.
            cur.execute("drop table if exists _tf")
            cur.execute(f"""create temp table _tf as
                            select t.id, t.drive_file_id fid, t.{dcol} d, {key} k
                            from public.{table} t where t.drive_file_id = any(%s)""",
                        ([f[0] for f in files],))
            xkey = key.replace("t.", "x.")
            for fid, covers, entry in files:
                # Row by row: a row goes only when a replacement file has the SAME
                # CAMPAIGN on the same date. Date coverage alone was not enough -- the
                # Amazon 30-Sep export lacks campaign-days the 29-Sep one had, and
                # deleting by date took Rs 6.6 L of real September spend with it.
                cur.execute("drop table if exists _ok")
                cur.execute(f"""create temp table _ok as
                                select f.id from _tf f
                                where f.fid = %s and f.d is not null and exists (
                                    select 1 from public.{table} x
                                    where x.{dcol} = f.d and x.drive_file_id = any(%s) and {xkey} = f.k)""",
                            (fid, covers))
                cur.execute("""select count(*), count(d), min(d), max(d),
                                      (select count(*) from _ok) from _tf where fid = %s""", (fid,))
                rows, dated, lo, hi, covered = cur.fetchone()
                entry.update(rows=int(rows or 0), dates=f"{lo}..{hi}" if lo else "")
                if not rows:
                    entry["action"] = "no rows left"
                elif not covered:
                    entry["action"] = "kept: no replacement has these campaigns on these dates yet"
                elif apply:
                    cur.execute(f"""insert into public.{dedupe.LOG_TABLE}
                                        (table_name, deleted_id, drive_file_id, source_file, row_date,
                                         content_md5, kept_file_id, reason, row_data)
                                    select %s, t.id, t.drive_file_id, t.source_file, t.{dcol},
                                           {dedupe._content()}, null, %s, to_jsonb(t)
                                    from public.{table} t
                                    where t.{dcol} between %s and %s and t.id in (select id from _ok)""",
                                (table, f"trashed in Drive, replaced (by {entry['by']})"[:200], lo, hi))
                    cur.execute(f"""delete from public.{table} t
                                    where t.{dcol} between %s and %s and t.id in (select id from _ok)""",
                                (lo, hi))
                    gone = cur.rowcount
                    left = rows - gone
                    if not left:
                        # Restoring the file in Drive then makes the next run load it again.
                        cur.execute("delete from public.mp_loaded_files where table_name = %s and drive_file_id = %s",
                                    (table, fid))
                    conn.commit()
                    entry["action"] = f"deleted {gone:,} rows" + (
                        f"; kept {left:,} whose campaign-day no replacement has" if left else "")
                else:
                    entry["action"] = (f"would delete {covered:,} rows"
                                       + (f"; keep {rows - covered:,} with no replacement" if rows - covered else ""))
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
