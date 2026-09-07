#!/usr/bin/env python3
"""One-off: carry the mp_sales ledger over to the Power BI sales folder.

Until 2026-09-07 the sales sources read "Market Place Data/5) Sales Raw Data",
which was a hand-made copy of the uploader's own "New Power BI Format Daily
Sales" folder. The sources now read that folder directly, so the copying stops.

The ledger (mp_loaded_files) records loaded files by Drive file id, and the
same file has a different id in the new folder. Without this script the next
run would re-download and re-parse ~2,600 files whose rows are already in
mp_sales (a no-op on content, but hours of work). So: for every file in the
new folder that matches a loaded file from the old folder on source, name and
byte size, insert a ledger row under the new id, copying rows_written and
loaded_at. Files with no match are left alone and load normally.

First Club is the exception: three of its August files were re-uploaded with
different content. --reset-firstclub removes its rows and ledger entries so the
whole platform reloads from the new folder (352 rows, seconds).

Dry run by default. --apply writes.

    python seed_sales_ledger.py                    # report only
    python seed_sales_ledger.py --apply --reset-firstclub
"""

import argparse
import json
import os
import sys
from collections import defaultdict

import psycopg2

from mp import drive
from mp.sink import load_dotenv
from mp.sources import SALES_TABLE, ordered_sources

HERE = os.path.dirname(os.path.abspath(__file__))
OLD_INDEX = os.path.join(HERE, "cache", "index_old_market_place_data.json")
RESET_PLATFORM = "First Club"
RESET_SOURCE = "sales_firstclub"


def connect():
    return psycopg2.connect(
        host=os.environ["PGHOST"], port=os.environ.get("PGPORT", "5432"),
        user=os.environ["PGUSER"], password=os.environ["PGPASSWORD"],
        dbname=os.environ.get("PGDATABASE", "postgres"),
        sslmode=os.environ.get("PGSSLMODE", "require"),
        sslrootcert=os.environ.get("PGSSLROOTCERT"))


def main(argv=None) -> int:
    load_dotenv(os.path.join(HERE, ".env"))
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    p.add_argument("--apply", action="store_true", help="write the ledger rows")
    p.add_argument("--reset-firstclub", action="store_true",
                   help=f"delete {RESET_PLATFORM} rows + ledger so it reloads")
    args = p.parse_args(argv)

    with open(OLD_INDEX, encoding="utf-8") as fh:
        old_index = json.load(fh)
    old_by_id = {f["id"]: f for f in old_index["files"]}
    new_index = drive.load_index()
    if not any(r["id"] != old_index["root_id"] for r in new_index.get("roots", [])):
        sys.exit("index.json has no sales root - run --refresh-index first")

    conn = connect()
    cur = conn.cursor()
    cur.execute("select source_key, drive_file_id, source_file, rows_written, "
                "loaded_at from mp_loaded_files where table_name = %s",
                (SALES_TABLE,))
    ledger = cur.fetchall()
    already = {r[1] for r in ledger}
    # (source_key, name, size) -> ledger row, via the old index for the size
    by_key = {}
    unknown_in_old_index = 0
    for src, fid, name, rows, at in ledger:
        old = old_by_id.get(fid)
        if not old:
            unknown_in_old_index += 1
            continue
        by_key.setdefault((src, name, old["size"]), (fid, rows, at))
    print(f"old ledger: {len(ledger)} rows, {unknown_in_old_index} not in old index")

    inserts = []
    summary = defaultdict(lambda: [0, 0, 0])   # seeded, to-load, already
    to_load = defaultdict(list)
    for s in ordered_sources():
        if s.table != SALES_TABLE:
            continue
        for f in drive.files_for(s, new_index):
            if f["id"] in already:
                summary[s.key][2] += 1
                continue
            if args.reset_firstclub and s.key == RESET_SOURCE:
                summary[s.key][1] += 1
                to_load[s.key].append(f["name"])
                continue
            hit = by_key.get((s.key, f["name"], f["size"]))
            if hit:
                inserts.append((s.key, SALES_TABLE, f["id"], f["name"], hit[1], hit[2]))
                summary[s.key][0] += 1
            else:
                summary[s.key][1] += 1
                to_load[s.key].append(f["name"])

    print(f"\n{'SOURCE':30} {'SEEDED':>7} {'TO LOAD':>8} {'ALREADY':>8}")
    for k, (a, b, c) in summary.items():
        print(f"{k:30} {a:7d} {b:8d} {c:8d}")
    tot = [sum(v[i] for v in summary.values()) for i in range(3)]
    print(f"{'TOTAL':30} {tot[0]:7d} {tot[1]:8d} {tot[2]:8d}")
    for k, names in to_load.items():
        print(f"\n{k}: would load {len(names)} file(s)")
        for n in names:
            print(f"    {n}")

    if not args.apply:
        print("\nDry run - nothing written. Re-run with --apply.")
        return 0

    if args.reset_firstclub:
        cur.execute("delete from mp_sales where platform = %s", (RESET_PLATFORM,))
        print(f"\n{RESET_PLATFORM}: removed {cur.rowcount} rows from {SALES_TABLE}")
        cur.execute("delete from mp_loaded_files where table_name = %s and "
                    "source_key = %s", (SALES_TABLE, RESET_SOURCE))
        print(f"{RESET_PLATFORM}: removed {cur.rowcount} ledger rows")
    cur.executemany(
        "insert into mp_loaded_files (source_key, table_name, drive_file_id, "
        "source_file, rows_written, loaded_at) values (%s, %s, %s, %s, %s, %s) "
        "on conflict (table_name, drive_file_id) do nothing", inserts)
    conn.commit()
    print(f"\nseeded {len(inserts)} ledger rows")
    return 0


if __name__ == "__main__":
    sys.exit(main())
