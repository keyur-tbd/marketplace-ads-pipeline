#!/usr/bin/env python3
"""One-off: carry the raw-ads ledger over to the "Visibility Data" folder.

Until 2026-09-09 the 22 raw ads sources read "Market Place Data/4) Ads Raw
Data". The uploaders now file everything in "Visibility Data" instead, so the
sources read that root and the old folder is frozen (excluded in ROOTS).

The new folder is a *copy*, not a move: none of its 4,129 files shares a Drive
id with the old ones, and 3,416 of them are byte-identical to files already in
Supabase. The ledger (mp_loaded_files) records loaded files by Drive file id,
so without this script the next run would re-download and re-parse all of them.
The row_hash upsert means that would not create duplicate rows - provenance is
excluded from the hash, so a copy collapses onto the row it already wrote - but
it is days of work for no new data, and it cannot finish inside a 330-minute
job. Instamart alone is ~3.5 GB.

So: for every file in the new folder that matches a loaded file on source key,
name and byte size, insert a ledger row under the new id, copying rows_written
and loaded_at. Files with no match are left alone and load normally - those are
the ~715 genuinely new files, which include new months of every report plus
some historical Amazon search-term data that the old folder never held.

Dry run by default. --apply writes.

    python seed_ads_ledger.py                 # report only
    python seed_ads_ledger.py --apply

Run `python -m mp.pipeline --refresh-index` first: the index must already hold
the new root, or there is nothing to seed against.
"""

import argparse
import json
import os
import sys
from collections import defaultdict

import psycopg2

from mp import drive
from mp.sink import load_dotenv
from mp.sources import ADS_ROOT_FOLDER_ID, ordered_sources

HERE = os.path.dirname(os.path.abspath(__file__))
#: Index snapshot taken before the cutover, while "4) Ads Raw Data" was still
#: walked. It is the only place the old files' byte sizes survive, and the
#: ledger stores no size of its own.
OLD_INDEX = os.path.join(HERE, "cache", "index_old_market_place_data_ads.json")


def connect():
    return psycopg2.connect(
        host=os.environ["PGHOST"], port=os.environ.get("PGPORT", "5432"),
        user=os.environ["PGUSER"], password=os.environ["PGPASSWORD"],
        dbname=os.environ.get("PGDATABASE", "postgres"),
        sslmode=os.environ.get("PGSSLMODE", "require"),
        sslrootcert=os.environ.get("PGSSLROOTCERT"))


def ads_sources():
    return [s for s in ordered_sources() if s.root == ADS_ROOT_FOLDER_ID]


def main(argv=None) -> int:
    load_dotenv(os.path.join(HERE, ".env"))
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    p.add_argument("--apply", action="store_true", help="write the ledger rows")
    args = p.parse_args(argv)

    with open(OLD_INDEX, encoding="utf-8") as fh:
        old_index = json.load(fh)
    old_by_id = {f["id"]: f for f in old_index["files"]}

    index = drive.load_index()
    if not any(r["id"] == ADS_ROOT_FOLDER_ID for r in index.get("roots", [])):
        sys.exit("index.json has no 'Visibility Data' root - "
                 "run: python -m mp.pipeline --refresh-index")

    tables = sorted({s.table for s in ads_sources()})
    conn = connect()
    cur = conn.cursor()
    cur.execute("select source_key, table_name, drive_file_id, source_file, "
                "rows_written, loaded_at from mp_loaded_files "
                "where table_name = any(%s)", (tables,))
    ledger = cur.fetchall()
    already = {r[2] for r in ledger}

    # (source_key, name.lower(), size) -> (rows_written, loaded_at)
    by_key = {}
    orphans = 0
    for src, table, fid, name, rows, at in ledger:
        old = old_by_id.get(fid)
        if not old:
            orphans += 1
            continue
        by_key.setdefault((src, name.lower(), old["size"]), (rows, at))
    print(f"ledger: {len(ledger)} rows across {len(tables)} tables, "
          f"{orphans} not in the old index snapshot")

    inserts = []
    summary = defaultdict(lambda: [0, 0, 0])      # seeded, to-load, already
    to_load = defaultdict(list)
    for s in ads_sources():
        for f in drive.files_for(s, index):
            if f["id"] in already:
                summary[s.key][2] += 1
                continue
            hit = by_key.get((s.key, f["name"].lower(), f["size"]))
            if hit:
                inserts.append((s.key, s.table, f["id"], f["name"], hit[0], hit[1]))
                summary[s.key][0] += 1
            else:
                summary[s.key][1] += 1
                to_load[s.key].append(f["name"])

    print(f"\n{'SOURCE':34} {'SEEDED':>7} {'TO LOAD':>8} {'ALREADY':>8}")
    for s in ads_sources():
        a, b, c = summary[s.key]
        print(f"{s.key:34} {a:7d} {b:8d} {c:8d}")
    tot = [sum(v[i] for v in summary.values()) for i in range(3)]
    print(f"{'TOTAL':34} {tot[0]:7d} {tot[1]:8d} {tot[2]:8d}")

    print(f"\nfiles that will actually load: {tot[1]}")
    for k in sorted(to_load):
        names = to_load[k]
        print(f"  {k}: {len(names)}")
        for n in sorted(names)[:6]:
            print(f"      {n}")
        if len(names) > 6:
            print(f"      ... and {len(names) - 6} more")

    if not args.apply:
        print("\nDry run - nothing written. Re-run with --apply.")
        return 0

    cur.executemany(
        "insert into mp_loaded_files (source_key, table_name, drive_file_id, "
        "source_file, rows_written, loaded_at) values (%s, %s, %s, %s, %s, %s) "
        "on conflict (table_name, drive_file_id) do nothing", inserts)
    conn.commit()
    print(f"\nseeded {len(inserts)} ledger rows")
    return 0


if __name__ == "__main__":
    sys.exit(main())
