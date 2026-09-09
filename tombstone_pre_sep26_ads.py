#!/usr/bin/env python3
"""One-off: retire the raw-ads back-history that the 2026-09-09 folder move exposed.

"Visibility Data" holds far more than "4) Ads Raw Data" ever did: archive
folders the uploaders keep (Amazon/Old Data, Instamart/Old Visibility data,
Zepto/Nov-24 To Aug-25 Files), months that were never loaded, and a handful of
master workbooks. After the cutover all of it read as "never loaded" and would
have flowed into Supabase. The decision (2026-09-09) is that only data from
**September 2026 onward** loads; everything older stays in Drive.

`--since` cannot express that on its own. It keeps any file whose name carries
no parseable date, deliberately - dropping those would silently lose the bulk
back-history files that legitimately have range names. But here those are
exactly what must not load: '2023 & 2025.csv', 'Zepto_Master.xlsx',
'01-May To 28-May-26.csv'. So the cutoff is applied to the ledger instead.

Every pending file that does not positively prove it is from the cutoff month
or later gets an mp_loaded_files row with rows_written = 0: a tombstone. The
pipeline then skips it forever, while anything genuinely new still loads with
no flag on the workflow - which is why this is preferred over baking --since
into the cron, where it would also silently ignore a late correction to an
August file.

This is a CUTOVER TOOL, not pipeline logic. Run it once. Running it later would
tombstone any new undated file, which is wrong outside this one migration.

Dry run by default. --apply writes.

    python tombstone_pre_sep26_ads.py
    python tombstone_pre_sep26_ads.py --apply
"""

import argparse
import os
import sys
from collections import Counter, defaultdict

import psycopg2

from mp import drive
from mp.readers import date_from_filename
from mp.sink import load_dotenv
from mp.sources import ADS_ROOT_FOLDER_ID, ordered_sources

HERE = os.path.dirname(os.path.abspath(__file__))
CUTOFF = "2026-09"


def connect():
    return psycopg2.connect(
        host=os.environ["PGHOST"], port=os.environ.get("PGPORT", "5432"),
        user=os.environ["PGUSER"], password=os.environ["PGPASSWORD"],
        dbname=os.environ.get("PGDATABASE", "postgres"),
        sslmode=os.environ.get("PGSSLMODE", "require"),
        sslrootcert=os.environ.get("PGSSLROOTCERT"))


def wanted(name: str) -> bool:
    """Only a file whose own name dates it at or after the cutoff loads."""
    iso, _ = date_from_filename(name)
    return iso is not None and iso[:7] >= CUTOFF


def main(argv=None) -> int:
    load_dotenv(os.path.join(HERE, ".env"))
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    p.add_argument("--apply", action="store_true", help="write the tombstones")
    args = p.parse_args(argv)

    index = drive.load_index()
    if not any(r["id"] == ADS_ROOT_FOLDER_ID for r in index.get("roots", [])):
        sys.exit("index.json has no 'Visibility Data' root - "
                 "run: python -m mp.pipeline --refresh-index")

    conn = connect()
    cur = conn.cursor()
    cur.execute("select drive_file_id from mp_loaded_files")
    done = {r[0] for r in cur.fetchall()}

    rows, keep = [], []
    by_folder = Counter()
    per_source = defaultdict(lambda: [0, 0])          # tombstoned, loading
    for s in ordered_sources():
        if s.root != ADS_ROOT_FOLDER_ID:
            continue
        for f in drive.files_for(s, index):
            if f["id"] in done:
                continue
            if wanted(f["name"]):
                per_source[s.key][1] += 1
                keep.append(f)
            else:
                per_source[s.key][0] += 1
                by_folder["/".join(f["path"].split("/")[1:-1])] += 1
                rows.append((s.key, s.table, f["id"], f["name"]))

    print(f"cutoff: files dated {CUTOFF} or later load; the rest are tombstoned\n")
    print(f"{'SOURCE':34} {'TOMBSTONE':>10} {'LOADING':>8}")
    for k in sorted(per_source):
        a, b = per_source[k]
        print(f"{k:34} {a:10d} {b:8d}")
    print(f"{'TOTAL':34} {len(rows):10d} {len(keep):8d}")

    print("\ntombstoned, by folder:")
    for k, v in sorted(by_folder.items()):
        print(f"  {v:5d}  {k}")

    print(f"\nwill still load ({len(keep)}):")
    for f in sorted(keep, key=lambda x: x["path"])[:10]:
        print(f"  {f['path']}")
    if len(keep) > 10:
        print(f"  ... and {len(keep) - 10} more")

    if not args.apply:
        print("\nDry run - nothing written. Re-run with --apply.")
        return 0

    cur.executemany(
        "insert into mp_loaded_files (source_key, table_name, drive_file_id, "
        "source_file, rows_written) values (%s, %s, %s, %s, 0) "
        "on conflict (table_name, drive_file_id) do nothing", rows)
    conn.commit()
    print(f"\ntombstoned {len(rows)} files (rows_written = 0)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
