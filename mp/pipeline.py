#!/usr/bin/env python3
"""Market Place Data (Google Drive) -> Supabase.

Sources are processed in the numbered order the Drive folders give:

    1) Discount Split Data     2) Off Invoice Split Data
    3) Ads Split Data          4) Ads Raw Data (per platform)

The "Not to be uploaded in Supabase" folder is never read.

Run these in order the first time:

    python -m mp.pipeline --list-sources         # what exists, and its table
    python -m mp.pipeline --refresh-index        # walk Drive once (~460 calls)
    python -m mp.pipeline --discover             # sample files -> column types
    python -m mp.pipeline --print-schema         # writes schema.sql; paste into Supabase
    python -m mp.pipeline --check                # credentials + tables reachable
    python -m mp.pipeline --run --dry-run        # parse everything, write nothing
    python -m mp.pipeline --run --source ads_split          # one source, for real
    python -m mp.pipeline --run                             # the whole backfill

Configuration comes from .env next to this package (real environment variables
always win):

    SUPABASE_URL=https://xxxxxxxx.supabase.co
    SUPABASE_SERVICE_ROLE_KEY=eyJhbGci...   # service role: bypasses RLS for writes
    GOOGLE_TOKEN_FILE=token.json            # Drive OAuth token (drive scope)
    MP_CACHE_DIR=cache                      # downloaded files
"""

import argparse
import json
import logging
import os
import sys
from collections import defaultdict
from datetime import datetime, timezone
from typing import Dict, List, Optional

from . import dbadmin, drive, schema
from .readers import WrongShape, date_from_filename, read_file
from .schema import COMMON_DATE, COMMON_TEXT
from .sink import SupabaseSink, build_rows, load_dotenv, mask
from .sources import Source, get_source, ordered_sources, tables

logger = logging.getLogger("mp")

PROJECT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TYPES_FILE = os.path.join(PROJECT, "discovered.json")
SCHEMA_FILE = os.path.join(PROJECT, "schema.sql")


def setup_logging(verbose: bool) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s %(levelname)-7s %(message)s",
        datefmt="%H:%M:%S",
        stream=sys.stdout,
    )


def select_sources(keys: Optional[List[str]]) -> List[Source]:
    if not keys:
        return ordered_sources()
    out = []
    for k in keys:
        matches = [s for s in ordered_sources()
                   if s.key == k or s.key.startswith(k) or s.platform.lower() == k.lower()]
        if not matches:
            get_source(k)          # raises with the list of known keys
        out.extend(m for m in matches if m not in out)
    return out


# --------------------------------------------------------------------------- #
# Commands                                                                     #
# --------------------------------------------------------------------------- #

def cmd_list_sources() -> int:
    print(f"{'ORDER':6} {'KEY':28} {'TABLE':34} {'PLATFORM':10} REPORT")
    for s in ordered_sources():
        print(f"{s.order:6} {s.key:28} {s.table:34} "
              f"{s.platform or '-':10} {s.report}")
    print(f"\n{len(list(ordered_sources()))} sources -> {len(tables())} tables")
    return 0


def cmd_index(refresh: bool) -> int:
    index = drive.load_index(refresh=refresh)
    counts: Dict[str, int] = defaultdict(int)
    size: Dict[str, int] = defaultdict(int)
    for s in ordered_sources():
        for f in drive.files_for(s, index):
            counts[s.key] += 1
            size[s.key] += f["size"]
    print(f"\nindex built {index.get('built_at')} - "
          f"{len(index['folders'])} folders, {len(index['files'])} files\n")
    print(f"{'ORDER':6} {'KEY':28} {'FILES':>6} {'MB':>9}")
    total_f = total_b = 0
    for s in ordered_sources():
        print(f"{s.order:6} {s.key:28} {counts[s.key]:6d} {size[s.key]/1e6:9.1f}")
        total_f += counts[s.key]
        total_b += size[s.key]
    print(f"{'':6} {'TOTAL':28} {total_f:6d} {total_b/1e6:9.1f}")

    unmatched = [f for f in index["files"]
                 if not any(f in drive.files_for(s, index) for s in [])]
    del unmatched
    return 0


def cmd_discover(keys: Optional[List[str]], max_files: int) -> int:
    """Sample real files and cache the inferred column types per table."""
    svc = drive.build_service()
    index = drive.load_index(svc)
    existing: Dict[str, Dict[str, str]] = {}
    if os.path.exists(TYPES_FILE):
        with open(TYPES_FILE, encoding="utf-8") as fh:
            existing = json.load(fh)

    for s in select_sources(keys):
        if s.projected:
            logger.info("[DISCOVER] %s: projected source, schema is fixed - skipped",
                        s.key)
            continue
        types = schema.discover(s, svc, index, max_files=max_files)
        if not types:
            continue
        # Several sources can share a table (Flipkart PLA daily + historical),
        # so the table's column set is the union of theirs.
        existing[s.table] = schema.merge_types(existing.get(s.table, {}), types)

    with open(TYPES_FILE, "w", encoding="utf-8") as fh:
        json.dump(existing, fh, indent=1, sort_keys=True)
    print(f"\nwrote {TYPES_FILE}: {len(existing)} tables, "
          f"{sum(len(v) for v in existing.values())} columns")
    return 0


def cmd_print_schema() -> int:
    if not os.path.exists(TYPES_FILE):
        print("Run --discover first: the schema is derived from real files.",
              file=sys.stderr)
        return 1
    with open(TYPES_FILE, encoding="utf-8") as fh:
        discovered = json.load(fh)
    sql = schema.build_all_sql(discovered)
    with open(SCHEMA_FILE, "w", encoding="utf-8") as fh:
        fh.write(sql + "\n")
    print(sql)
    print(f"\n-- written to {SCHEMA_FILE}", file=sys.stderr)
    return 0


def cmd_apply_schema() -> int:
    """Apply schema.sql over a direct Postgres connection."""
    if not os.path.exists(SCHEMA_FILE):
        print("No schema.sql - run --print-schema first.", file=sys.stderr)
        return 1
    missing = dbadmin.missing_config()
    if missing:
        print("Postgres not configured, missing: " + ", ".join(missing),
              file=sys.stderr)
        return 1
    with open(SCHEMA_FILE, encoding="utf-8") as fh:
        sql = fh.read()
    before = dbadmin.database_size()
    print(f"database before: {before[1]}")
    applied, errors = dbadmin.apply_sql(sql)
    print(f"applied {applied} statements, {len(errors)} error(s)")
    for e in errors[:20]:
        print(f"  ERROR {e}")
    after = dbadmin.database_size()
    print(f"database after : {after[1]}")
    return 1 if errors else 0


def cmd_sizes() -> int:
    total, pretty = dbadmin.database_size()
    print(f"DATABASE: {pretty}  ({total/1e9:.2f} GB)\n")
    ours = set(tables())
    print(f"{'TABLE':36} {'SIZE':>10} {'ROWS':>12}  mine")
    for name, b, size, rows in dbadmin.table_sizes():
        if b < 1_000_000 and name not in ours:
            continue
        print(f"{name:36} {size:>10} {rows:12,}  {'*' if name in ours else ''}")
    return 0


def cmd_check() -> int:
    sink = SupabaseSink()
    missing = sink.missing_config()
    print(f"SUPABASE_URL : {sink.url or '(unset)'}")
    print(f"SERVICE KEY  : {mask(sink.key)}")
    print(f"TOKEN FILE   : {drive._token_path()}")
    if missing:
        print("\nMissing config: " + ", ".join(missing), file=sys.stderr)
        return 1

    ok = True
    print(f"\n{'TABLE':34} {'EXISTS':8} ROWS")
    for table in [sink.log_table] + tables():
        exists = sink.table_exists(table)
        rows = sink.count_rows(table) if exists else None
        print(f"{table:34} {'yes' if exists else 'NO':8} "
              f"{rows if rows is not None else '-'}")
        ok = ok and exists
    if not ok:
        print("\nSome tables are missing. Run --print-schema and apply the SQL "
              "in the Supabase SQL editor.", file=sys.stderr)

    try:
        drive.build_service().files().get(
            fileId=drive.ROOT_FOLDER_ID, fields="name",
            supportsAllDrives=True).execute()
        print("\nDrive: root folder reachable")
    except Exception as exc:  # noqa: BLE001
        print(f"\nDrive: NOT reachable - {exc}", file=sys.stderr)
        ok = False
    return 0 if ok else 1


# --------------------------------------------------------------------------- #
# Run                                                                          #
# --------------------------------------------------------------------------- #

def _within_since(meta: dict, since: Optional[str]) -> bool:
    """Keep a file if its date is >= `since` (YYYY-MM).

    Files whose name carries no date (bulk history) are always kept - dropping
    them would silently lose back-history.
    """
    if not since:
        return True
    iso, _ = date_from_filename(meta["name"])
    if iso is None:
        return True
    return iso[:7] >= since


def run_source(s: Source, sink: Optional[SupabaseSink], svc, index: dict,
               types_by_table: Dict[str, Dict[str, str]],
               dry_run: bool, limit: Optional[int], since: Optional[str],
               reload_files: bool) -> dict:
    started = datetime.now(timezone.utc)
    files = [f for f in drive.files_for(s, index) if _within_since(f, since)]
    seen = len(files)

    types = s.fixed_types() if s.projected else types_by_table.get(s.table)
    if not types:
        logger.warning("[%s] no discovered columns for table %s - run --discover",
                       s.key, s.table)
        return {"source": s.key, "status": "skipped-no-schema",
                "files_seen": seen, "files_loaded": 0, "rows_written": 0}

    skipped = 0
    if sink and not dry_run:
        if reload_files:
            # --reload must really re-read, so forget what the ledger claims.
            sink.forget_files(s.table)
        else:
            done = sink.completed_file_ids(s.table)
            before = len(files)
            files = [f for f in files if f["id"] not in done]
            skipped = before - len(files)
            if skipped:
                logger.info("[%s] resuming: %d file(s) already complete",
                            s.key, skipped)

    if limit:
        files = files[:limit]

    rows_written = 0
    rows_parsed = 0
    loaded = 0
    repeats = 0
    misfiled: List[str] = []
    unverified: List[str] = []
    # The source's real column shape, used to spot a file of the wrong
    # report type before any of its rows are built.
    core = {c for c in types if c not in schema._PROVENANCE}
    date_key = "sale_date" if s.projected else "report_date"
    unmapped: Dict[str, int] = defaultdict(int)
    no_date = 0
    failures: List[str] = []

    for n, meta in enumerate(files, 1):
        try:
            path = drive.download(svc, meta)
        except Exception as exc:  # noqa: BLE001
            logger.error("[%s] download failed %s: %s", s.key, meta["name"], exc)
            failures.append(f"{meta['name']}: download {exc}")
            continue

        file_rows = 0
        occurrences: Dict[str, int] = {}
        # One hash per batch, checked after the file is done. A batch that the
        # API accepted but did not persist is otherwise completely silent.
        probes: List[str] = []
        try:
            first = True
            for batch in read_file(path, s, meta):
                # Guard against a file of the wrong report type sitting in this
                # folder. Drive has at least one (a placement export filed under
                # 'Monthly - Keyword wise Format'); loading it would put 365
                # rows of the wrong shape into the table, with every real column
                # null and the actual values buried in raw_data.
                if first and not s.projected:
                    first = False
                    overlap = schema.shape_overlap(set(batch[0]), core)
                    if overlap < schema.SHAPE_MIN_OVERLAP:
                        logger.warning(
                            "[%s] SKIPPED %s - only %.0f%% of the expected "
                            "columns; looks like a different report filed in "
                            "the wrong folder", s.key, meta["name"], overlap * 100)
                        misfiled.append(f"{meta['name']} ({overlap*100:.0f}% match)")
                        break
                file_rows += len(batch)
                for row in batch:
                    if row.get(date_key) is None:
                        no_date += 1
                    for col in row:
                        if col not in types:
                            unmapped[col] += 1
                # Built even on a dry run, so type coercion, hashing and
                # de-duplication are exercised against every row rather than
                # only at load time.
                db_rows, repeated = build_rows(batch, types, occurrences,
                                               raw=not s.projected)
                repeats += repeated
                if not dry_run and sink:
                    rows_written += sink.upsert(s.table, db_rows)
                    if db_rows:
                        probes.append(db_rows[0]["row_hash"])
        except WrongShape as exc:
            logger.warning("[%s] SKIPPED %s - %s", s.key, meta["name"], exc)
            misfiled.append(f"{meta['name']} (no mapped columns)")
            continue
        except Exception as exc:  # noqa: BLE001
            logger.error("[%s] read failed %s: %s", s.key, meta["name"], exc)
            failures.append(f"{meta['name']}: read {exc}")
            continue

        rows_parsed += file_rows

        if sink and not dry_run:
            # Confirm the rows are really there before calling the file done.
            # If they are not, leave it out of the ledger so the next run
            # retries it, rather than recording a file that never landed.
            found = sink.verify_present(s.table, probes)
            if found >= 0 and found < len(probes):
                logger.error("[%s] %s: %d of %d probe rows missing after "
                             "upsert - NOT marking loaded, will retry",
                             s.key, meta["name"], len(probes) - found, len(probes))
                unverified.append(f"{meta['name']} ({len(probes)-found}/{len(probes)})")
                continue
            sink.mark_file_loaded(s.key, s.table, meta, file_rows)
        loaded += 1
        drive.discard(path)
        if n % 25 == 0 or n == len(files):
            logger.info("[%s] %d/%d files, %d rows parsed", s.key, n, len(files),
                        rows_parsed)

    ended = datetime.now(timezone.utc)
    stats = {
        "source": s.key, "table": s.table,
        "status": "dry-run" if dry_run else ("partial" if failures else "success"),
        "files_seen": seen, "files_loaded": loaded, "files_skipped": skipped,
        "rows_parsed": rows_parsed, "rows_written": rows_written,
        "repeated_lines": repeats,
        "rows_without_date": no_date,
        "unmapped_columns": dict(sorted(unmapped.items(), key=lambda x: -x[1])[:15]),
        "failures": failures[:10],
        "misfiled": misfiled[:10],
        "unverified": unverified[:10],
        "seconds": round((ended - started).total_seconds(), 1),
    }
    if sink and not dry_run:
        sink.log_run(s.key, "drive_to_supabase", started, ended,
                     {**stats, "details": {"unmapped": stats["unmapped_columns"],
                                           "failures": stats["failures"]}})
    return stats


def cmd_run(keys: Optional[List[str]], dry_run: bool, limit: Optional[int],
            since: Optional[str], reload_files: bool) -> int:
    if not os.path.exists(TYPES_FILE):
        print("Run --discover first.", file=sys.stderr)
        return 1
    with open(TYPES_FILE, encoding="utf-8") as fh:
        types_by_table = json.load(fh)

    svc = drive.build_service()
    index = drive.load_index(svc)
    sink = None
    if not dry_run:
        sink = SupabaseSink()
        missing = sink.missing_config()
        if missing:
            print("Supabase not configured: " + ", ".join(missing), file=sys.stderr)
            return 1

    results = []
    for s in select_sources(keys):
        logger.info("=== %s  %s -> %s ===", s.order, s.key, s.table)
        results.append(run_source(s, sink, svc, index, types_by_table,
                                  dry_run, limit, since, reload_files))

    print("\n" + "=" * 100)
    print("DRY RUN - nothing written" if dry_run else "LOAD COMPLETE")
    print("=" * 100)
    print(f"{'KEY':28} {'FILES':>6} {'ROWS':>10} {'WRITTEN':>9} {'REPEATS':>8} "
          f"{'NO-DATE':>8} {'UNMAPPED':>8} {'FAIL':>5}")
    tot = defaultdict(int)
    for r in results:
        print(f"{r['source']:28} {r.get('files_loaded', 0):6d} "
              f"{r.get('rows_parsed', 0):10d} {r.get('rows_written', 0):9d} "
              f"{r.get('repeated_lines', 0):8d} "
              f"{r.get('rows_without_date', 0):8d} "
              f"{len(r.get('unmapped_columns', {})):8d} "
              f"{len(r.get('failures', [])):5d}")
        for k in ("files_loaded", "rows_parsed", "rows_written",
                  "rows_without_date", "repeated_lines"):
            tot[k] += r.get(k, 0)
    print(f"{'TOTAL':28} {tot['files_loaded']:6d} {tot['rows_parsed']:10d} "
          f"{tot['rows_written']:9d} {tot['repeated_lines']:8d} "
          f"{tot['rows_without_date']:8d}")

    problems = [r for r in results if r.get("failures")
                or r.get("unmapped_columns") or r.get("misfiled")
                or r.get("unverified")]
    if problems:
        print("\n--- attention ---")
        for r in problems:
            if r.get("unmapped_columns"):
                print(f"  {r['source']}: columns not in schema -> "
                      f"{list(r['unmapped_columns'])[:8]}")
            for u in r.get("unverified", []):
                print(f"  {r['source']}: NOT VERIFIED IN DB, will retry -> {u}")
            for m in r.get("misfiled", []):
                print(f"  {r['source']}: WRONG REPORT TYPE, skipped -> {m}")
            for f in r.get("failures", []):
                print(f"  {r['source']}: FAILED {f[:150]}")
    return 0


# --------------------------------------------------------------------------- #

def main(argv: Optional[List[str]] = None) -> int:
    load_dotenv(os.path.join(PROJECT, ".env"))
    p = argparse.ArgumentParser(
        prog="mp.pipeline",
        description="Market Place Data (Drive) -> Supabase",
        formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--list-sources", action="store_true")
    p.add_argument("--index", action="store_true",
                   help="show per-source file counts from the cached index")
    p.add_argument("--refresh-index", action="store_true",
                   help="re-walk the Drive tree (needed when new months appear)")
    p.add_argument("--discover", action="store_true",
                   help="sample real files to derive column types")
    p.add_argument("--discover-files", type=int, default=4,
                   help="files sampled per source during discovery (default 4)")
    p.add_argument("--print-schema", action="store_true",
                   help="emit schema.sql from the discovered columns")
    p.add_argument("--check", action="store_true",
                   help="verify Supabase credentials, tables and Drive access")
    p.add_argument("--run", action="store_true")
    p.add_argument("--dry-run", action="store_true",
                   help="parse everything and report, but write nothing")
    p.add_argument("--source", action="append", metavar="KEY",
                   help="limit to a source key, prefix, or platform (repeatable)")
    p.add_argument("--limit", type=int, metavar="N",
                   help="at most N files per source")
    p.add_argument("--since", metavar="YYYY-MM",
                   help="skip files whose name dates them before this month")
    p.add_argument("--reload", action="store_true",
                   help="re-read files already present in the table")
    p.add_argument("--apply-schema", action="store_true",
                   help="run schema.sql against Postgres directly (DDL cannot go "
                        "through PostgREST)")
    p.add_argument("--sizes", action="store_true",
                   help="report database and per-table disk usage")
    p.add_argument("--purge-cache", action="store_true")
    p.add_argument("--purge-table", action="store_true",
                   help="DELETE every row in the selected sources' tables before "
                        "loading. Needed when row identity changes.")
    p.add_argument("-v", "--verbose", action="store_true")
    args = p.parse_args(argv)
    setup_logging(args.verbose)

    if args.apply_schema:
        return cmd_apply_schema()
    if args.sizes:
        return cmd_sizes()
    if args.purge_cache:
        print(f"freed {drive.purge_cache()/1e6:.1f} MB")
        return 0
    if args.purge_table:
        sink = SupabaseSink()
        if sink.missing_config():
            print("Supabase not configured", file=sys.stderr)
            return 1
        targets = []
        for s in select_sources(args.source):
            if s.table not in targets:
                targets.append(s.table)
        for t in targets:
            print(f"{t}: removed {sink.purge_table(t)} rows")
        return 0
    if args.list_sources:
        return cmd_list_sources()
    if args.refresh_index or args.index:
        return cmd_index(refresh=args.refresh_index)
    if args.discover:
        return cmd_discover(args.source, args.discover_files)
    if args.print_schema:
        return cmd_print_schema()
    if args.check:
        return cmd_check()
    if args.run:
        return cmd_run(args.source, args.dry_run, args.limit, args.since,
                       args.reload)
    p.print_help()
    return 0


if __name__ == "__main__":
    sys.exit(main())
