"""The daily data-gaps mail: what is missing in the marketplace data, in plain words.

    python -m mp.gaps_report            # print it
    python -m mp.gaps_report --send     # print it and mail it (GAPS_MAIL_TO, comma separated)

Run after the sync, so it describes what Supabase holds NOW. Six questions:

  1. How far behind is each party's ADS feed, and which days inside the last 60 are missing?
  2. How far behind is each party's SECONDARY SALES (sell-out) feed?
  3. Which files sit in Drive and are NOT in Supabase -- never loaded, or edited in Drive
     after we loaded them? (Files deliberately skipped at the 9 Sep 2026 cutover are not
     nagged about: a skipped file that nobody has touched since is a decision, not a gap.)
  4. Does what the parties' ad portals report still agree with what finance booked?
  5. Is any row loaded twice -- the same rows, or the same line restated, from more than one file?
  6. Which loaded files were taken out of Drive and still count, waiting on a replacement?

It only reads. It never raises into the workflow: a report that cannot be built says so in
the mail rather than failing the sync it follows.
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import date, datetime, timedelta, timezone
from typing import Dict, List

from . import dbadmin, drive
from .sources import ordered_sources

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from etl_alerts import send_mail  # noqa: E402

logger = logging.getLogger(__name__)

ADS_LATE_DAYS = 3        # an ads export normally lands 2-3 days after the day it describes
SALES_LATE_DAYS = 3
LOOKBACK_DAYS = 60
RECENT_FILE_DAYS = 45    # only files touched this recently are reported as "not loaded"
# portal ads as % of finance ads: outside this band is worth a look. Zepto sits near 220%
# because its export counts the free-of-cost credits finance never pays for.
PORTAL_BAND = {"Zepto": (120, 270)}
DEFAULT_BAND = (75, 130)


def _rows(cur, sql: str, params=()) -> List[tuple]:
    cur.execute(sql, params)
    return list(cur.fetchall())


def _runs(days: List[date]) -> str:
    """[1 Sep, 2 Sep, 3 Sep, 9 Sep] -> '1-3 Sep, 9 Sep'."""
    out, i = [], 0
    days = sorted(days)
    while i < len(days):
        j = i
        while j + 1 < len(days) and (days[j + 1] - days[j]).days == 1:
            j += 1
        a, b = days[i], days[j]
        out.append(f"{a.day} {a:%b}" if a == b else (f"{a.day}-{b.day} {b:%b}" if a.month == b.month else f"{a.day} {a:%b} - {b.day} {b:%b}"))
        i = j + 1
    return ", ".join(out)


def ads_section(cur, today: date) -> tuple[List[str], int]:
    lines, issues = [], 0
    since = today - timedelta(days=LOOKBACK_DAYS)
    rows = _rows(cur, "select platform, max(day) from warehouse.ads_feed_days group by 1 order by 1")
    have: Dict[str, set] = {}
    for platform, day in _rows(cur, "select platform, day from warehouse.ads_feed_days where day >= %s", (since,)):
        have.setdefault(platform, set()).add(day)
    for platform, last in rows:
        behind = (today - last).days
        flag = "LATE" if behind > ADS_LATE_DAYS else "ok"
        if behind > ADS_LATE_DAYS:
            issues += 1
        missing = [since + timedelta(days=k) for k in range((last - since).days + 1)
                   if since + timedelta(days=k) not in have.get(platform, set())]
        line = f"  {platform:<12} latest {last:%d %b}, {behind} day(s) behind  [{flag}]"
        if missing:
            issues += 1
            line += f"\n               missing inside the last {LOOKBACK_DAYS} days: {_runs(missing)} ({len(missing)} day(s))"
        lines.append(line)
    return lines, issues


def sales_section(cur, today: date) -> tuple[List[str], int]:
    lines, issues = [], 0
    rows = _rows(cur, """select platform, max(sale_date) from warehouse.sales_daily
                          where stream = 'secondary' group by 1 order by 1""")
    for platform, last in rows:
        behind = (today - last).days
        late = behind > SALES_LATE_DAYS
        issues += 1 if late else 0
        lines.append(f"  {platform:<14} latest {last:%d %b}, {behind} day(s) behind  [{'LATE' if late else 'ok'}]")
    return lines, issues


def files_section(cur) -> tuple[List[str], int]:
    """Drive against the ledger: recent files never loaded, and files edited after their load."""
    lines: List[str] = []
    ledger: Dict[tuple, tuple] = {}
    for table, fid, rows_written, loaded_at in _rows(
            cur, "select table_name, drive_file_id, rows_written, loaded_at from public.mp_loaded_files"):
        ledger[(table, fid)] = (rows_written, loaded_at)
    index = drive.load_index()
    cutoff = datetime.now(timezone.utc) - timedelta(days=RECENT_FILE_DAYS)
    never, edited = [], []
    for s in ordered_sources():
        for f in drive.files_for(s, index):
            try:
                modified = datetime.fromisoformat(str(f.get("modified", "")).replace("Z", "+00:00"))
            except ValueError:
                continue
            entry = ledger.get((s.table, f["id"]))
            where = f"{s.key}: {f.get('path') or f['name']}"
            if entry is None:
                if modified >= cutoff:
                    never.append(f"  {where}  (in Drive since {modified:%d %b})")
            else:
                loaded_at = entry[1]
                if loaded_at is not None and (modified - loaded_at).total_seconds() > 600:
                    kind = "skipped at the cutover, then edited" if not entry[0] else "edited after we loaded it"
                    edited.append(f"  {where}  ({kind}: Drive {modified:%d %b %H:%M} UTC, loaded {loaded_at:%d %b %H:%M} UTC)")
    if never:
        lines.append(f"In Drive, not in Supabase ({len(never)}):")
        lines += never[:40] + ([f"  ... and {len(never) - 40} more"] if len(never) > 40 else [])
    if edited:
        lines.append(f"Edited in Drive after the last load -- the next run reloads these ({len(edited)}):")
        lines += edited[:40] + ([f"  ... and {len(edited) - 40} more"] if len(edited) > 40 else [])
    if not lines:
        lines.append("  Every file in Drive is loaded, and none has changed since.")
    return lines, len(never) + len(edited)


def portal_section(cur) -> tuple[List[str], int]:
    lines, issues = [], 0
    rows = _rows(cur, """
        with m as (select max(month) mm from warehouse.spend_sales_monthly where ads is not null)
        select s.platform, to_char(s.month, 'Mon YYYY'), sum(s.ads), sum(s.ads_raw)
          from warehouse.spend_sales_monthly s, m
         where s.month = m.mm and (s.ads is not null or s.ads_raw is not null)
         group by 1, 2 having coalesce(sum(s.ads), 0) > 0 or coalesce(sum(s.ads_raw), 0) > 0
         order by sum(s.ads) desc nulls last""")
    for platform, month, fin, raw in rows:
        fin, raw = float(fin or 0), float(raw or 0)
        if not fin:
            lines.append(f"  {platform:<12} {month}: portal Rs {raw:,.0f}, finance has booked nothing")
            continue
        pct = 100 * raw / fin
        lo, hi = PORTAL_BAND.get(platform, DEFAULT_BAND)
        off = not (lo <= pct <= hi)
        issues += 1 if off else 0
        lines.append(f"  {platform:<12} {month}: portal {pct:5.0f}% of finance (Rs {raw:,.0f} vs Rs {fin:,.0f})"
                     f"  [{'CHECK' if off else 'ok'}, usual {lo}-{hi}%]")
    last = _rows(cur, "select to_char(max(month), 'Mon YYYY') from warehouse.spend_sales_monthly where ads is not null")
    lines.append(f"  Finance spend (Feeder File) is loaded through {last[0][0] if last and last[0][0] else 'nothing yet'}.")
    return lines, issues


def trashed_section() -> tuple[List[str], int]:
    """Loaded files no longer in Drive whose rows are still counted (mp/trashed.py).
    The sync just before this mail already removed every trashed file whose replacement
    had loaded, so what is left is waiting on a replacement, or vanished (sharing?)."""
    from . import trashed
    svc = drive.build_service()
    open_items = [e for e in trashed.reconcile(svc, drive.load_index(svc), apply=False)
                  if e["action"] not in ("no rows left",) and not e["action"].startswith("deleted")]
    lines = trashed.report_lines(open_items)
    if not lines:
        lines = ["  None: every file removed from Drive has had its rows replaced."]
    return lines, len(open_items)


def duplicates_section(cur, today: date) -> tuple[List[str], int]:
    """Rows counted twice because more than one file carries them (mp/dedupe.py).

    Exact duplicates should never be here -- the load removes them -- so any is a fault.
    'Restated' lines are the same ad line on the same day exported with different numbers
    by several files (an interim and a final export of a month); Amazon's are resolved
    automatically (newest file wins), everyone else's need a person to say which file is
    the real one, and sums double-count them until then.
    """
    from . import dedupe
    since = str(today - timedelta(days=LOOKBACK_DAYS))
    ads = dedupe.ads_tables()
    lines, issues = [], 0
    for table in sorted({s.table for s in ordered_sources()}):
        found = dedupe.audit(cur, table, since, None, restated=True, ads=ads)
        dup = [(m, extra) for m, _, extra, _ in found if extra]
        rest = [(m, n) for m, _, _, n in found if n]
        if dup:
            issues += 1
            lines.append(f"  {table}: {sum(e for _, e in dup):,} duplicate rows ("
                         + ", ".join(f"{m:%b %Y} {e:,}" for m, e in dup)
                         + ") -- the load should have removed these; check the run log")
        if rest:
            issues += 1
            months = ", ".join(f"{m:%b %Y} {n:,}" for m, n in rest)
            if table in dedupe.SUPERSEDE_TABLES:
                lines.append(f"  {table}: {sum(n for _, n in rest):,} rows are lines restated by more than one "
                             f"file ({months}) that the newest-file rule did not resolve -- its lines were "
                             "not unique within a file there; check the run log.")
            else:
                lines.append(f"  {table}: {sum(n for _, n in rest):,} rows are lines that more than one "
                             f"file exports with different numbers ({months}). Which file is the final "
                             "one? Until one is removed, totals count both.")
    if not lines:
        lines.append(f"  No row is loaded twice in the last {LOOKBACK_DAYS} days.")
    return lines, issues


def build() -> tuple[str, str]:
    today = datetime.now(timezone(timedelta(hours=5, minutes=30))).date()
    parts: List[str] = []
    total = 0
    conn = dbadmin.connect(timeout=120)
    try:
        cur = conn.cursor()
        for title, fn, args in [
            ("1. ADS FEEDS (the parties' own ad exports)", ads_section, (cur, today)),
            ("2. SECONDARY SALES FEEDS (sell-out)", sales_section, (cur, today)),
            ("3. FILES: DRIVE AGAINST SUPABASE", files_section, (cur,)),
            ("4. PORTAL ADS AGAINST FINANCE", portal_section, (cur,)),
            ("5. ROWS LOADED TWICE (the same rows from more than one file)", duplicates_section, (cur, today)),
            ("6. FILES TAKEN OUT OF DRIVE AFTER WE LOADED THEM", trashed_section, ()),
        ]:
            parts.append(title)
            try:
                lines, n = fn(*args)
                total += n
                parts += lines
            except Exception as exc:  # noqa: BLE001
                logger.exception("section failed: %s", title)
                total += 1
                parts.append(f"  This section could not be built: {str(exc)[:200]}")
                try:
                    conn.rollback()
                except Exception:  # noqa: BLE001
                    pass
            parts.append("")
    finally:
        conn.close()
    head = [f"Marketplace data gaps, {today:%A %d %b %Y}",
            f"{total} thing(s) need a look." if total else "Nothing needs a look today.",
            "An ads or sales export normally lands 2-3 days after the day it describes; 'LATE' is beyond that.",
            ""]
    subject = f"Birbal data gaps {today:%d %b}: " + (f"{total} to look at" if total else "all clear")
    return subject, "\n".join(head + parts + ["-- Birbal (sent after the 3 PM marketplace sync)"])


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    ap = argparse.ArgumentParser()
    ap.add_argument("--send", action="store_true")
    args = ap.parse_args()
    subject, body = build()
    print(subject)
    print(body)
    if args.send:
        to = [x.strip() for x in os.environ.get("GAPS_MAIL_TO", "").split(",") if x.strip()]
        ok = send_mail(subject, body, to)
        print("mailed to " + ", ".join(to) if ok else "NOT mailed (no recipients or no Gmail credentials)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
