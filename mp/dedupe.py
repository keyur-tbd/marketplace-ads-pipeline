"""Duplicate rows across files: find them, and keep every table free of them.

    python -m mp.dedupe --audit                    # every table, whole history, report only
    python -m mp.dedupe --audit --table zepto_campaign_performance --since 2026-08-01
    python -m mp.dedupe --clean                    # apply the rules below to every table
    python -m mp.dedupe --clean --table instamart_ads_performance --since 2026-08-01 --until 2026-08-31

Why this exists when row_hash already upserts duplicates away. row_hash is taken over
the row as the loader built it at the time -- typed columns, raw_data overflow,
date_grain -- so it is only stable while the loader is. Instamart's Aug-26-1.csv
(loaded 2 Sep from 'Market Place Data') and Aug-26-2.csv (21 Sep, from 'Visibility
Data') carry 21k identical rows a day whose hashes differ, because the loader changed
between the two loads, and both files also repeat 15-19% of their own rows where every
other month repeats none. Nothing collapsed any of it: August read Rs 157.8 L of spend
against Rs 89.2 L real. So duplicates are judged here on what is STORED, not on row_hash:

  content = every column of the stored row except provenance and load bookkeeping
            (id, source/drive file, path, processed_at, created_at, row_hash, date_grain)
  line    = the row's text and date columns (plus the few numeric identifiers in
            ID_NUMERIC): what the row is ABOUT, without its numbers

Rule 1, exact duplicates (every table, automatic):
  * ads exports (the "4.x" sources) list each line once per day, so rows with identical
    content on the same date are one row however many files -- or copies inside one
    file -- carry them. One is kept, the earliest loaded.
  * sales and the finance splits can genuinely list the same line N times (four equal
    discount events are four events: sink.row_hash's `occurrence`), so there a content
    keeps max(N per file) copies, all from the file holding the most.

Rule 2, newest file wins (SUPERSEDE_TABLES only, automatic):
  Amazon's exports restate: "Campaign_report -01-Sep-26 to 29-Sep-26.csv" and "...to
  30-Sep-26.csv" both carry 1-29 Sep, attribution maturing in between; a bulk
  "Feb-25 To Mar-26" history repeats the monthly files. For the same date and line in
  several files, only the most recently loaded file's rows stay. Sep 2026 Amazon
  campaign spend summed Rs 62.3 L across 20 such files against Rs 14.7 L in the newest.
  Applied only while a line really identifies one row inside a file (LINE_UNIQUE_MIN of
  a window's rows); if not, the table is reported and left alone.

Rule 3, same delivery (ads tables, automatic):
  the same line with the same spend / impressions / clicks / views in two files is one
  delivery, whatever else differs -- matured attribution (more GMV for the same day in a
  later export) or a value the old loader kept in raw_data. The newest file's copy stays.
  This is what finished Instamart's August: rule 1 took it to Rs 113.6 L, rule 3 to 89.3 L.

Everything deleted is written to mp_dedupe_log first -- the full row for rule 2, whose
numbers differ from what is kept; for rule 1 the kept file is enough, the content is the
same. Rows with no date are never touched. Same line with different numbers in tables
outside SUPERSEDE_TABLES is counted as "restated" by the audit and reported in the daily
gaps mail, not deleted: which version is right there is a business call.
"""
from __future__ import annotations

import argparse
import logging
import os
import re
import sys
from datetime import date
from typing import List, Optional, Sequence, Tuple

from . import dbadmin

logger = logging.getLogger("mp.dedupe")

LOG_TABLE = "mp_dedupe_log"

#: Not content. date_grain is in here because it is derived from the FILE NAME, not
#: the row (readers.py), so the same row read from a daily and a monthly file differs
#: only there.
META_COLS = ("id", "source_file", "drive_file_id", "source_path", "source_file_lower",
             "processed_at", "created_at", "row_hash", "date_grain")

#: Numeric columns that identify a line rather than measure it. Every other ID in these
#: exports is stored as text.
ID_NUMERIC = ("subcampaign_id",)

#: Exports that restate earlier ones (cumulative month-to-date files, bulk histories).
SUPERSEDE_TABLES = frozenset({
    "amz_sp_campaign_daily", "amz_sp_campaign_report", "amz_sp_budget_report",
    "amz_sp_placement_report", "amz_sp_advertised_product", "amz_sp_search_term_report",
    "amz_search_term_impression",
})

#: Share of a window's rows whose line is unique within their file, below which rule 2
#: refuses to run: the line columns do not identify a row there.
LINE_UNIQUE_MIN = 0.99

_NUMERIC = ("numeric", "double precision", "real", "integer", "bigint", "smallint")


def ads_tables() -> frozenset:
    from .sources import ordered_sources
    return frozenset(s.table for s in ordered_sources() if str(s.order).startswith("4"))


def log_table_ddl() -> str:
    return f"""create table if not exists public.{LOG_TABLE} (
    id            bigserial primary key,
    table_name    text not null,
    deleted_id    bigint not null,
    drive_file_id text,
    source_file   text,
    row_date      date,
    content_md5   text not null,
    kept_file_id  text,
    reason        text not null,
    row_data      jsonb,
    deleted_at    timestamptz not null default now()
);
create index if not exists {LOG_TABLE}_table_idx on public.{LOG_TABLE} (table_name, deleted_at);
comment on table public.{LOG_TABLE} is
  'Rows the marketplace pipeline deleted as duplicates (mp/dedupe.py). exact: same content as a kept row from kept_file_id on that date -- undo by copying that row back with this provenance. superseded: the same line from a newer file; row_data holds the deleted row.';
alter table public.{LOG_TABLE} enable row level security;"""


def date_column(cur, table: str) -> Optional[str]:
    cur.execute("""select column_name from information_schema.columns
                   where table_schema = 'public' and table_name = %s
                     and column_name in ('report_date', 'sale_date')""", (table,))
    cols = {r[0] for r in cur.fetchall()}
    return "report_date" if "report_date" in cols else ("sale_date" if "sale_date" in cols else None)


def _content(alias: str = "t") -> str:
    keys = ",".join(META_COLS)
    return f"md5((to_jsonb({alias}) - '{{{keys}}}'::text[])::text)"


def _line_cols(cur, table: str, alias: str = "t") -> List[str]:
    cur.execute("""select column_name, data_type from information_schema.columns
                   where table_schema = 'public' and table_name = %s order by ordinal_position""",
                (table,))
    return [f"{alias}.{c}" for c, kind in cur.fetchall()
            if c not in META_COLS and c != "raw_data" and (kind not in _NUMERIC or c in ID_NUMERIC)]


def _line(cur, table: str, alias: str = "t") -> str:
    return f"md5(concat_ws('|', {', '.join(_line_cols(cur, table, alias))}))"


#: What a line DELIVERED: spend, impressions, clicks, views under each platform's names.
#: Ratios, ranks, CVRs and "missed"/"last year" figures are not delivery.
_DELIVERY = re.compile(r"^(?!.*(per_|_per|cpc|ctr|roas|rate|share|last_year|avg|average|ecpm|ecpc|cpm"
                       r"|percent|pct|_roi|rank|cvr|missed)).*(impression|click|spend|budget_burnt"
                       r"|budget_consumed|^cost$|_cost$|views)")


def _delivery(cur, table: str) -> List[str]:
    cur.execute("""select column_name, data_type from information_schema.columns
                   where table_schema = 'public' and table_name = %s order by ordinal_position""",
                (table,))
    return [c for c, kind in cur.fetchall() if kind in _NUMERIC and _DELIVERY.search(c)]


def _window(dcol: str, since: Optional[str], until: Optional[str]) -> Tuple[str, list]:
    where, params = [f"{dcol} is not null"], []
    if since:
        where.append(f"{dcol} >= %s")
        params.append(since)
    if until:
        where.append(f"{dcol} <= %s")
        params.append(until)
    return " and ".join(where), params


def _exact_keepers(collapse_within: bool) -> str:
    """SQL over `cc` (id, d, f, h): the ids rule 1 deletes."""
    if collapse_within:
        return """select cc.*, k.kf kept from cc join (
                      select distinct on (d, h) d, h, id kid, f kf from cc order by d, h, id
                  ) k using (d, h) where cc.id <> k.kid"""
    return """select cc.*, keeper.f kept from cc join (
                  select distinct on (d, h) d, h, f from (
                      select d, h, f, count(*) n, min(id) first_id from cc group by 1, 2, 3
                  ) n order by d, h, n desc, first_id
              ) keeper using (d, h)
              where cc.f is distinct from keeper.f"""


def audit(cur, table: str, since: Optional[str] = None, until: Optional[str] = None,
          restated: bool = True, ads: Optional[frozenset] = None) -> List[tuple]:
    """Per month: (month, rows, exact duplicates rule 1 would delete, rows of lines
    restated by more than one file)."""
    dcol = date_column(cur, table)
    if not dcol:
        return []
    ads = ads_tables() if ads is None else ads
    where, params = _window(dcol, since, until)
    if table in ads:
        extra_sql = "sum(total - 1)"
        group = "select d, h, count(*) total from c group by 1, 2"
    else:
        extra_sql = "sum(total - keep)"
        group = """select d, h, sum(k) total, max(k) keep from (
                       select d, h, f, count(*) k from c group by 1, 2, 3) n group by 1, 2"""
    cur.execute(f"""
        with c as (select {dcol} d, drive_file_id f, {_content()} h from public.{table} t where {where}),
        g as ({group})
        select date_trunc('month', d)::date, sum(total)::bigint, {extra_sql}::bigint
        from g group by 1 order by 1""", params)
    rows = {m: [m, total, extra, 0] for m, total, extra in cur.fetchall()}
    if restated and rows:
        cur.execute(f"""
            with c as (select {dcol} d, drive_file_id f, {_line(cur, table)} k, {_content()} h
                       from public.{table} t where {where}),
            g as (select d, k from c group by 1, 2
                  having count(distinct f) > 1 and count(distinct h) > 1)
            select date_trunc('month', c.d)::date, count(*)::bigint
            from c join g using (d, k) group by 1""", params)
        for m, n in cur.fetchall():
            if m in rows:
                rows[m][3] = n
    return [tuple(r) for r in rows.values()]


def line_uniqueness(cur, table: str, since: Optional[str], until: Optional[str]) -> float:
    dcol = date_column(cur, table)
    where, params = _window(dcol, since, until)
    cur.execute(f"""select count(*), count(distinct (drive_file_id, {dcol}, {_line(cur, table)}))
                    from public.{table} t where {where}""", params)
    n, distinct = cur.fetchone()
    return 1.0 if not n else distinct / n


def _delete_logged(cur, table: str, reason: str, full_row: bool) -> int:
    row = "(select to_jsonb(t) from public." + table + " t where t.id = _dd.id)" if full_row else "null"
    cur.execute(f"""insert into public.{LOG_TABLE}
                        (table_name, deleted_id, drive_file_id, source_file, row_date,
                         content_md5, kept_file_id, reason, row_data)
                    select %s, id, f, sf, d, h, kept, %s, {row} from _dd""", (table, reason))
    cur.execute(f"delete from public.{table} t using _dd where t.id = _dd.id")
    deleted = cur.rowcount
    cur.execute("drop table if exists _dd")
    return deleted


def dedupe(cur, table: str, since: Optional[str], until: Optional[str],
           only_file: Optional[str] = None, reason: str = "cleanup",
           ads: Optional[frozenset] = None) -> Tuple[int, int]:
    """Apply rule 1, then rule 2 where it applies, to the date window. With `only_file`,
    only dates/contents/lines that file holds are considered (the guard after a load).
    Returns (exact rows deleted, superseded rows deleted). Caller commits."""
    dcol = date_column(cur, table)
    if not dcol:
        return 0, 0
    ads = ads_tables() if ads is None else ads
    where, params = _window(dcol, since, until)

    focus, fparams = "", []
    if only_file:
        focus = "where (d, h) in (select d, h from c where f = %s)"
        fparams = [only_file]
    cur.execute(f"""
        create temp table _dd as
        with c as (select id, {dcol} d, drive_file_id f, source_file sf, {_content()} h
                   from public.{table} t where {where}),
        cc as (select * from c {focus})
        {_exact_keepers(table in ads)}""", params + fparams)
    exact = _delete_logged(cur, table, f"exact: {reason}"[:200], full_row=False)

    superseded = 0
    if table in SUPERSEDE_TABLES:
        share = line_uniqueness(cur, table, since, until)
        if share < LINE_UNIQUE_MIN:
            logger.warning("[dedupe] %s: only %.1f%% of lines are unique within their file "
                           "in %s..%s - newest-file rule NOT applied", table, share * 100,
                           since, until)
        else:
            focus = ""
            if only_file:
                focus = "where (d, k) in (select d, k from c where f = %s)"
            cur.execute(f"""
                create temp table _dd as
                with c as (select id, {dcol} d, drive_file_id f, source_file sf, created_at,
                                  {_line(cur, table)} k, {_content()} h
                           from public.{table} t where {where}),
                cc as (select * from c {focus}),
                g as (select d, k from cc group by 1, 2 having count(distinct f) > 1),
                newest as (
                    select distinct on (d, k) d, k, f from (
                        select d, k, f, max(created_at) ca, max(id) mid
                        from cc join g using (d, k) group by 1, 2, 3
                    ) x order by d, k, ca desc, mid desc
                )
                select cc.id, cc.d, cc.f, cc.sf, cc.h, newest.f kept
                from cc join newest using (d, k) where cc.f is distinct from newest.f""",
                        params + fparams)
            superseded = _delete_logged(cur, table, f"superseded: {reason}"[:200], full_row=True)

    # Rule 3, ads tables: the same line with the same DELIVERY (spend, impressions,
    # clicks...) in two files is one delivery, whatever else differs -- a later export
    # with matured attribution (more conversions/GMV for the same day), or a value the
    # old loader kept in raw_data and the new one types. Matching on the delivery
    # numbers as well as the line makes a false match practically impossible, so unlike
    # rule 2 this needs no uniqueness check. Rows that delivered nothing are left alone.
    # The newest file's copy stays: its attribution is the most mature.
    delivery = _delivery(cur, table) if table in ads else []
    if delivery:
        active = " or ".join(f"coalesce(t.{c}, 0) <> 0" for c in delivery)
        key = "md5(concat_ws('|', " + ", ".join(_line_cols(cur, table) + [f"t.{c}" for c in delivery]) + "))"
        focus = "where (d, k) in (select d, k from c where f = %s)" if only_file else ""
        cur.execute(f"""
            create temp table _dd as
            with c as (select id, {dcol} d, drive_file_id f, source_file sf, created_at,
                              {key} k, {_content()} h
                       from public.{table} t where {where} and ({active})),
            cc as (select * from c {focus}),
            g as (select d, k from cc group by 1, 2 having count(distinct f) > 1),
            newest as (
                select distinct on (d, k) d, k, f from (
                    select d, k, f, max(created_at) ca, max(id) mid
                    from cc join g using (d, k) group by 1, 2, 3
                ) x order by d, k, ca desc, mid desc
            )
            select cc.id, cc.d, cc.f, cc.sf, cc.h, newest.f kept
            from cc join newest using (d, k) where cc.f is distinct from newest.f""",
                    params + fparams)
        superseded += _delete_logged(cur, table, f"same delivery: {reason}"[:200], full_row=False)
    return exact, superseded


def delete_file_rows(cur, table: str, drive_file_id: str) -> int:
    """Remove every row an earlier load of this Drive file wrote. Used before an
    edited-in-Drive file is reloaded: its new version replaces the old one, rather than
    landing on top of it (the repeated rows a growing monthly export used to leave)."""
    cur.execute(f"delete from public.{table} where drive_file_id = %s", (drive_file_id,))
    return cur.rowcount


class Guard:
    """The load loop's handle on the rules above.

    A connection per call: a backfill run lasts hours and the pooler drops idle
    sessions. Without the direct Postgres credentials (PGHOST...) it does nothing and
    says so once -- the load still runs, and the daily audit in the gaps mail is then
    what catches a duplicate.
    """

    def __init__(self) -> None:
        self.enabled = not dbadmin.missing_config()
        self.ads = ads_tables()
        if not self.enabled:
            logger.warning("[dedupe] PG* not configured: cross-file duplicate guard is OFF")
            return
        conn = self._connect()
        try:
            ensure_log_table(conn)
        finally:
            conn.close()

    @staticmethod
    def _connect():
        conn = dbadmin.connect(timeout=1800)
        conn.cursor().execute("set statement_timeout = '30min'")
        return conn

    def replace_file(self, table: str, drive_file_id: str) -> int:
        """Drop an edited file's previous rows before its new version loads."""
        if not self.enabled:
            return 0
        conn = self._connect()
        try:
            n = delete_file_rows(conn.cursor(), table, drive_file_id)
            conn.commit()
            return n
        finally:
            conn.close()

    def after_load(self, table: str, drive_file_id: str, name: str,
                   lo: Optional[str], hi: Optional[str]) -> Tuple[int, int]:
        """Apply both rules to what the file just loaded. Returns (exact, superseded)."""
        if not self.enabled or not lo:
            return 0, 0
        conn = self._connect()
        try:
            out = dedupe(conn.cursor(), table, lo, hi, only_file=drive_file_id,
                         reason=f"load {name}", ads=self.ads)
            conn.commit()
            return out
        finally:
            conn.close()


def ensure_log_table(conn) -> None:
    cur = conn.cursor()
    # DDL only when something is missing: several clean-up processes started together
    # all ran `create ... if not exists` + `alter table` on it at once and deadlocked.
    cur.execute("""select count(*) from information_schema.columns
                   where table_schema = 'public' and table_name = %s and column_name = 'row_data'""",
                (LOG_TABLE,))
    if cur.fetchone()[0]:
        conn.commit()
        return
    for stmt in dbadmin.split_statements(log_table_ddl()):
        cur.execute(stmt)
    # Added after the first version of the table; harmless when already there.
    cur.execute(f"alter table public.{LOG_TABLE} add column if not exists row_data jsonb")
    conn.commit()


def _months(cur, table: str, since: Optional[str], until: Optional[str]) -> List[Tuple[str, str]]:
    dcol = date_column(cur, table)
    if not dcol:
        return []
    where, params = _window(dcol, since, until)
    cur.execute(f"""select distinct date_trunc('month', {dcol})::date from public.{table}
                    where {where} order by 1""", params)
    out = []
    for (m,) in cur.fetchall():
        nxt = date(m.year + (m.month == 12), m.month % 12 + 1, 1)
        last = date.fromordinal(nxt.toordinal() - 1)
        out.append((max(str(m), since or ""), min(str(last), until or "9999")))
    return out


def main(argv: Optional[Sequence[str]] = None) -> int:
    from .sink import load_dotenv
    from .sources import ordered_sources
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    load_dotenv(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env"))
    ap = argparse.ArgumentParser()
    mode = ap.add_mutually_exclusive_group(required=True)
    mode.add_argument("--audit", action="store_true")
    mode.add_argument("--clean", action="store_true")
    ap.add_argument("--table", action="append")
    ap.add_argument("--since")
    ap.add_argument("--until")
    ap.add_argument("--no-restated", action="store_true", help="skip the slower restated count")
    args = ap.parse_args(argv)

    tables = args.table or sorted({s.table for s in ordered_sources()})
    ads = ads_tables()
    conn = dbadmin.connect(timeout=1800)
    cur = conn.cursor()
    cur.execute("set statement_timeout = '30min'")
    if args.clean:
        ensure_log_table(conn)
    totals = [0, 0]
    for table in tables:
        # Month by month: the big tables (17M rows) cannot be hashed in one statement
        # inside the pooler's limits, and a month is also the unit people check.
        for lo, hi in _months(cur, table, args.since, args.until):
            if args.audit:
                for m, rows, extra, restated in audit(cur, table, lo, hi, not args.no_restated, ads):
                    if extra or restated:
                        print(f"{table:38} {m:%b %Y}  rows {rows:>10,}  duplicates {extra:>9,}"
                              f"  restated {restated:>8,}", flush=True)
                    totals[0] += extra
                    totals[1] += restated
            else:
                exact, sup = dedupe(cur, table, lo, hi, reason="cleanup", ads=ads)
                conn.commit()
                if exact or sup:
                    print(f"{table:38} {lo[:7]}  deleted {exact:,} exact duplicates, "
                          f"{sup:,} superseded rows", flush=True)
                totals[0] += exact
                totals[1] += sup
    if args.audit:
        print(f"\nduplicate rows: {totals[0]:,}   rows in restated lines: {totals[1]:,}")
    else:
        print(f"\ndeleted: {totals[0]:,} exact duplicates, {totals[1]:,} superseded rows")
    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
