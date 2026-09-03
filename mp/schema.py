"""Derive each table's columns from real files, and emit the CREATE TABLE SQL.

Hand-listing ~500 columns across 26 report types would be wrong within a month -
platforms rename columns without warning. Instead this samples actual files per
source and infers a type per column.

Nothing depends on the inference being complete: a column that appears later, or
one this missed, still reaches Postgres inside raw_data (jsonb) and is queryable
as raw_data->>'key'. Promote it to a typed column by re-running discovery and
applying the `alter table` lines it emits.
"""

import logging
from collections import defaultdict
from typing import Dict, List, Optional, Sequence, Tuple

from . import drive, readers
from .sources import (ID_COLUMNS, SALES_COLUMNS, SALES_TABLE, SALES_TYPES,
                      Source, ordered_sources)

logger = logging.getLogger("mp.schema")

#: Provenance columns every table carries, in a fixed order.
COMMON_TEXT = ["source_file", "drive_file_id", "source_path", "platform",
               "date_grain"]
COMMON_DATE = ["report_date"]

#: Column names that are dates regardless of what the values look like.
_DATE_HINT = ("date", "month", "_at", "flight_start", "flight_end",
              "start_time", "end_time", "timestamp")
#: ...except these, which merely contain a date word but are not dates.
_DATE_ANTI = ("date_grain", "update", "days", "date_range")

LOG_TABLE = "workflow_logs"


def _is_date_name(col: str) -> bool:
    if any(a in col for a in _DATE_ANTI):
        return False
    return any(h in col for h in _DATE_HINT)


def infer_types(samples: Dict[str, List], source: Source) -> Dict[str, str]:
    """column -> 'date' | 'numeric' | 'text'."""
    types: Dict[str, str] = {}
    declared_dates = set(source.date_columns)
    forced_text = set(source.text_columns) | ID_COLUMNS

    for col, values in samples.items():
        vals = [v for v in values if v is not None][:400]
        if col in forced_text:
            types[col] = "text"
            continue
        if col in declared_dates or _is_date_name(col):
            # Only believe it if the values agree; a column called
            # 'campaign_creation_date' full of free text stays text.
            if vals and sum(1 for v in vals if readers.parse_date(v) is not None) \
                    >= len(vals) * 0.8:
                types[col] = "date"
                continue
        if vals and sum(1 for v in vals
                        if readers.parse_number(v) is not None) >= len(vals) * 0.9:
            types[col] = "numeric"
            continue
        types[col] = "text"
    return types


#: Provenance columns are attached to every row regardless of report type, so
#: they must not count towards judging whether two files are the same report.
_PROVENANCE = set(COMMON_TEXT) | set(COMMON_DATE)

#: A file whose columns overlap the source's core shape by less than this is a
#: different report type that has been filed in the wrong folder.
SHAPE_MIN_OVERLAP = 0.4


def core_columns(column_sets: Sequence[set]) -> set:
    """Columns present in at least half the files - the source's real shape."""
    if not column_sets:
        return set()
    counts: Dict[str, int] = defaultdict(int)
    for cols in column_sets:
        for c in cols - _PROVENANCE:
            counts[c] += 1
    threshold = max(1, len(column_sets) / 2)
    return {c for c, n in counts.items() if n >= threshold}


def shape_overlap(cols: set, core: set) -> float:
    """How much of the source's core shape this file actually has."""
    if not core:
        return 1.0
    return len((cols - _PROVENANCE) & core) / len(core)


def _drop_foreign_files(source: Source, per_file):
    """Discard sampled files that are a different report type.

    Drive contains at least one genuinely misfiled export - a placement report
    sitting in 'PLA/Monthly - Keyword wise Format' - and typing its columns into
    the keyword table would both pollute the schema and load 365 rows of the
    wrong shape. This keeps bulk back-history files, which are legitimate
    outliers: they share the core columns and merely add more.
    """
    if len(per_file) < 3:
        return per_file
    core = core_columns([cols for _, cols, _ in per_file])
    kept, rejected = [], []
    for meta, cols, vals in per_file:
        if shape_overlap(cols, core) < SHAPE_MIN_OVERLAP:
            rejected.append((meta, shape_overlap(cols, core)))
        else:
            kept.append((meta, cols, vals))
    for meta, ov in rejected:
        logger.warning("[DISCOVER] %s: %s looks like a different report "
                       "(%.0f%% of the expected columns) - excluded from the "
                       "schema. Check it is filed in the right folder.",
                       source.key, meta["name"], ov * 100)
    return kept or per_file


def discover(source: Source, svc, index: dict, max_files: int = 4,
             max_batches: int = 2) -> Dict[str, str]:
    """Sample a source's files and return column -> type.

    Samples the newest file, the oldest file, and a couple in between, because
    column sets drift over time and the union is what the table must hold.
    """
    files = drive.files_for(source, index)
    if not files:
        logger.warning("[DISCOVER] %s: no files under %s", source.key, source.folder)
        return {}

    if len(files) <= max_files:
        picks = files
    else:
        step = max(1, len(files) // (max_files - 1))
        picks = [files[0]] + files[step::step][:max_files - 2] + [files[-1]]
        # Always sample the largest file as well. Bulk back-history exports are
        # both the biggest files and the ones whose columns differ most from the
        # dailies (they carry their own Date column, extra metrics, and a
        # different header), and sampling only by position misses them - which
        # left 312k Flipkart keyword rows with a null report_date.
        largest = max(files, key=lambda f: f["size"])
        if largest not in picks:
            picks.append(largest)

    per_file: List[Tuple[dict, set, Dict[str, List]]] = []
    for meta in picks:
        try:
            path = drive.download(svc, meta)
        except Exception as exc:  # noqa: BLE001
            logger.warning("[DISCOVER] %s: cannot fetch %s: %s",
                           source.key, meta["name"], exc)
            continue
        seen_cols: set = set()
        vals: Dict[str, List] = defaultdict(list)
        for i, batch in enumerate(readers.read_file(path, source, meta)):
            for row in batch[:200]:
                for col, val in row.items():
                    seen_cols.add(col)
                    if len(vals[col]) < 400:
                        vals[col].append(val)
            if i + 1 >= max_batches:
                break
        if seen_cols:
            per_file.append((meta, seen_cols, vals))

    kept = _drop_foreign_files(source, per_file)

    samples: Dict[str, List] = defaultdict(list)
    for _, _, vals in kept:
        for col, values in vals.items():
            samples[col].extend(values[:400 - len(samples[col])])

    types = infer_types(samples, source)
    for col in COMMON_TEXT:
        types[col] = "text"
    for col in COMMON_DATE:
        types[col] = "date"
    logger.info("[DISCOVER] %s: %d columns from %d file(s)",
                source.key, len(types), len(picks))
    return types


def merge_types(a: Dict[str, str], b: Dict[str, str]) -> Dict[str, str]:
    """Union two column maps, widening to the safer type on disagreement.

    text > date > numeric: if one file's 'orders' column holds 'N/A', the column
    has to be text or the load fails on that row.
    """
    rank = {"numeric": 0, "date": 1, "text": 2}
    out = dict(a)
    for col, t in b.items():
        out[col] = t if col not in out else max(out[col], t, key=lambda x: rank[x])
    return out


# --------------------------------------------------------------------------- #
# SQL                                                                          #
# --------------------------------------------------------------------------- #

def build_table_sql(table: str, types: Dict[str, str],
                    sources: Sequence[Source] = ()) -> str:
    """CREATE TABLE + indexes for one target table. Idempotent."""
    ordered = (COMMON_TEXT + COMMON_DATE
               + sorted(c for c in types if c not in COMMON_TEXT + COMMON_DATE))
    sql = {"date": "date", "numeric": "numeric", "text": "text"}

    lines = []
    if sources:
        lines.append(f"-- {table}")
        for s in sources:
            lines.append(f"--   {s.order} {s.platform or 'ALL'} | {s.report}")
            lines.append(f"--     Drive: {s.folder}"
                         + (f" [sheet: {s.sheet}]" if s.sheet else ""))
            if s.note:
                for chunk in _wrap(s.note, 68):
                    lines.append(f"--     {chunk}")
    lines.append(f"create table if not exists public.{table} (")
    lines.append("    id                bigint generated always as identity primary key,")
    lines.append("    row_hash          text        not null unique,")
    width = max([len(c) for c in ordered] + [17])
    for col in ordered:
        lines.append(f"    {col.ljust(width)} {sql[types.get(col, 'text')]},")
    lines.append("    processed_at      timestamptz not null default now(),")
    lines.append("    raw_data          jsonb,")
    lines.append("    created_at        timestamptz not null default now()")
    lines.append(");")
    lines.append("")
    # Case-insensitive lookup of already-loaded files. PostgREST's in_() is
    # case-sensitive, so the skip-existing check runs against this column.
    lines.append(f"alter table public.{table}")
    lines.append("    add column if not exists source_file_lower text")
    lines.append("    generated always as (lower(source_file)) stored;")
    lines.append("")
    # `create table if not exists` does nothing once the table exists, so a
    # column discovered later would never be added. These make the file
    # converge instead of merely create: re-run it after any --discover.
    lines.append(f"-- Column evolution: no-ops on a fresh table, and the reason "
                 f"re-running")
    lines.append(f"-- this file picks up columns discovered after it was first "
                 f"applied.")
    for col in ordered:
        lines.append(f"alter table public.{table} add column if not exists "
                     f"{col} {sql[types.get(col, 'text')]};")
    lines.append("")
    lines.append(f"comment on column public.{table}.row_hash is")
    lines.append("    'sha256 of the typed row; makes re-runs idempotent via upsert';")
    lines.append(f"comment on column public.{table}.raw_data is")
    lines.append("    'overflow only: columns this table has no typed column "
                 "for. NULL when there are none';")
    lines.append("")
    for col, idx in (("source_file_lower", "source_file_lower"),
                     ("report_date", "report_date"),
                     ("platform", "platform")):
        lines.append(f"create index if not exists {table}_{idx}_idx "
                     f"on public.{table} ({col});")
    lines.append("")
    lines.append("-- No GIN index on raw_data by design. It is the single most "
                 "expensive index")
    lines.append("-- available here, and raw_data is now overflow-only and "
                 "usually NULL, so it")
    lines.append("-- would cost storage to index almost nothing. If a column "
                 "does start")
    lines.append("-- arriving in raw_data and needs querying, prefer promoting "
                 "it to a typed")
    lines.append("-- column (re-run --discover). Add the index only if you "
                 "really need it:")
    lines.append(f"--   create index {table}_raw_data_idx "
                 f"on public.{table} using gin (raw_data);")
    return "\n".join(lines)


LOADED_TABLE = "mp_loaded_files"


def build_loaded_table_sql() -> str:
    """Ledger of files whose rows are ALL in, used to resume safely.

    Skipping on 'does this table already have rows from that file name' is not
    good enough: a file interrupted part-way through its upserts looks loaded,
    so a resume skips it and its remaining rows are lost for good. A row is
    written here only after every batch of a file has been upserted, so a
    resume re-reads exactly the files that did not finish.
    """
    return f"""create table if not exists public.{LOADED_TABLE} (
    id            bigint generated always as identity primary key,
    source_key    text        not null,
    table_name    text        not null,
    drive_file_id text        not null,
    source_file   text        not null,
    rows_written  integer,
    loaded_at     timestamptz not null default now(),
    unique (table_name, drive_file_id)
);

create index if not exists {LOADED_TABLE}_table_idx
    on public.{LOADED_TABLE} (table_name);

comment on table public.{LOADED_TABLE} is
    'One row per fully-loaded source file. Written only after every batch of
     that file succeeded, so an interrupted run resumes without gaps.';"""


def build_sales_table_sql() -> str:
    """The unified sales table. Fixed, not discovered.

    Exactly the six columns of 'Sales Column Name for All Platform.xlsx' -
    platform plus SKU Code, SKU Name, Date, QTY, Sub City - and the minimal
    provenance the ledger and dedup need. No raw_data: the requirement is that
    only the named columns reach the database.
    """
    cols = "\n".join(
        f"    {c.ljust(14)} {SALES_TYPES.get(c, 'text')},"
        for c in ["platform"] + SALES_COLUMNS)
    return f"""-- {SALES_TABLE}: 5) Sales Raw Data, all platforms, projected to the spec's
-- six columns. Scootsy rows carry platform = 'Instamart'.
create table if not exists public.{SALES_TABLE} (
    id             bigint generated always as identity primary key,
    row_hash       text        not null unique,
{cols}
    source_file    text,
    drive_file_id  text,
    processed_at   timestamptz not null default now(),
    created_at     timestamptz not null default now()
);

alter table public.{SALES_TABLE}
    add column if not exists source_file_lower text
    generated always as (lower(source_file)) stored;

create index if not exists {SALES_TABLE}_source_file_lower_idx
    on public.{SALES_TABLE} (source_file_lower);
create index if not exists {SALES_TABLE}_sale_date_idx
    on public.{SALES_TABLE} (sale_date);
create index if not exists {SALES_TABLE}_platform_idx
    on public.{SALES_TABLE} (platform);
create index if not exists {SALES_TABLE}_sku_code_idx
    on public.{SALES_TABLE} (sku_code);"""


def build_log_table_sql() -> str:
    return f"""create table if not exists public.{LOG_TABLE} (
    id               bigint generated always as identity primary key,
    source           text,
    workflow         text        not null,
    started_at       timestamptz not null,
    ended_at         timestamptz not null,
    duration_seconds numeric,
    status           text,
    files_seen       integer,
    files_loaded     integer,
    files_skipped    integer,
    rows_written     integer,
    details          jsonb,
    created_at       timestamptz not null default now()
);

create index if not exists {LOG_TABLE}_started_at_idx
    on public.{LOG_TABLE} (started_at desc);

-- workflow_logs is shared with the GRN schedulers and already exists there with
-- a narrower column set, so `create table if not exists` above is a no-op and
-- these columns have to be added explicitly. Without them every run logs
-- PGRST204 "Could not find the 'files_loaded' column".
alter table public.{LOG_TABLE} add column if not exists source text;
alter table public.{LOG_TABLE} add column if not exists files_seen integer;
alter table public.{LOG_TABLE} add column if not exists files_loaded integer;
alter table public.{LOG_TABLE} add column if not exists files_skipped integer;
alter table public.{LOG_TABLE} add column if not exists rows_written integer;
alter table public.{LOG_TABLE} add column if not exists duration_seconds numeric;
alter table public.{LOG_TABLE} add column if not exists status text;
alter table public.{LOG_TABLE} add column if not exists details jsonb;"""


def _wrap(text: str, width: int) -> List[str]:
    words, line, out = text.split(), "", []
    for w in words:
        if len(line) + len(w) + 1 > width:
            out.append(line)
            line = w
        else:
            line = f"{line} {w}".strip()
    if line:
        out.append(line)
    return out


def build_all_sql(discovered: Dict[str, Dict[str, str]]) -> str:
    """Full schema file for every table, in the numbered source order."""
    by_table: Dict[str, List[Source]] = defaultdict(list)
    for s in ordered_sources():
        by_table[s.table].append(s)

    parts = [
        "-- Market Place Data (Google Drive) -> Supabase.",
        "-- Generated by: python -m mp.pipeline --print-schema",
        "-- Run this whole file in the Supabase SQL editor. Safe to re-run.",
        "--",
        "-- Folder order follows the numbered Drive folders:",
        "--   1) Discount Split  2) Off Invoice Split  3) Ads Split  4) Ads Raw",
        "-- The 'Not to be uploaded in Supabase' folder is never read.",
        "",
        build_log_table_sql(),
        "",
        build_loaded_table_sql(),
        "",
    ]
    for table, srcs in by_table.items():
        if all(s.projected for s in srcs):
            parts.append(build_sales_table_sql())
            parts.append("")
            continue
        types = discovered.get(table)
        if not types:
            parts.append(f"-- {table}: discovery found no columns (no readable "
                         f"files sampled); skipped.\n")
            continue
        parts.append(build_table_sql(table, types, srcs))
        parts.append("")
    return "\n".join(parts)
