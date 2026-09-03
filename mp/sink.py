"""Everything that touches Supabase. No Google APIs in here.

Follows the same contract as the GRN schedulers' supabase_sink.py so the two
pipelines behave identically in production:

  * rows are upserted on `row_hash`, so re-running a load is a no-op rather than
    a duplicate;
  * `row_hash` is taken over a row's CONTENT only, never its provenance, so the
    same row reaching us twice - once in a daily export, once inside a bulk
    back-history file covering the same period - is stored once;
  * whole files already loaded are skipped via the generated source_file_lower
    column, so a nightly run only pays for new files;
  * each run writes one workflow_logs record.
"""

import hashlib
import json
import logging
import os
import time
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

from .readers import parse_date, parse_number
from .schema import COMMON_DATE, COMMON_TEXT, LOADED_TABLE, LOG_TABLE

logger = logging.getLogger("mp.sink")

try:
    from supabase import Client, create_client
    SUPABASE_AVAILABLE = True
except ImportError:  # pragma: no cover
    Client = Any  # type: ignore
    create_client = None  # type: ignore
    SUPABASE_AVAILABLE = False


def chunked(items: Sequence[Any], size: int) -> Iterable[Sequence[Any]]:
    for start in range(0, len(items), size):
        yield items[start:start + size]


def load_dotenv(path: str) -> None:
    """Load KEY=VALUE lines. Real environment variables always win."""
    if not os.path.exists(path):
        return
    with open(path, encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, value = line.partition("=")
            key, value = key.strip(), value.strip().strip('"').strip("'")
            if key and key not in os.environ:
                os.environ[key] = value


def normalize_supabase_url(url: str) -> str:
    """Reduce whatever is in .env to the bare project origin.

    create_client() appends '/rest/v1' itself. The GRN schedulers' .env stores
    the full REST endpoint, so copying that value verbatim produced
    '.../rest/v1/rest/v1/<table>' and a 404 on every table - which looks exactly
    like the tables not existing.
    """
    url = (url or "").strip().rstrip("/")
    if url and not url.startswith("http"):
        url = "https://" + url
    for suffix in ("/rest/v1", "/rest"):
        if url.endswith(suffix):
            url = url[: -len(suffix)]
    return url.rstrip("/")


def mask(secret: Optional[str]) -> str:
    if not secret:
        return "(unset)"
    return f"{secret[:6]}...{secret[-4:]} ({len(secret)} chars)"


# --------------------------------------------------------------------------- #
# Row building                                                                 #
# --------------------------------------------------------------------------- #

#: Excluded from row_hash. Provenance is deliberately NOT part of a row's
#: identity: the same day's campaign row arrives both in a daily export and
#: again inside a bulk back-history file covering that period, and those are one
#: row, not two. Hashing source_file would make them two. processed_at changes
#: every run and must never participate.
HASH_EXCLUDE = frozenset({
    "processed_at", "source_file", "drive_file_id", "source_path", "row_hash",
})


def row_hash(payload: Dict[str, Any], occurrence: int) -> str:
    """Deterministic identity for a row: its content, plus which copy it is.

    `occurrence` is what separates the two things people call a duplicate:

      * A re-run, or the same day appearing in both a daily export and a bulk
        back-history file, replays a file's rows in the same order, so copy N
        hashes the same both times and the upsert collapses it. No duplicates.

      * A file that genuinely lists the same line four times - four separate
        discount events on one SKU that happen to share an amount - yields
        copies 0..3 with distinct hashes, so all four survive and still sum
        correctly. Dropping them would silently understate the total.
    """
    canonical = json.dumps(payload, sort_keys=True, default=str, ensure_ascii=False)
    return hashlib.sha256(f"{canonical}#{occurrence}".encode("utf-8")).hexdigest()


def to_db_row(row: Dict[str, Any], types: Dict[str, str],
              occurrence: int = 0, raw: bool = True) -> Dict[str, Any]:
    """Normalized row -> table row, coercing each column to its declared type.

    Nothing is dropped: the full normalized row goes into raw_data, so a column
    that discovery never saw is still stored and queryable.
    """
    out: Dict[str, Any] = {}
    for col, kind in types.items():
        value = row.get(col)
        if kind == "date":
            out[col] = parse_date(value)
        elif kind == "numeric":
            out[col] = parse_number(value)
        else:
            out[col] = None if value is None else str(value)

    out["processed_at"] = datetime.now(timezone.utc).isoformat()

    # raw_data carries only the keys with no typed column of their own.
    #
    # Storing the whole row here instead would trebled the table: every value
    # would sit in a typed column AND again as jsonb text, and the GIN index
    # would then be indexing that duplicate. Overflow-only keeps the guarantee
    # that nothing is silently dropped - a column the discovery pass never saw
    # is still stored and queryable as raw_data->>'key' - at a fraction of the
    # size. NULL rather than '{}' when there is no overflow, which is the
    # common case, because a null costs a bit in the null bitmap and no more.
    if raw:
        overflow = {k: v for k, v in row.items() if k not in types}
        out["raw_data"] = overflow or None
    # A projected source (raw=False) has no raw_data column at all: the
    # requirement there is that only the named columns reach the database.

    # raw_data IS part of the identity now that it is overflow-only: two rows
    # differing solely in a column discovery has not typed yet are genuinely
    # different rows, and collapsing them would lose data.
    hashable = {k: v for k, v in out.items() if k not in HASH_EXCLUDE}
    out["row_hash"] = row_hash(hashable, occurrence)
    return out


def build_rows(rows: Sequence[Dict[str, Any]], types: Dict[str, str],
               seen: Optional[Dict[str, int]] = None, raw: bool = True
               ) -> Tuple[List[Dict[str, Any]], int]:
    """Convert a batch to table rows, numbering repeated identical lines.

    Returns (rows, repeats) where `repeats` counts rows that were the 2nd or
    later identical copy within their file. Every row is kept - `repeats` is
    reported so an unexpectedly high count is visible, not silently dropped.

    Numbering also satisfies a hard constraint of the upsert: PostgREST sends a
    batch as one INSERT ... ON CONFLICT, and Postgres rejects the whole
    statement with "cannot affect row a second time" if the same conflict key
    appears twice in it. Distinct occurrences cannot collide.

    `seen` is per file, carried across that file's batches so a repeat split
    over a chunk boundary keeps counting from the right number. Rows repeated
    ACROSS files need no bookkeeping here: they hash identically and the upsert
    collapses them.
    """
    seen = {} if seen is None else seen
    out: List[Dict[str, Any]] = []
    repeats = 0
    for row in rows:
        probe = to_db_row(row, types, 0, raw)
        key = probe["row_hash"]
        n = seen.get(key, 0)
        seen[key] = n + 1
        if n:
            repeats += 1
        out.append(probe if n == 0 else to_db_row(row, types, n, raw))
    return out, repeats


# --------------------------------------------------------------------------- #
# Sink                                                                         #
# --------------------------------------------------------------------------- #

class SupabaseSink:
    def __init__(self, url: Optional[str] = None, key: Optional[str] = None,
                 log_table: str = LOG_TABLE):
        self.url = normalize_supabase_url(url or os.environ.get("SUPABASE_URL", ""))
        self.key = (key
                    or os.environ.get("SUPABASE_SERVICE_ROLE_KEY", "").strip()
                    or os.environ.get("SUPABASE_KEY", "").strip())
        self.log_table = os.environ.get("SUPABASE_LOG_TABLE", log_table)
        self._client = None

    def missing_config(self) -> List[str]:
        missing = []
        if not SUPABASE_AVAILABLE:
            missing.append("supabase package (pip install supabase)")
        if not self.url:
            missing.append("SUPABASE_URL")
        if not self.key:
            missing.append("SUPABASE_SERVICE_ROLE_KEY (or SUPABASE_KEY)")
        return missing

    @property
    def client(self):
        if self._client is None:
            missing = self.missing_config()
            if missing:
                raise RuntimeError("Supabase not configured: " + ", ".join(missing))
            self._client = create_client(self.url, self.key)
        return self._client

    # -- reads ------------------------------------------------------------- #

    def table_exists(self, table: str) -> bool:
        try:
            self.client.table(table).select("id", count="exact").limit(1).execute()
            return True
        except Exception as exc:  # noqa: BLE001
            logger.debug("[CHECK] %s not reachable: %s", table, exc)
            return False

    def count_rows(self, table: str) -> Optional[int]:
        try:
            r = self.client.table(table).select("id", count="exact").limit(1).execute()
            return r.count
        except Exception as exc:  # noqa: BLE001
            logger.error("[COUNT] %s: %s", table, exc)
            return None

    def existing_source_files(self, table: str, candidates: Sequence[str]) -> set:
        """Which of `candidates` already have rows in `table`?

        Queries only the candidates rather than scanning, so this stays cheap as
        the table grows past a few million rows.
        """
        if not candidates:
            return set()
        wanted = sorted({c.lower().strip() for c in candidates if c})
        found = set()
        try:
            for batch in chunked(wanted, 100):
                r = (self.client.table(table)
                     .select("source_file_lower")
                     .in_("source_file_lower", list(batch))
                     .execute())
                for rec in r.data or []:
                    v = (rec.get("source_file_lower") or "").strip()
                    if v:
                        found.add(v)
        except Exception as exc:  # noqa: BLE001
            logger.error("[SUPABASE] existing-file lookup on %s failed: %s", table, exc)
            logger.error("[SUPABASE] treating all files as new; the row_hash "
                         "upsert still prevents duplicates.")
            return set()
        return found

    # -- writes ------------------------------------------------------------- #

    def upsert(self, table: str, rows: List[Dict[str, Any]],
               batch_size: Optional[int] = None) -> int:
        """Upsert on row_hash. Returns rows written.

        returning='minimal' matters more than it looks. PostgREST defaults to
        'representation', which sends every upserted row back in full - so a
        batch of 500 rows across 30 columns is transferred twice and Postgres
        has to materialise the result set. Since the rows are only counted here,
        that response is pure waste, and dropping it took throughput from ~10
        rows/sec to a workable rate. At Zepto's ~5M rows the difference is days.
        """
        if not rows:
            return 0
        batch_size = batch_size or int(os.environ.get("MP_BATCH_SIZE", "1000"))
        written = 0
        for batch in chunked(rows, batch_size):
            for attempt in range(1, 4):
                try:
                    (self.client.table(table)
                     .upsert(list(batch), on_conflict="row_hash",
                             returning="minimal")
                     .execute())
                    written += len(batch)
                    break
                except Exception as exc:  # noqa: BLE001
                    if attempt == 3:
                        logger.error("[SUPABASE] %s: batch of %d failed after 3 "
                                     "attempts: %s", table, len(batch), exc)
                    else:
                        logger.warning("[SUPABASE] %s: attempt %d failed: %s",
                                       table, attempt, exc)
                        time.sleep(2 * attempt)
        return written

    def completed_file_ids(self, table: str) -> set:
        """Drive file ids whose rows are ALL in `table`.

        Preferred over matching on source_file, which cannot tell a finished
        file from one that died half way through its upserts.
        """
        done, start = set(), 0
        try:
            while True:
                # Ordered paging: an unordered range() can repeat or skip rows.
                r = (self.client.table(LOADED_TABLE)
                     .select("drive_file_id")
                     .eq("table_name", table)
                     .order("id")
                     .range(start, start + 999)
                     .execute())
                rows = r.data or []
                done.update(x["drive_file_id"] for x in rows)
                if len(rows) < 1000:
                    return done
                start += 1000
        except Exception as exc:  # noqa: BLE001
            logger.warning("[SUPABASE] %s unreadable (%s); falling back to "
                           "source_file matching", LOADED_TABLE, str(exc)[:80])
            return set()

    def mark_file_loaded(self, source_key: str, table: str, meta: dict,
                         rows: int) -> None:
        """Record a file as fully loaded. Called only after its last batch."""
        try:
            (self.client.table(LOADED_TABLE)
             .upsert({"source_key": source_key, "table_name": table,
                      "drive_file_id": meta["id"], "source_file": meta["name"],
                      "rows_written": rows},
                     on_conflict="table_name,drive_file_id",
                     returning="minimal")
             .execute())
        except Exception as exc:  # noqa: BLE001
            logger.warning("[SUPABASE] could not record %s in %s: %s",
                           meta["name"], LOADED_TABLE, str(exc)[:100])

    def forget_files(self, table: str) -> None:
        """Drop a table's ledger entries, so --reload really re-reads."""
        try:
            (self.client.table(LOADED_TABLE).delete()
             .eq("table_name", table).execute())
        except Exception as exc:  # noqa: BLE001
            logger.warning("[SUPABASE] could not clear %s for %s: %s",
                           LOADED_TABLE, table, str(exc)[:100])

    def verify_present(self, table: str, hashes: Sequence[str]) -> int:
        """How many of `hashes` are actually in the table?

        The upsert counts a batch as written the moment the HTTP call returns
        success. That is not the same as the rows being there: two Amazon files
        were reported as fully written, logged no error at all, and landed
        nothing - 1,906 rows that only surfaced during reconciliation days
        later. Probing a few hashes per file turns that silent class of failure
        into a loud one, cheaply, because row_hash is uniquely indexed.
        """
        if not hashes:
            return 0
        found = 0
        try:
            for batch in chunked(list(hashes), 200):
                r = (self.client.table(table)
                     .select("row_hash", count="exact")
                     .in_("row_hash", list(batch))
                     .execute())
                found += r.count if r.count is not None else len(r.data or [])
        except Exception as exc:  # noqa: BLE001
            logger.warning("[SUPABASE] verification query failed on %s: %s",
                           table, str(exc)[:100])
            return -1
        return found

    def purge_table(self, table: str) -> int:
        """Delete every row in `table`. Returns the count removed.

        Needed when row_hash semantics change: the old rows would not be
        overwritten by the upsert, they would simply linger alongside the new
        ones. PostgREST refuses an unfiltered delete, hence the always-true id
        predicate.
        """
        before = self.count_rows(table) or 0
        if not before:
            return 0
        try:
            self.client.table(table).delete().gte("id", 0).execute()
        except Exception as exc:  # noqa: BLE001
            logger.error("[SUPABASE] purge %s failed: %s", table, exc)
            return 0
        after = self.count_rows(table) or 0
        logger.info("[SUPABASE] purged %s: %d -> %d rows", table, before, after)
        return before - after

    def delete_by_source_file(self, table: str, source_file: str) -> None:
        """Remove one file's rows, for re-loading a file that was fixed upstream."""
        try:
            (self.client.table(table)
             .delete()
             .eq("source_file_lower", source_file.lower().strip())
             .execute())
        except Exception as exc:  # noqa: BLE001
            logger.error("[SUPABASE] delete %s from %s failed: %s",
                         source_file, table, exc)

    def log_run(self, source: str, workflow: str, started: datetime,
                ended: datetime, stats: Dict[str, Any]) -> None:
        record = {
            "source": source,
            "workflow": workflow,
            "started_at": started.isoformat(),
            "ended_at": ended.isoformat(),
            "duration_seconds": (ended - started).total_seconds(),
            "status": stats.get("status", "success"),
            "files_seen": stats.get("files_seen"),
            "files_loaded": stats.get("files_loaded"),
            "files_skipped": stats.get("files_skipped"),
            "rows_written": stats.get("rows_written"),
            "details": stats.get("details"),
        }
        try:
            self.client.table(self.log_table).insert(record).execute()
        except Exception as exc:  # noqa: BLE001
            logger.warning("[SUPABASE] could not write %s: %s", self.log_table, exc)
