"""Sync the Google Sheets the P&L is keyed on, incrementally.

    python -m mp.sheets --check                  # read + parse, write nothing
    python -m mp.sheets --run                    # load every sheet that changed
    python -m mp.sheets --run --source pnl_feeder --force

Two sheets, both read as marketing@thebakersdozen.in (the same token.json as
the Drive sync):

  campaign_master  "Ecom Campaign Master" (owned by instamart@, edited daily by
                   the ecom team): each marketplace ads campaign -> the SKU or ALL
                   group it advertises. Keys the P&L's ads onto products
                   (Birbal migration 066). -> ref_ads_campaign, ref_ads_name_map
  pnl_feeder       the P&L "Feeder File": the below-gross-margin costs by month
                   (Birbal migration 045). -> the seven pnl_* tables

Incremental, in two layers:

  1. A sheet whose Drive modifiedTime has not moved since its last good load is
     not read at all (public.sheet_sync_state).
  2. A changed sheet is read whole -- a sheet has no "rows since" -- but every
     row is UPSERTED on the target's own key and a row is written only when a
     value differs, so the counts reported are real inserts and real updates.

Nothing is ever deleted: a campaign dropped from the sheet keeps its mapping so
its past spend keeps landing on its product. Every tab is matched on its HEADER
NAMES, never on position; a tab whose headers changed aborts that sheet's load
(the others still run) rather than writing shifted columns.

The P&L picks the new rows up at the next public.o2c_refresh() beat (09:00 and
18:00 IST), which the workflow is timed to precede.
"""
from __future__ import annotations

import argparse
import datetime as dt
import logging
import os
import re
import sys
from dataclasses import dataclass
from typing import Callable, Dict, List, Optional, Sequence, Tuple

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build

from . import dbadmin
from .drive import PROJECT, _token_path
from .sink import load_dotenv

logger = logging.getLogger("mp.sheets")

EPOCH = dt.date(1899, 12, 30)          # Sheets serial-date origin
CHUNK = 400                            # rows per staging INSERT


# --------------------------------------------------------------------------- #
# Google                                                                      #
# --------------------------------------------------------------------------- #

def _clients():
    path = _token_path()
    if not os.path.exists(path):
        raise SystemExit(f"No Google token at {path} (set GOOGLE_TOKEN_FILE).")
    creds = Credentials.from_authorized_user_file(path)
    if not creds.valid:
        creds.refresh(Request())
        if os.path.dirname(os.path.abspath(path)) == PROJECT:
            with open(path, "w", encoding="utf-8") as fh:
                fh.write(creds.to_json())
    return (build("sheets", "v4", credentials=creds, cache_discovery=False),
            build("drive", "v3", credentials=creds, cache_discovery=False))


def _modified_time(drive, sheet_id: str) -> dt.datetime:
    meta = drive.files().get(fileId=sheet_id, fields="modifiedTime",
                             supportsAllDrives=True).execute()
    return dt.datetime.fromisoformat(meta["modifiedTime"].replace("Z", "+00:00"))


def _grid(sheets, sheet_id: str, tab: str, cols: str = "AZ", rows: int = 20000):
    return sheets.spreadsheets().values().get(
        spreadsheetId=sheet_id, range=f"'{tab}'!A1:{cols}{rows}",
        valueRenderOption="UNFORMATTED_VALUE").execute().get("values", [])


def _header_index(tab: str, header: Sequence, spec: Dict[str, Sequence[str]],
                  optional: Sequence[str] = ()) -> Dict[str, int]:
    """{db column: position}, from the accepted header names (lower case)."""
    hdr = [str(h).strip().lower() for h in header]
    idx, missing = {}, []
    for col, names in spec.items():
        hit = next((hdr.index(n) for n in names if n in hdr), None)
        if hit is not None:
            idx[col] = hit
        elif col not in optional:
            missing.append(f"{col} ({'/'.join(names)})")
    if missing:
        raise ValueError(
            f"tab {tab!r}: headers changed, cannot find {', '.join(missing)} "
            f"(saw: {', '.join(hdr[:14])}). Refusing to load rather than write "
            f"shifted columns.")
    return idx


def _cell(row: Sequence, i: int):
    return row[i] if i < len(row) else None


# --------------------------------------------------------------------------- #
# Postgres: stage, then upsert only what changed                              #
# --------------------------------------------------------------------------- #

def merge(cur, table: str, cols: List[str], keys: List[str], rows: List[tuple],
          touch: str) -> Tuple[int, int]:
    """Upsert rows into public.<table>; returns (inserted, updated).

    `touch` is the timestamp column set to now() on an UPDATE; an INSERT takes
    the table's default. A row identical to what is stored is not written.
    """
    if not rows:
        return 0, 0
    collist = ", ".join(cols)
    cur.execute(f"drop table if exists _stg")
    cur.execute(f"create temp table _stg as select {collist} from public.{table} limit 0")
    one = "(" + ", ".join(["%s"] * len(cols)) + ")"
    for i in range(0, len(rows), CHUNK):
        part = rows[i:i + CHUNK]
        cur.execute(f"insert into _stg ({collist}) values " + ", ".join([one] * len(part)),
                    [v for r in part for v in r])
    vals = [c for c in cols if c not in keys]
    cur.execute(
        f"insert into public.{table} as t ({collist}) select {collist} from _stg "
        f"on conflict ({', '.join(keys)}) do update set "
        + ", ".join(f"{c} = excluded.{c}" for c in vals) + f", {touch} = now() "
        f"where ({', '.join('t.' + c for c in vals)}) is distinct from "
        f"({', '.join('excluded.' + c for c in vals)}) "
        f"returning (xmax = 0)")
    flags = [r[0] for r in cur.fetchall()]
    cur.execute("drop table _stg")
    return sum(1 for f in flags if f), sum(1 for f in flags if not f)


# --------------------------------------------------------------------------- #
# The Ecom Campaign Master                                                    #
# --------------------------------------------------------------------------- #

CAMPAIGN_SHEET = "1oDQXCOiV83rRN4CZReo6U4sj0XuRXUXFtgmIkDA94cI"

# tab -> (P&L platform, {db column -> accepted headers}). campaign_id is only on
# Instamart's tab, and only Instamart's raw export carries it too.
CAMPAIGN_TABS = [
    ("Blinkit Campaign Master", "Blinkit", {
        "campaign_name": ["campaign name"], "sku_name": ["sku name"],
        "category": ["category"], "city": ["city"], "ad_type": ["ad types", "ad type"]}),
    ("Instamart Campaign Master", "Instamart", {
        "campaign_id": ["campaign id"], "campaign_name": ["campaign"],
        "sku_name": ["daily sales name"], "category": ["category"], "city": ["city"],
        "ad_type": ["type"]}),
    ("Zepto Campaign Master", "Zepto", {
        "campaign_name": ["campaign name"], "sku_name": ["daily sales name"],
        "category": ["category"], "city": ["city"], "ad_type": ["ad type"]}),
    ("Amazon Campaign Master", "Amazon", {
        "campaign_name": ["campaigns"], "sku_name": ["sku"], "category": ["category"],
        "city": ["city"], "ad_type": ["ad type"]}),
    # the raw fk_* exports are Flipkart Minutes, which the P&L reports as FLIPKART QUICK
    ("Flipkart Campaign Master", "Flipkart Quick", {
        "campaign_name": ["campaign name"], "sku_name": ["sku"], "category": ["category"],
        "city": ["city"], "ad_type": ["ad type"]}),
    ("BB Campaign Master", "Big Basket", {
        "campaign_name": ["campaign name"], "sku_name": ["sku name"],
        "category": ["category"], "city": ["city"], "ad_type": ["ad type"]}),
]
NAME_TAB = ("Master", {"ads_name": ["daily sales name"], "f_sku_name": ["f sku name"]})


def campaign_key(name: str) -> str:
    """Must match mv_ads_campaign_month.campaign_key (Birbal migration 066)."""
    return re.sub(r"\s+", " ", name.strip()).lower()


def product_key(name: str) -> str:
    """Python twin of public.ads_product_key()."""
    s = re.sub(r"^\s*O\s+", "", name or "", flags=re.I)
    return re.sub(r"[^A-Z0-9]", "", s.upper())


def _text(v) -> Optional[str]:
    if v is None:
        return None
    s = str(v).strip()
    return None if s in ("", "-", "#N/A", "#REF!", "#VALUE!") else s


def load_campaign_master(sheets, cur, write: bool) -> Tuple[int, int, int, str]:
    campaigns: Dict[tuple, dict] = {}
    clashes = []
    for tab, platform, spec in CAMPAIGN_TABS:
        grid = _grid(sheets, CAMPAIGN_SHEET, tab, "Z")
        if not grid:
            raise ValueError(f"tab {tab!r} is empty")
        idx = _header_index(tab, grid[0], spec, optional=("campaign_id",))
        n = 0
        for row in grid[1:]:
            rec = {c: _text(_cell(row, i)) for c, i in idx.items()}
            if not rec.get("campaign_name"):
                continue
            key = (platform, campaign_key(rec["campaign_name"]), rec.get("campaign_id") or "")
            prev = campaigns.get(key)
            if prev is not None:
                # the sheet lists some campaigns twice; the first row wins
                if prev["sku_name"] != rec.get("sku_name"):
                    clashes.append(f"{tab}: {rec['campaign_name']!r} "
                                   f"{prev['sku_name']} | {rec.get('sku_name')}")
                continue
            campaigns[key] = dict(rec, source_tab=tab)
            n += 1
        logger.info("  %-28s -> %-15s %5d campaigns", tab, platform, n)
    for c in clashes[:10]:
        logger.warning("  duplicate campaign, first row kept: %s", c)

    grid = _grid(sheets, CAMPAIGN_SHEET, NAME_TAB[0], "Z")
    idx = _header_index(NAME_TAB[0], grid[0], NAME_TAB[1])
    names: Dict[str, tuple] = {}
    for row in grid[1:]:
        a, f = _text(_cell(row, idx["ads_name"])), _text(_cell(row, idx["f_sku_name"]))
        if a and f:
            names.setdefault(product_key(a), (product_key(a), a, f))
    logger.info("  %-28s -> %d name mappings", NAME_TAB[0], len(names))

    read = len(campaigns) + len(names)
    if not write:
        return read, 0, 0, f"{len(clashes)} duplicate campaign names"
    ci, cu = merge(cur, "ref_ads_campaign",
                   ["platform", "campaign_key", "campaign_id", "campaign_name", "sku_name",
                    "category", "city", "ad_type", "source_tab"],
                   ["platform", "campaign_key", "campaign_id"],
                   [(k[0], k[1], k[2], c["campaign_name"], c.get("sku_name"), c.get("category"),
                     c.get("city"), c.get("ad_type"), c["source_tab"])
                    for k, c in campaigns.items()],
                   touch="updated_at")
    ni, nu = merge(cur, "ref_ads_name_map", ["name_key", "ads_name", "f_sku_name"],
                   ["name_key"], list(names.values()), touch="updated_at")
    return (read, ci + ni, cu + nu,
            f"campaigns +{ci} ~{cu}, names +{ni} ~{nu}, {len(clashes)} duplicate names")


# --------------------------------------------------------------------------- #
# The P&L Feeder File                                                         #
# --------------------------------------------------------------------------- #

FEEDER_SHEET = "1NScaI-YVRVJTvsSo01dd60GiRlk5B9v6_Vf4qSSWjSY"
FEEDER_TEXT = {"city", "party", "party_name", "erp_name", "promoter_tsi", "designation",
               "salary_name", "firm"}

# tab -> (target table, key columns, {sheet header (lowercased) -> db column})
FEEDER_TABS = [
    ("Corporate Overheads", "pnl_corporate_overhead", ["month"], {
        "month": "month", "provision for gratuity & pl": "gratuity_provision",
        "consultancy & legal fees": "consultancy_legal",
        "travelling & conveyance": "travel_conveyance",
        "gst": "gst", "software exp": "software", "others": "others"}),
    ("Logistics Cost", "pnl_logistics_cost", ["month", "city"], {
        "month": "month", "city": "city", "inter": "inter", "intra": "intra"}),
    ("Channel Spends", "pnl_channel_spend", ["month", "party"], {
        "month": "month", "party": "party", "offers": "offers",
        "ads": "ads", "other": "other"}),
    ("Brand Building costs & Salary", "pnl_brand_building", ["month"], {
        "month": "month", "brand building costs": "brand_building",
        "salary - corporate": "salary_corporate"}),
    ("Salary Direct", "pnl_salary_direct", ["month"], {
        "month": "month", "marketplace": "marketplace", "trade": "trade"}),
    ("Rent and Utilities", "pnl_rent_utilities", ["month", "city"], {
        "month": "month", "city": "city", "amount": "amount"}),
    # 'Month' is the ELEVENTH column of this tab, past the ten a first read samples.
    # Leaving it out of the key folded five months into one (1,692 rows -> 124).
    ("Salaries Stores & promoters", "pnl_store_salaries",
     ["month", "party_name", "salary_name"], {
        "month": "month",
        "party name": "party_name", "erp name": "erp_name", "city": "city",
        "promoter / tsi": "promoter_tsi", "designation": "designation",
        "salary name": "salary_name", "firm": "firm",
        "adj gross billing": "adj_gross_billing", "salary": "salary",
        "salary split": "salary_split"}),
]


def _feeder_tab(sheets, tab, keys, colmap):
    grid = _grid(sheets, FEEDER_SHEET, tab, "AZ", 2000)
    if not grid:
        raise ValueError(f"tab {tab!r} is empty")
    hdr = [str(h).strip().lower() for h in grid[0]]
    idx = {}
    for i, h in enumerate(hdr):
        if h in colmap and colmap[h] not in idx:
            idx[colmap[h]] = i
    missing = set(colmap.values()) - set(idx)
    if missing:
        raise ValueError(
            f"tab {tab!r}: headers changed, cannot find {', '.join(sorted(missing))} "
            f"(saw: {', '.join(hdr[:12])}). Refusing to load rather than write "
            f"shifted columns.")
    cols = list(idx)
    out = {}
    for row in grid[1:]:
        rec = {c: _cell(row, idx[c]) for c in cols}
        if rec.get("month") in (None, ""):
            continue
        if not isinstance(rec["month"], (int, float)):
            raise ValueError(f"tab {tab!r}: month {rec['month']!r} is not a serial number")
        rec["month"] = EPOCH + dt.timedelta(days=int(rec["month"]))
        for k, v in list(rec.items()):
            if v == "":
                rec[k] = None
            elif k in FEEDER_TEXT:
                rec[k] = None if v is None else str(v).strip()
            elif isinstance(v, str):
                rec[k] = None                  # a stray label in a numeric cell
        # dedupe on the target table's OWN key: the store-salary tab repeats a
        # (party, salary name) pair across cities
        key = tuple(rec.get(k) for k in keys)
        if any(v is None for v in key):
            continue
        out[key] = rec
    return cols, list(out.values())


def load_feeder(sheets, cur, write: bool) -> Tuple[int, int, int, str]:
    parsed = []
    for tab, table, keys, colmap in FEEDER_TABS:
        cols, rows = _feeder_tab(sheets, tab, keys, colmap)
        months = sorted({r["month"] for r in rows})
        span = f"{months[0]}..{months[-1]}" if months else "empty"
        logger.info("  %-32s -> %-24s %4d rows  %s", tab, table, len(rows), span)
        parsed.append((table, keys, cols, rows))
    read = sum(len(p[3]) for p in parsed)
    if not write:
        return read, 0, 0, ""
    ins = upd = 0
    detail = []
    for table, keys, cols, rows in parsed:
        i, u = merge(cur, table, cols, keys, [tuple(r[c] for c in cols) for r in rows],
                     touch="loaded_at")
        ins, upd = ins + i, upd + u
        if i or u:
            detail.append(f"{table} +{i} ~{u}")
    return read, ins, upd, ", ".join(detail) or "no row changed"


# --------------------------------------------------------------------------- #
# Driver                                                                      #
# --------------------------------------------------------------------------- #

@dataclass
class SheetSource:
    key: str
    sheet_id: str
    load: Callable


SOURCES = [
    SheetSource("campaign_master", CAMPAIGN_SHEET, load_campaign_master),
    SheetSource("pnl_feeder", FEEDER_SHEET, load_feeder),
]


def run(keys: Sequence[str], write: bool, force: bool) -> int:
    sheets, drive = _clients()
    conn = dbadmin.connect(timeout=300)
    cur = conn.cursor()
    cur.execute("set statement_timeout = '600000'")
    failures = 0
    for src in SOURCES:
        if keys and src.key not in keys:
            continue
        try:
            mtime = _modified_time(drive, src.sheet_id)
            cur.execute("select modified_time from public.sheet_sync_state where source_key = %s",
                        [src.key])
            row = cur.fetchone()
            if write and not force and row and row[0] is not None and row[0] >= mtime:
                logger.info("%s: unchanged since %s, skipped", src.key, row[0])
                continue
            logger.info("%s: sheet modified %s, reading", src.key, mtime)
            read, ins, upd, detail = src.load(sheets, cur, write)
            if not write:
                logger.info("%s: parsed %d rows (check only, nothing written) %s",
                            src.key, read, detail)
                conn.rollback()
                continue
            cur.execute("""
                insert into public.sheet_sync_state
                    (source_key, sheet_id, modified_time, synced_at, rows_read,
                     rows_inserted, rows_updated, detail)
                values (%s, %s, %s, now(), %s, %s, %s, %s)
                on conflict (source_key) do update set
                    sheet_id = excluded.sheet_id, modified_time = excluded.modified_time,
                    synced_at = now(), rows_read = excluded.rows_read,
                    rows_inserted = excluded.rows_inserted,
                    rows_updated = excluded.rows_updated, detail = excluded.detail""",
                        [src.key, src.sheet_id, mtime, read, ins, upd, detail])
            conn.commit()
            logger.info("%s: read %d, inserted %d, updated %d (%s)",
                        src.key, read, ins, upd, detail)
        except Exception as exc:                 # one bad sheet must not stop the other
            conn.rollback()
            failures += 1
            logger.error("%s: FAILED, nothing written: %s", src.key, exc)
    conn.close()
    return failures


def main(argv: Optional[Sequence[str]] = None) -> int:
    load_dotenv(os.path.join(PROJECT, ".env"))     # real environment variables win
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    mode = ap.add_mutually_exclusive_group(required=True)
    mode.add_argument("--check", action="store_true", help="read and parse, write nothing")
    mode.add_argument("--run", action="store_true", help="load every sheet that changed")
    ap.add_argument("--source", action="append", default=[],
                    choices=[s.key for s in SOURCES], help="limit to one sheet (repeatable)")
    ap.add_argument("--force", action="store_true",
                    help="read even if the sheet has not changed since the last load")
    a = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    return 1 if run(a.source, write=a.run, force=a.force) else 0


if __name__ == "__main__":
    sys.exit(main())
