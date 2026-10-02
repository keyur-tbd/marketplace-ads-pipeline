"""Sync the Google Sheets the P&L is keyed on, incrementally.

    python -m mp.sheets --check                  # read + parse, write nothing
    python -m mp.sheets --run                    # load every sheet that changed
    python -m mp.sheets --run --source pnl_feeder --force

Six sheets, all read as marketing@thebakersdozen.in (the same token.json as
the Drive sync):

  campaign_master  "Ecom Campaign Master" (owned by instamart@, edited daily by
                   the ecom team): each marketplace ads campaign -> the SKU or ALL
                   group it advertises. Keys the P&L's ads onto products
                   (Birbal migration 066). -> ref_ads_campaign, ref_ads_name_map
  pnl_feeder       the P&L "Feeder File": the below-gross-margin costs by month
                   (Birbal migration 045). -> six pnl_* tables (store salaries: staff_salary_loader, 132)
  party_master     the party sheet's "Customer Location & Route MASTER" tab:
                   ship-to name -> the party the business calls it (Amazon Fresh,
                   Reliance Signature, Ratnadeep, GT ...). Every Birbal board
                   names parties this way (Birbal migration 095).
                   -> ref_party_master
  prn_bb, prn_nb   the marketing team's Big Basket / Nature's Basket PRN sheets (tabs BB RTV
                   and "Nature's Basket Raw sheet"): PRN lines for the months before the
                   partners' daily PRN feeds began (Birbal migration 105, PRN tracking).
                   -> prn_marketing_lines
  capping_booked   finance's "ERP Capping RTV Data" (tabs FY26-27, FY25-26): each booked
                   capping-RTV credit memo and the sales month it settles (Birbal
                   migrations 036/044/075). -> rtv_capping_booked

Incremental, in two layers:

  1. A sheet whose Drive modifiedTime has not moved since its last good load is
     not read at all (public.sheet_sync_state).
  2. A changed sheet is read whole -- a sheet has no "rows since" -- but every
     row is UPSERTED on the target's own key and a row is written only when a
     value differs, so the counts reported are real inserts and real updates.

Nothing is deleted from the P&L sheets' tables: a campaign dropped from the
sheet keeps its mapping so its past spend keeps landing on its product. The
party tab and the capping sheet are the exceptions -- a ship-to taken off the tab
stops naming a party, a credit memo taken off the capping sheet stops being a
capping RTV (guarded: a read of fewer than 500 / 400 rows deletes nothing). Every tab is matched
on its HEADER NAMES, never on position; a tab whose headers changed aborts that sheet's load
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

import httplib2
from google.auth.transport.requests import Request
from google_auth_httplib2 import AuthorizedHttp
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
    # 180 s, not httplib2's 60: the party sheet takes 30-75 s to serve one tab, and
    # at 60 s every other read of it timed out
    sheets_http = AuthorizedHttp(creds, http=httplib2.Http(timeout=180))
    return (build("sheets", "v4", http=sheets_http, cache_discovery=False),
            build("drive", "v3", credentials=creds, cache_discovery=False))


def _modified_time(drive, sheet_id: str) -> dt.datetime:
    meta = drive.files().get(fileId=sheet_id, fields="modifiedTime",
                             supportsAllDrives=True).execute()
    return dt.datetime.fromisoformat(meta["modifiedTime"].replace("Z", "+00:00"))


def _grid(sheets, sheet_id: str, tab: str, cols: str = "AZ", rows: int = 20000):
    return sheets.spreadsheets().values().get(
        spreadsheetId=sheet_id, range=f"'{tab}'!A1:{cols}{rows}",
        valueRenderOption="UNFORMATTED_VALUE").execute(num_retries=2).get("values", [])


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
FEEDER_TEXT = {"city", "party"}

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
    # "Salaries Stores & promoters" is NOT synced here since Birbal migration 132 (2026-10-02): the
    # (month, party, salary name) key folded a person's stores into one row (April 7.68 L of 12.05 L),
    # and salaries are confidential. sheet_grn_loader/staff_salary_loader.py loads the store grain,
    # without names, from the Salary Detail sheet the Feeder tab copies.
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
        # dedupe on the target table's OWN key
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

# --------------------------------------------------------------------------- #
# The party master: "Customer Location & Route MASTER"                        #
# --------------------------------------------------------------------------- #

# The SKU POD Master Tracker (owned by the supply team). Its "Customer Location &
# Route MASTER" tab names the party every ship-to belongs to; Birbal shows these
# parties on every board (Birbal migration 094). The tracker is a monthly workbook,
# so its id can change: PARTY_MASTER_SHEET overrides the default without a deploy.
PARTY_MASTER_SHEET = os.environ.get("PARTY_MASTER_SHEET", "1q2XU3WufZuflL-n9hy5HyB0YmMptbtfGi3WCayBKGls")
PARTY_MASTER_TAB = ("Customer Location & Route MASTER", {
    "ship_to_name": ["name"], "party_raw": ["display name"], "channel": ["channel"],
    "city": ["city location"], "city_type": ["city type"]})


def load_party_master(sheets, cur, write: bool) -> Tuple[int, int, int, str]:
    tab, spec = PARTY_MASTER_TAB
    # A1:L6000, not the default A1:AZ20000: the tab is ~2k rows and its columns sit in A-E,
    # and the wide read took over a minute (one run timed out)
    grid = _grid(sheets, PARTY_MASTER_SHEET, tab, cols="L", rows=6000)
    if not grid:
        raise ValueError(f"tab {tab!r} is empty")
    idx = _header_index(tab, grid[0], spec, optional=("city", "city_type"))
    rows: Dict[str, tuple] = {}
    for r in grid[1:]:
        name = _text(_cell(r, idx["ship_to_name"]))
        party = _text(_cell(r, idx["party_raw"]))
        if not name or not party:
            continue
        key = re.sub(r"\s+", " ", name).strip().upper()
        # a ship-to listed twice keeps its last row (the tab has no conflicting pair today)
        rows[key] = (key, name, party, _text(_cell(r, idx["channel"])),
                     _text(_cell(r, idx["city"])) if "city" in idx else None,
                     _text(_cell(r, idx["city_type"])) if "city_type" in idx else None)
    if not write:
        return len(rows), 0, 0, f"{len(set(v[2] for v in rows.values()))} parties"
    ins, upd = merge(cur, "ref_party_master",
                     ["ship_to_key", "ship_to_name", "party_raw", "channel", "city", "city_type"],
                     ["ship_to_key"], list(rows.values()), "loaded_at")
    # a ship-to taken off the tab stops naming a party; guarded so a half-read tab
    # (a renamed header, an API hiccup) cannot empty the map
    gone = 0
    if len(rows) >= 500:
        cur.execute("delete from public.ref_party_master where not (ship_to_key = any(%s))",
                    [list(rows.keys())])
        gone = cur.rowcount
    return len(rows), ins, upd, f"{len(set(v[2] for v in rows.values()))} parties, {gone} dropped"


# --------------------------------------------------------------------------- #
# The marketing team's PRN sheets (Big Basket, Nature's Basket)               #
# --------------------------------------------------------------------------- #
# The PRN LINES behind the marketing team's two PRN trackers, for the months before the partners'
# daily PRN feeds (bb_net_prn from 2026-08-19, nb_prn from 2026-08-25) began. Birbal's SCM Tracker
# "PRN tracking" view (migration 105) reads the feeds first, these lines second.
#   Big Basket    "Big Basket 2027 PRN Summary", tab BB RTV: PRN x ship-to x SKU, built by the team
#                 from BB's PRN mails and portal exports ("Data from" = MAIL / PORTAL)
#   Nature's Basket "Nature's Basket PRN Data 2027", tab "Nature's Basket Raw sheet" (header on row
#                 2, a totals row above it): NB's SAP RTV lines -- Art. Doc. = the PRN document,
#                 ref no = the ORDER number our credit memos quote, negative Qty / LC Amt.
PRN_BB_SHEET = os.environ.get("PRN_BB_SHEET", "1rq6JYWnMkxvGuDUDH33srFxVbH6Sni9VaaWLukL_Pqc")
PRN_NB_SHEET = os.environ.get("PRN_NB_SHEET", "1i5_UuAeQUzziiaTPcMD8bAeLmds_CXSdDaTdkavhI7Q")
# Big Basket: the team's "PRN wise mapping" takes each PRN (ship-to code + PRN number) from the Mail tab
# if BB mailed it, else the Portal export, else the "Not Found in Mail & Portal" tab -- verified to rebuild
# its Total Unique Value on all 1,529 PRNs. The loader takes the SKU lines by the same rule.
PRN_BB_TABS = [
    ("MAIL", "Mail", 0, {"prn_no": ["gonno"], "ship_to_code": ["shiptocode"], "branch": ["shiptoname"],
                         "prn_date": ["invoicedate"], "article_code": ["skucode"], "description": ["skudesc"],
                         "quantity": ["quantity"], "value": ["totalvalue"], "warehouse": ["erp warehouse"],
                         "city": ["city"]}),
    ("PORTAL", "Portal", 1, {"prn_no": ["prn_number"], "ship_to_code": ["loccode"], "branch": ["locname"],
                             "prn_date": ["prndate"], "article_code": ["skucode"], "description": ["skudesc"],
                             "quantity": ["qty"], "value": ["totalval"], "warehouse": ["erp werehouse", "erp warehouse"],
                             "city": ["city"]}),
    ("NOT FOUND", "Not Found in Mail & Portal", 1, {"prn_no": ["prn_number"], "ship_to_code": ["loccode"], "branch": ["locname"],
                             "prn_date": ["prndate"], "article_code": ["skucode"], "description": ["skudesc"],
                             "quantity": ["qty"], "value": ["totalval"], "warehouse": ["erp werehouse", "erp warehouse"],
                             "city": ["city"]}),
]
PRN_NB_TAB = ("Nature's Basket Raw sheet", {"article_code": ["article"], "description": ["article description"],
                                             "tbd_item": ["tbd item name"], "category": ["category"],
                                             "city": ["city"], "branch": ["name 1"], "prn_no": ["art. doc."],
                                             "prn_date": ["doc. date"], "quantity": ["qty"], "value": ["lc amt."],
                                             "order_no": ["ref no"], "posting_status": ["posting status"],
                                             "sro_status": ["status"], "sro_created": ["sro created"]})
PRN_COLS = ["line_key", "platform", "prn_no", "order_no", "prn_date", "branch", "ship_to_code", "city",
            "article_code", "description", "tbd_item", "category", "quantity", "value", "data_from", "warehouse",
            "mkt_status"]


def _date(v) -> Optional[dt.date]:
    if isinstance(v, (int, float)) and 20000 < v < 80000:
        return EPOCH + dt.timedelta(days=int(v))
    if isinstance(v, str) and re.match(r"^\d{4}-\d{2}-\d{2}", v.strip()):
        return dt.date.fromisoformat(v.strip()[:10])
    return None


def _num(v) -> Optional[float]:
    if isinstance(v, (int, float)):
        return float(v)
    try:
        return float(str(v).replace(",", "").strip())
    except (TypeError, ValueError):
        return None


def _ident(v) -> Optional[str]:
    """PRN / order / article numbers arrive as numbers: keep them as whole-number text."""
    if isinstance(v, float) and v.is_integer():
        v = int(v)
    return _text(v)


def _mkt_status(posted, status, created) -> Optional[str]:
    """The marketing sheet's own verdict, in the tracker's words."""
    posted, status, created = (_text(x) or "" for x in (posted, status, created))
    if posted.lower() == "posted":
        return "Posted"
    if created.lower() == "no" or status.lower().startswith("sro creation"):
        return "SRO creation pending"
    for word in ("Open", "Pending Approval", "Released"):
        if status.lower() == word.lower():
            return word
    return status or None


def _prn_rows(platform: str, grid: List[list], header_row: int, tab: str, spec, data_from=None,
              status_of: Optional[Dict[str, str]] = None) -> Dict[str, list]:
    idx = _header_index(tab, grid[header_row], spec,
                        optional=("data_from", "ship_to_code", "tbd_item", "category", "city", "order_no", "warehouse",
                                  "posting_status", "sro_status", "sro_created"))
    rows: Dict[str, list] = {}
    for r in grid[header_row + 1:]:
        prn = _ident(_cell(r, idx["prn_no"]))
        when = _date(_cell(r, idx["prn_date"]))
        if not prn or not when or not re.match(r"^\d+$", prn):
            continue
        g = lambda k: _cell(r, idx[k]) if k in idx else None  # noqa: E731
        art = _ident(g("article_code"))
        branch = _text(g("branch"))
        key = "|".join([platform, prn, art or "", (branch or "").upper()])
        qty, val = abs(_num(g("quantity")) or 0.0), abs(_num(g("value")) or 0.0)
        if key in rows:                       # the same SKU twice on one PRN: one line, summed
            rows[key][12] += qty
            rows[key][13] += val
            continue
        wh = _text(g("warehouse"))
        rows[key] = [key, platform, prn, _ident(g("order_no")), when, branch, _text(g("ship_to_code")),
                     _text(g("city")), art, _text(g("description")), _text(g("tbd_item")), _text(g("category")),
                     qty, val, data_from or _text(g("data_from")),
                     wh if wh and re.match(r"^[A-Z]{3,}WH", wh) else None,
                     (status_of or {}).get((_text(g("ship_to_code")) or "").upper() + prn)
                     if status_of is not None else _mkt_status(g("posting_status"), g("sro_status"), g("sro_created"))]
    return rows


def _write_prn(cur, write: bool, platform: str, rows: Dict[str, list]):
    value = sum(r[13] for r in rows.values())
    detail = f"{len({r[2] for r in rows.values()})} PRNs, Rs {value / 1e5:.1f} L"
    if not write:
        return len(rows), 0, 0, detail
    ins, upd = merge(cur, "prn_marketing_lines", PRN_COLS, ["line_key"],
                     [tuple(r) for r in rows.values()], "loaded_at")
    gone = 0
    if len(rows) >= 500:                      # a half-read tab never empties the history
        cur.execute("delete from public.prn_marketing_lines where platform = %s and not (line_key = any(%s))",
                    [platform, list(rows.keys())])
        gone = cur.rowcount
    return len(rows), ins, upd, f"{detail}, {gone} dropped"


def load_prn_bb(sheets, cur, write: bool) -> Tuple[int, int, int, str]:
    # the team's own list of PRNs ("PRN wise mapping", CONCATENATE = ship-to code + PRN number): the
    # Portal export also carries PRNs the team does not track, which must not inflate the history
    mapping = _grid(sheets, PRN_BB_SHEET, "PRN wise mapping ", cols="W", rows=40000)
    head = [str(h).strip().lower() for h in mapping[1]] if len(mapping) > 1 else []
    i_status = head.index("status") if "status" in head else None
    i_posted = head.index("posted") if "posted" in head else None
    listed, status_of = set(), {}
    for r in mapping[2:]:
        if len(r) > 3 and _ident(r[3]):
            key = str(_ident(r[3])).upper()
            listed.add(key)
            status_of[key] = _mkt_status(_cell(r, i_posted) if i_posted is not None else None,
                                         _cell(r, i_status) if i_status is not None else None, None)
    if len(listed) < 500:
        raise ValueError(f"'PRN wise mapping' lists only {len(listed)} PRNs; refusing to load a partial history")
    taken: Dict[str, str] = {}                # ship-to code + PRN -> the tab it comes from
    out: Dict[str, list] = {}
    for label, tab, header_row, spec in PRN_BB_TABS:
        grid = _grid(sheets, PRN_BB_SHEET, tab, cols="AF", rows=40000)
        if len(grid) <= header_row:
            raise ValueError(f"tab {tab!r} is empty")
        rows = _prn_rows("Big Basket", grid, header_row, tab, spec, data_from=label, status_of=status_of)
        mine = {}
        for key, r in rows.items():
            prn_key = (r[6] or r[5] or "").upper() + "|" + r[2]
            if ((r[6] or "").upper() + r[2]) not in listed:
                continue
            if taken.get(prn_key, label) != label:
                continue                      # an earlier tab already gave this PRN
            mine[prn_key] = label
            out[key] = r
        taken.update(mine)
    return _write_prn(cur, write, "Big Basket", out)


def load_prn_nb(sheets, cur, write: bool) -> Tuple[int, int, int, str]:
    tab, spec = PRN_NB_TAB
    grid = _grid(sheets, PRN_NB_SHEET, tab, cols="AD", rows=40000)
    if len(grid) <= 1:
        raise ValueError(f"tab {tab!r} is empty")
    return _write_prn(cur, write, "Nature's Basket", _prn_rows("Nature's Basket", grid, 1, tab, spec, data_from="SAP RTV"))


# --------------------------------------------------------------------------- #
# ERP Capping RTV Data                                                        #
# --------------------------------------------------------------------------- #
# Finance's list of the booked capping-RTV credit memos and the SALES month each settles
# (Birbal migration 036; since 075 the sheet outranks the return reason code). A note listed
# here is EXP dated to that month, and a month x platform with a booked note carries no
# provision (044). public.o2c_refresh() rebuilds mv_rtv_capping_ledger at its 09:00/14:00 IST
# beat, so this only keeps the table current. Was a one-off manual load (9 Sep 2026) until
# 29 Sep 2026; capping_rtv_pipeline/load_cm_map.py in D:\Python\Birbal is the old loader.
CAPPING_SHEET = "1keFOUYTRjQ7DfBYbncPe216fgjYd1fbafCKQT5OiMqk"
CAPPING_TABS = ["FY26-27", "FY25-26"]            # a note on both tabs keeps the FY26-27 row
CAPPING_SPEC = {"platform_raw": ["capping rtv"], "customer_no": ["customer no."],
                "doc_no": ["document no."], "external_doc_no": ["external document no."],
                "taxable_amount": ["taxable amount"],
                "sales_month": ["month", "hitesh month"]}   # 'Hitesh MOnth' on FY25-26
# the sheet names the partner, the register the invoicing entity (Instamart bills through five)
CAPPING_PLATFORM = {"ZEPTO": "Zepto", "BLINKIT": "Blinkit", "SWIGGY": "Instamart",
                    "INSTAMART": "Instamart", "FLIPKART": "Flipkart"}
CAPPING_COLS = ["doc_no", "platform_raw", "platform", "customer_no", "external_doc_no",
                "sales_month", "taxable_amount", "source_tab"]
_MONTHS = {m: i + 1 for i, m in enumerate(
    ["jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"])}


def _sales_month(v) -> Optional[dt.date]:
    """FY26-27 holds real dates (serials); FY25-26 text like "Nov'24 Capping RTV"."""
    d = _date(v)
    if d:
        return d.replace(day=1)
    m = re.search(r"([A-Za-z]{3})[a-z]*[\s\-']*(\d{2,4})", _text(v) or "")
    if not m or m.group(1).lower() not in _MONTHS:
        return None
    yr = int(m.group(2))
    return dt.date(yr + 2000 if yr < 100 else yr, _MONTHS[m.group(1).lower()], 1)


def load_capping_booked(sheets, cur, write: bool) -> Tuple[int, int, int, str]:
    rows: Dict[str, tuple] = {}
    clashes = []
    for tab in CAPPING_TABS:
        # A-K only: the columns past K are scratch working
        grid = _grid(sheets, CAPPING_SHEET, tab, cols="K", rows=5000)
        if not grid:
            raise ValueError(f"tab {tab!r} is empty")
        idx = _header_index(tab, grid[0], CAPPING_SPEC)
        n = 0
        for r in grid[1:]:
            doc = _text(_cell(r, idx["doc_no"]))
            if not doc:
                continue
            plat = _text(_cell(r, idx["platform_raw"])) or ""
            month = _sales_month(_cell(r, idx["sales_month"]))
            if doc in rows:                  # one memo split over several rows: first row wins
                if rows[doc][5] != month:
                    clashes.append(f"{doc} {rows[doc][5]} vs {month}")
                continue
            rows[doc] = (doc, plat, CAPPING_PLATFORM.get(plat.upper()),
                         _ident(_cell(r, idx["customer_no"])),
                         _ident(_cell(r, idx["external_doc_no"])),
                         month, _num(_cell(r, idx["taxable_amount"])), tab)
            n += 1
        logger.info("  %-8s %4d credit memos", tab, n)
    unmapped = sorted({r[1] for r in rows.values() if not r[2]})
    if unmapped:
        # a new partner name would load with no platform and silently drop out of the ledger
        raise ValueError(f"'Capping RTV' values with no platform mapping: {unmapped}; "
                         f"add them to CAPPING_PLATFORM")
    for c in clashes[:10]:
        logger.warning("  credit memo given two sales months, first kept: %s", c)
    months = sorted({r[5] for r in rows.values() if r[5]})
    detail = (f"{len(rows)} notes, sales months {months[0]}..{months[-1]}, "
              f"{sum(1 for r in rows.values() if not r[5])} with no month")
    if not write:
        return len(rows), 0, 0, detail
    ins, upd = merge(cur, "rtv_capping_booked", CAPPING_COLS, ["doc_no"],
                     list(rows.values()), "loaded_at")
    # a note taken off the sheet is no longer a capping RTV; guarded so a half-read
    # sheet cannot empty the table
    gone = 0
    if len(rows) >= 400:
        cur.execute("delete from public.rtv_capping_booked where not (doc_no = any(%s))",
                    [list(rows.keys())])
        gone = cur.rowcount
    return len(rows), ins, upd, f"{detail}, {gone} dropped"


@dataclass
class SheetSource:
    key: str
    sheet_id: str
    load: Callable


SOURCES = [
    SheetSource("campaign_master", CAMPAIGN_SHEET, load_campaign_master),
    SheetSource("pnl_feeder", FEEDER_SHEET, load_feeder),
    SheetSource("party_master", PARTY_MASTER_SHEET, load_party_master),
    SheetSource("prn_bb", PRN_BB_SHEET, load_prn_bb),
    SheetSource("prn_nb", PRN_NB_SHEET, load_prn_nb),
    SheetSource("capping_booked", CAPPING_SHEET, load_capping_booked),
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
