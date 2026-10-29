"""Sync the Google Sheets the P&L is keyed on, incrementally.

    python -m mp.sheets --check                  # read + parse, write nothing
    python -m mp.sheets --run                    # load every sheet that changed
    python -m mp.sheets --run --source pnl_feeder --force

The sheets below, all read as marketing@thebakersdozen.in (the same token.json as
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
  mis_costs        the same Management MIS FY27: tabs City X Channel, Marketplace / Trade Party
                   Break-up PnL and P&L FY27 Statement with Reco -> the six pnl_* cost tables for
                   EVERY month finance has closed there (source = 'management_mis', Birbal 224). The
                   MIS is finance's final word and runs ahead of the Feeder, so it replaces the
                   Feeder's rows for those months and pnl_feeder no longer writes them. The
                   statement's "-Yield Loss & WH Loss" goes to mis_yield_month (Birbal 233), which
                   the MIS spreads over products.
  mis_party_pnl    finance's Management MIS FY27, tab "Marketplace Party Break-up PnL":
                   the month x party P&L lines, GMV to EBITDA. The Power BI marketplace P&L
                   takes a month's costs from here while the Feeder File has not reached
                   it (finance filled September 2026 here and not in the Feeder).
                   -> powerbi.mp_mis_party_pnl (outside Birbal's migrations, created by
                   powerbi_mp_pnl.sql in "Marketplace P&L Data to PowerBi")

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
          touch: str, schema: str = "public") -> Tuple[int, int]:
    """Upsert rows into <schema>.<table>; returns (inserted, updated).

    `touch` is the timestamp column set to now() on an UPDATE; an INSERT takes
    the table's default. A row identical to what is stored is not written.
    """
    if not rows:
        return 0, 0
    collist = ", ".join(cols)
    cur.execute(f"drop table if exists _stg")
    cur.execute(f"create temp table _stg as select {collist} from {schema}.{table} limit 0")
    one = "(" + ", ".join(["%s"] * len(cols)) + ")"
    for i in range(0, len(rows), CHUNK):
        part = rows[i:i + CHUNK]
        cur.execute(f"insert into _stg ({collist}) values " + ", ".join([one] * len(part)),
                    [v for r in part for v in r])
    vals = [c for c in cols if c not in keys]
    cur.execute(
        f"insert into {schema}.{table} as t ({collist}) select {collist} from _stg "
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
        # a month the Management MIS has closed is its own (mis_costs, Birbal 224): never overwrite it
        cur.execute(f"select distinct month from public.{table} where source = 'management_mis'")
        owned = {r[0] for r in cur.fetchall()}
        rows = [r for r in rows if r["month"] not in owned]
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


# --------------------------------------------------------------------------- #
# Platform FOC and bills (Birbal migration 139)                               #
# --------------------------------------------------------------------------- #

# "Platform Wise FOC Amount" (owner harshil@): one tab per platform, month x bucket rows.
# FOC is the platform's free-of-cost ad credit; the bill is what the brand actually paid.
FOC_SHEET = os.environ.get("FOC_SHEET", "1RUoht5XkElx2Em3_OqswQplhBPh8NEZC1G-_5eWLtdo")
FOC_SPEC = {"platform_raw": ["platform"], "month": ["month"], "bucket_raw": ["category"],
            "foc_amount": ["foc amount"], "bill_amount": ["actual bill amount"],
            "wallet_recharge": ["actual wallet recharge"]}
FOC_PLATFORM = {"INSTAMART": "Instamart", "SWIGGY INSTAMART": "Instamart", "ZEPTO": "Zepto",
                "BLINKIT": "Blinkit", "AMAZON": "Amazon", "FLIPKART": "Flipkart",
                "BIG BASKET": "Big Basket", "BIGBASKET": "Big Basket"}
# The sheet's three buckets. "BREADS" and "BREAD" are the same bucket (Apr-Jul vs Aug on).
FOC_BUCKET = {"BREAD": "BREAD", "BREADS": "BREAD", "CHIPS": "CHIPS",
              "NON BREAD": "NON BREADS", "NON BREADS": "NON BREADS", "NON-BREADS": "NON BREADS"}
FOC_COLS = ["platform", "month", "bucket", "foc_amount", "bill_amount", "wallet_recharge", "source_tab"]


def load_platform_foc(sheets, cur, write: bool) -> Tuple[int, int, int, str]:
    meta = sheets.spreadsheets().get(spreadsheetId=FOC_SHEET, fields="sheets.properties.title").execute()
    rows: Dict[tuple, tuple] = {}
    for tab in [s["properties"]["title"] for s in meta["sheets"]]:
        grid = _grid(sheets, FOC_SHEET, tab, cols="F", rows=500)
        if not grid:
            continue
        idx = _header_index(tab, grid[0], FOC_SPEC, optional=["wallet_recharge"])
        for r in grid[1:]:
            plat_raw = (_text(_cell(r, idx["platform_raw"])) or "").upper()
            if not plat_raw:
                continue
            platform = FOC_PLATFORM.get(plat_raw)
            bucket = FOC_BUCKET.get((_text(_cell(r, idx["bucket_raw"])) or "").upper())
            month = _sales_month(_cell(r, idx["month"]))
            if not platform or not bucket or not month:
                # a new partner or bucket name would otherwise drop out of the MIS silently
                raise ValueError(f"tab {tab!r}: cannot read row {r[:5]} "
                                 f"(platform {platform}, bucket {bucket}, month {month})")
            key = (platform, month, bucket)
            if key in rows:
                raise ValueError(f"tab {tab!r}: {key} appears twice")
            # blank is NOT zero: no bill yet means "use the ads export less FOC"
            wallet = _num(_cell(r, idx["wallet_recharge"])) if "wallet_recharge" in idx else None
            rows[key] = (platform, month, bucket, _num(_cell(r, idx["foc_amount"])),
                         _num(_cell(r, idx["bill_amount"])), wallet, tab)
    months = sorted({k[1] for k in rows})
    detail = (f"{len(rows)} platform x month x bucket rows, {months[0]}..{months[-1]}, "
              f"{sum(1 for v in rows.values() if v[4] is not None)} with a bill") if rows else "empty"
    if not write:
        return len(rows), 0, 0, detail
    ins, upd = merge(cur, "mp_platform_foc", FOC_COLS, ["platform", "month", "bucket"],
                     list(rows.values()), "loaded_at")
    gone = 0
    if len(rows) >= 10:          # a half-read sheet must not empty the table
        cur.execute("""delete from public.mp_platform_foc
                       where (platform, month, bucket) not in (select * from unnest(%s::text[], %s::date[], %s::text[]))""",
                    [[k[0] for k in rows], [k[1] for k in rows], [k[2] for k in rows]])
        gone = cur.rowcount
    return len(rows), ins, upd, f"{detail}, {gone} dropped"


# --------------------------------------------------------------------------- #
# Management MIS FY27: "Marketplace Party Break-up PnL"                       #
# --------------------------------------------------------------------------- #

# Finance's own month x party P&L. Row 3 opens each month block with the month (a serial) and names the
# block's parties in the columns after it; column A labels the lines. Matched on the LABELS, never on row
# numbers; the first occurrence above EBITDA wins ("Others" is the channel-spend line, the corporate
# overheads' "Others" comes after it).
MIS_SHEET = os.environ.get("MIS_SHEET", "1qT4atLhT0YRRtT6S-WsMmHspOiVnn22DBPkEoYUUwL4")
MIS_TAB = "Marketplace Party Break-up PnL"
MIS_HDR_ROW = 2                                # 0-indexed: row 3
MIS_HEADS = {
    "gmv": "gmv", "gross billing": "gross_billing", "adjusted gross billing": "adjusted_gross_billing",
    "net sales": "net_sales", "cogs": "cogs", "material expenses": "material",
    "manufacturing expenses": "manufacturing", "yield / wh loss": "yield_loss",
    "gross margin": "gross_margin", "logistics cost": "logistics", "cm 1": "cm1",
    "other marketing exp": "other_marketing_exp", "offers": "offers", "ads": "ads",
    "others": "off_invoice", "salaries stores & promoters": "store_salaries",
    "store operating costs": "store_operating_cost", "cm 2": "cm2",
    "brand building cost": "brand_building", "cm 3": "cm3", "salary - corporate": "salary_corporate",
    "salary - direct corporate": "salary_direct", "rent and utilities": "rent_utilities",
    "corporate overheads": "corporate_overheads", "ebitda": "ebitda"}
MIS_PARTY = {"blinkit": "Blinkit", "instamart": "Instamart", "zepto": "Zepto", "big basket": "Big Basket",
             "amazon fresh": "Amazon Fresh", "aripl": "Aripl", "milk basket": "Milk Basket",
             "flipkart supermart": "Flipkart Supermart", "flipkart quick": "Flipkart Quick",
             "first club": "First Club", "dmart ready": "DMart Ready"}
MIS_COLS = ["month", "party", "head", "amount"]


def load_mis_party_pnl(sheets, cur, write: bool) -> Tuple[int, int, int, str]:
    grid = _grid(sheets, MIS_SHEET, MIS_TAB, cols="HZ", rows=120)
    if len(grid) <= MIS_HDR_ROW:
        raise ValueError(f"tab {MIS_TAB!r} is empty")
    labels: Dict[str, int] = {}
    for r, row in enumerate(grid):
        lab = str(_cell(row, 0) or "").strip().lower()
        if lab in MIS_HEADS and MIS_HEADS[lab] not in labels:
            labels[MIS_HEADS[lab]] = r
        if lab == "ebitda":
            break
    missing = set(MIS_HEADS.values()) - set(labels)
    if missing:
        raise ValueError(f"tab {MIS_TAB!r}: line labels changed, cannot find {', '.join(sorted(missing))}. "
                         f"Refusing to load rather than read the wrong rows.")
    hdr = grid[MIS_HDR_ROW]
    rows: Dict[tuple, tuple] = {}
    for i, v in enumerate(hdr):
        if not isinstance(v, (int, float)) or isinstance(v, bool):
            continue
        month = EPOCH + dt.timedelta(days=int(v))
        cols = []
        j = i + 1
        while j < len(hdr) and isinstance(hdr[j], str) and hdr[j].strip():
            name = hdr[j].strip()
            party = MIS_PARTY.get(name.lower())
            if not party:
                raise ValueError(f"tab {MIS_TAB!r}: unknown party {name!r} in the {month} block")
            cols.append((j, party))
            j += 1
        block = {}
        for head, r in labels.items():
            for j, party in cols:
                amt = _num(_cell(grid[r], j))
                if amt is not None:
                    block[(month, party, head)] = (month, party, head, round(amt, 2))
        # the tab carries template blocks for months finance has not filled (all zero); skip them
        if not any(abs(t[3]) > 0 for k, t in block.items() if k[2] == "net_sales"):
            continue
        if any(k[0] == month for k in rows):
            raise ValueError(f"tab {MIS_TAB!r}: two filled blocks for {month}")
        rows.update(block)
    months = sorted({k[0] for k in rows})
    detail = f"{len(rows)} month x party x head rows, {months[0]}..{months[-1]}" if rows else "empty"
    if not write:
        return len(rows), 0, 0, detail
    ins, upd = merge(cur, "mp_mis_party_pnl", MIS_COLS, ["month", "party", "head"],
                     list(rows.values()), "loaded_at", schema="powerbi")
    gone = 0
    if len(rows) >= 200:         # a half-read sheet must not empty the table
        cur.execute("""delete from powerbi.mp_mis_party_pnl
                       where (month, party, head) not in (select * from unnest(%s::date[], %s::text[], %s::text[]))""",
                    [[k[0] for k in rows], [k[1] for k in rows], [k[2] for k in rows]])
        gone = cur.rowcount
    return len(rows), ins, upd, f"{detail}, {gone} dropped"


# --------------------------------------------------------------------------- #
# Management MIS FY27 -> the six pnl_* cost tables (Birbal migration 224)     #
# --------------------------------------------------------------------------- #
# Finance closes a month in the Management MIS; the Feeder File lags it (September 2026 was never put in the
# Feeder) and is restated by it. So for every month the MIS has filled, these rows REPLACE the Feeder's (source
# = 'management_mis'), and load_feeder leaves that month alone. Four tabs, matched on their line LABELS (column A,
# column B on the statement), never on row numbers:
#   City X Channel                  a block of columns per month (its header serial carries the month, but a 2024
#                                   year: the month NUMBER is taken and the year comes from the statement); in a
#                                   block, one column per city (its total) and one per city x channel (that
#                                   split of direct salary is a sales-weight spread, NOT finance's marketplace /
#                                   trade split -- which the party tabs carry, so it is taken from there)
#   Marketplace Party Break-up PnL  a block per month, one column per marketplace party
#   Trade Party Break-up PnL        a block per month, one column per trade party, group subtotals among them
#   P&L FY27 Statement with Reco    one column per month: the company totals every month is checked against
MISC_CITY_TAB = "City X Channel"
MISC_TRADE_TAB = "Trade Party Break-up PnL"
MISC_STMT_TAB = "P&L FY27 Statement with Reco"
MISC_HDR_LABEL = "channel p&l: business i"
MISC_CITIES = ["Mumbai", "Pune", "Delhi NCR", "Hyderabad", "Bangalore", "Chennai", "Kolkata", "Ahmedabad",
               "Vadodara", "Surat"]                  # the Feeder's own spellings, so both sources key alike
MISC_CITY_LINES = {"net sales": "net_sales", "inter city": "inter", "intra city": "intra",
                   "rent and utilities": "rent", "salary - corporate": "salary_corporate",
                   "salary - direct corporate": "salary_direct", "corporate overheads": "corporate_overheads"}
MISC_STMT_LINES = {"net sales": "net_sales", "outward logistics": "logistics", "channel spends": "channel_spends",
                   "brand building costs": "brand_building", "consultancy & legal fees": "consultancy_legal",
                   "travelling & conveyance": "travel_conveyance", "gst": "gst", "software exp": "software",
                   "others": "others", "-yield loss & wh loss": "yield_loss"}
MISC_PARTY_LINES = {"net sales": "net_sales", "offers": "offers", "ads": "ads", "others": "other",
                    "salary - direct corporate": "salary_direct"}
MISC_TRADE_GROUPS = {"MTPREMIUM", "MTMASS", "DISTRIBUTOR", "GT"}          # subtotal columns in the trade blocks
MISC_TRADE_ALIAS = {"NB": "NATURESBASKET", "RELIANCESIGNATURE": "RELIANCE", "RELIANCESMART": "RELIANCE",
                    "METROCC": "RELIANCE", "GTS": "GT"}
MISC_TOLERANCE = 1000.0                              # rupees: a month that misses the statement by more is refused


def _k(s) -> str:
    return re.sub(r"[^A-Z0-9]", "", str(s or "").upper())


def _label(v) -> str:
    return re.sub(r"\s+", " ", str(v or "")).strip().lower()


def _first_rows(grid, col: int, wanted: Dict[str, str], stop: str = "ebitda") -> Dict[str, int]:
    """{field: row} for each wanted label, its first occurrence above `stop`."""
    out: Dict[str, int] = {}
    for r, row in enumerate(grid):
        lab = _label(_cell(row, col))
        if lab in wanted and wanted[lab] not in out:
            out[wanted[lab]] = r
        if lab == stop:
            break
    return out


def _serial_month(v) -> Optional[dt.date]:
    if isinstance(v, (int, float)) and not isinstance(v, bool) and v > 40000:
        return (EPOCH + dt.timedelta(days=int(v))).replace(day=1)
    return None


def _mis_statement(sheets) -> Dict[dt.date, Dict[str, float]]:
    grid = _grid(sheets, MIS_SHEET, MISC_STMT_TAB, cols="AZ", rows=80)
    rows = _first_rows(grid, 1, MISC_STMT_LINES)
    missing = set(MISC_STMT_LINES.values()) - set(rows)
    if missing:
        raise ValueError(f"tab {MISC_STMT_TAB!r}: cannot find {', '.join(sorted(missing))}")
    out = {}
    for c, v in enumerate(grid[0]):
        month = _serial_month(v)
        if month:
            vals = {f: _num(_cell(grid[r], c)) or 0.0 for f, r in rows.items()}
            if vals["net_sales"] > 0:                # an open month is all zero
                out[month] = vals
    return out


def _mis_city_blocks(sheets, by_number: Dict[int, dt.date]):
    grid = _grid(sheets, MIS_SHEET, MISC_CITY_TAB, cols="AHZ", rows=80)
    hr = next((i for i, row in enumerate(grid) if _label(_cell(row, 0)) == MISC_HDR_LABEL), None)
    if hr is None or hr < 2:
        raise ValueError(f"tab {MISC_CITY_TAB!r}: header row not found")
    rows = _first_rows(grid, 0, MISC_CITY_LINES)
    missing = set(MISC_CITY_LINES.values()) - set(rows)
    if missing:
        raise ValueError(f"tab {MISC_CITY_TAB!r}: cannot find {', '.join(sorted(missing))}")
    hdr, cityrow = grid[hr], grid[hr - 2]
    val = lambda f, c: _num(_cell(grid[rows[f]], c)) or 0.0          # noqa: E731
    out: Dict[dt.date, dict] = {}
    month, inblock = None, False
    for c, v in enumerate(hdr):
        sm = _serial_month(v)
        if sm:
            month, inblock = by_number.get(sm.month), True
            if month is not None:
                if month in out:
                    raise ValueError(f"tab {MISC_CITY_TAB!r}: two blocks for {month:%b %Y}")
                out[month] = {"city": {}}
            continue
        if not inblock or month is None or not isinstance(v, str):
            continue                                 # the YTD block before the first month, or an open month
        name = re.sub(r"\s+", " ", v).strip()
        b = out[month]
        if name in MISC_CITIES and str(_cell(cityrow, c) or "").strip() == name:
            b["city"][name] = {f: val(f, c) for f in MISC_CITY_LINES.values()}
    return out


def _mis_party_blocks(sheets, tab: str, trade_parties=None):
    """{month: {party: {offers, ads, other, net_sales}}} for the filled blocks. A trade block skips its group
    subtotals and names each party as Birbal does that month (else the sheet's own name)."""
    grid = _grid(sheets, MIS_SHEET, tab, cols="KZ", rows=120)
    hr = next((i for i, row in enumerate(grid) if _label(_cell(row, 0)) == MISC_HDR_LABEL), None)
    if hr is None:
        raise ValueError(f"tab {tab!r}: header row not found")
    rows = _first_rows(grid, 0, MISC_PARTY_LINES)
    missing = set(MISC_PARTY_LINES.values()) - set(rows)
    if missing:
        raise ValueError(f"tab {tab!r}: cannot find {', '.join(sorted(missing))}")
    blocks: List[Tuple[dt.date, Dict[str, Dict[str, float]]]] = []
    for c, v in enumerate(grid[hr]):
        sm = _serial_month(v)
        if sm:
            blocks.append((sm, {}))
            continue
        if not blocks or not isinstance(v, str) or not v.strip():
            continue
        month, blk = blocks[-1]
        name = re.sub(r"\s+", " ", v).strip()
        if trade_parties is not None:
            k = _k(name)
            if k in MISC_TRADE_GROUPS:
                continue
            known = trade_parties.get(month, {})
            name = known.get(k) or known.get(MISC_TRADE_ALIAS.get(k, "")) or name
        acc = blk.setdefault(name, {f: 0.0 for f in MISC_PARTY_LINES.values()})
        for f, r in rows.items():
            acc[f] += _num(_cell(grid[r], c)) or 0.0
    out: Dict[dt.date, Dict[str, Dict[str, float]]] = {}
    for month, blk in blocks:
        if not any(abs(p["net_sales"]) > 0 for p in blk.values()):
            continue                                 # template blocks for months finance has not filled
        if month in out:
            raise ValueError(f"tab {tab!r}: two filled blocks for {month:%b %Y}")
        out[month] = blk
    return out


MISC_TABLES = [   # table, key columns, value columns
    ("pnl_logistics_cost", ["month", "city"], ["inter", "intra"]),
    ("pnl_rent_utilities", ["month", "city"], ["amount"]),
    ("pnl_channel_spend", ["month", "party"], ["offers", "ads", "other"]),
    ("pnl_brand_building", ["month"], ["brand_building", "salary_corporate"]),
    ("pnl_salary_direct", ["month"], ["marketplace", "trade"]),
    ("pnl_corporate_overhead", ["month"], ["gratuity_provision", "consultancy_legal", "travel_conveyance", "gst",
                                           "software", "others"]),
    ("mis_yield_month", ["month"], ["amount"]),                         # Birbal 233: split over products there
]


def mis_cost_rows(sheets, cur) -> Tuple[Dict[str, List[tuple]], List[str]]:
    """{table: rows (keys + values)} for every month the MIS has closed, and a note per month."""
    stmt = _mis_statement(sheets)
    city = _mis_city_blocks(sheets, {m.month: m for m in stmt})
    mkt = _mis_party_blocks(sheets, MIS_TAB)
    cur.execute("select distinct month, party from warehouse.pnl_lines where business = 'TRADE' and party is not null")
    trade_parties: Dict[dt.date, Dict[str, str]] = {}
    for m, p in cur.fetchall():
        trade_parties.setdefault(m, {})[_k(p)] = p
    trade = _mis_party_blocks(sheets, MISC_TRADE_TAB, trade_parties)
    out: Dict[str, List[tuple]] = {t: [] for t, _, _ in MISC_TABLES}
    notes = []
    r2 = lambda x: round(x, 2)                                                  # noqa: E731
    for m in sorted(stmt):
        s, cb = stmt[m], city.get(m)
        if not cb or not cb["city"] or m not in mkt:
            notes.append(f"{m:%b %Y}: not in every tab yet, left to the Feeder")
            continue
        cities = cb["city"]
        tot = lambda f: sum(v[f] for v in cities.values())                      # noqa: E731
        mk = mkt[m]
        tr = {p: v for p, v in trade.get(m, {}).items() if any(abs(v[h]) > 0 for h in ("offers", "ads", "other"))}
        spend = sum(v[h] for d in (mk, tr) for v in d.values() for h in ("offers", "ads", "other"))
        checks = {"net sales": (tot("net_sales"), s["net_sales"]),
                  "logistics": (tot("inter") + tot("intra"), s["logistics"]),
                  "channel spends": (spend, s["channel_spends"])}
        bad = [f"{k} {a:,.0f} vs statement {b:,.0f}" for k, (a, b) in checks.items() if abs(a - b) > MISC_TOLERANCE]
        if bad:
            raise ValueError(f"{m:%b %Y}: the tabs do not tie to the statement ({'; '.join(bad)}). Refusing to load.")
        five = sum(s[h] for h in ("consultancy_legal", "travel_conveyance", "gst", "software", "others"))
        for c, v in cities.items():
            out["pnl_logistics_cost"].append((m, c, r2(v["inter"]), r2(v["intra"])))
            out["pnl_rent_utilities"].append((m, c, r2(v["rent"])))
        for p, v in list(mk.items()) + list(tr.items()):
            out["pnl_channel_spend"].append((m, p, r2(v["offers"]), r2(v["ads"]), r2(v["other"])))
        out["pnl_brand_building"].append((m, r2(s["brand_building"]), r2(tot("salary_corporate"))))
        sd_mkt = sum(v["salary_direct"] for v in mk.values())
        out["pnl_salary_direct"].append((m, r2(sd_mkt), r2(tot("salary_direct") - sd_mkt)))
        out["pnl_corporate_overhead"].append((m, r2(tot("corporate_overheads") - five), r2(s["consultancy_legal"]),
                                              r2(s["travel_conveyance"]), r2(s["gst"]), r2(s["software"]),
                                              r2(s["others"])))
        out["mis_yield_month"].append((m, r2(s["yield_loss"])))
        unplaced = sorted(p for p in tr if p not in trade_parties.get(m, {}).values())
        notes.append(f"{m:%b %Y}: logistics {tot('inter') + tot('intra'):,.0f}, channel spends {spend:,.0f} "
                     f"({len(mk)} marketplace + {len(tr)} trade parties"
                     + (f"; not a Birbal trade party, so spread: {', '.join(unplaced)}" if unplaced else "") + ")")
    return out, notes


def _norm_row(r) -> tuple:
    return tuple(round(float(x), 2) if x is not None and not isinstance(x, (str, dt.date)) else x for x in r)


def load_mis_costs(sheets, cur, write: bool) -> Tuple[int, int, int, str]:
    rows, notes = mis_cost_rows(sheets, cur)
    for n in notes:
        logger.info("  %s", n)
    months = sorted({r[0] for r in rows["pnl_brand_building"]})
    read = sum(len(v) for v in rows.values())
    span = f"{len(months)} months" + (f" {months[0]}..{months[-1]}" if months else "")
    if not write or not months:
        return read, 0, 0, span
    ins = gone_all = 0
    detail = []
    for table, keys, vals in MISC_TABLES:
        cols = keys + vals
        cur.execute(f"select {', '.join(cols)}, source from public.{table} where month = any(%s)", [months])
        held: Dict[dt.date, set] = {}
        for r in cur.fetchall():
            held.setdefault(r[0], set()).add(_norm_row(r))
        new: Dict[dt.date, set] = {}
        for r in rows[table]:
            new.setdefault(r[0], set()).add(_norm_row(tuple(r) + ("management_mis",)))
        changed = [m for m in months if held.get(m, set()) != new.get(m, set())]
        if not changed:
            continue
        cur.execute(f"delete from public.{table} where month = any(%s)", [changed])
        gone = cur.rowcount
        put = [tuple(r) + ("management_mis",) for r in rows[table] if r[0] in changed]
        one = "(" + ", ".join(["%s"] * (len(cols) + 1)) + ")"
        for i in range(0, len(put), CHUNK):
            part = put[i:i + CHUNK]
            cur.execute(f"insert into public.{table} ({', '.join(cols)}, source) values " + ", ".join([one] * len(part)),
                        [v for r in part for v in r])
        ins, gone_all = ins + len(put), gone_all + gone
        detail.append(f"{table}: {len(changed)} months replaced (-{gone} +{len(put)})")
    return read, ins, gone_all, f"{span}; " + ("; ".join(detail) or "no row changed")


@dataclass
class SheetSource:
    key: str
    sheet_id: str
    load: Callable


SOURCES = [
    SheetSource("campaign_master", CAMPAIGN_SHEET, load_campaign_master),
    SheetSource("pnl_feeder", FEEDER_SHEET, load_feeder),
    SheetSource("mis_costs", MIS_SHEET, load_mis_costs),
    SheetSource("party_master", PARTY_MASTER_SHEET, load_party_master),
    SheetSource("prn_bb", PRN_BB_SHEET, load_prn_bb),
    SheetSource("prn_nb", PRN_NB_SHEET, load_prn_nb),
    SheetSource("capping_booked", CAPPING_SHEET, load_capping_booked),
    SheetSource("platform_foc", FOC_SHEET, load_platform_foc),
    SheetSource("mis_party_pnl", MIS_SHEET, load_mis_party_pnl),
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
