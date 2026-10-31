"""Platform stock on hand (SOH) -> public.mp_soh, for Birbal (migration 237).

    python -m mp.soh --check                 # list + parse every file, write nothing
    python -m mp.soh --run                   # load files that are new or edited in Drive
    python -m mp.soh --run --force           # re-read every file

The marketplace team uploads each platform's stock export to the Drive folder
"Platform Wise SOH Data" (owned by harshil@, shared to marketing@, started
2026-10-01), one sub-folder per platform:

  Zepto       city x SKU "Units"; one file per day named DD-MM-YYYY (no extension).
              Some files carry a Date column and several days (05-10-2026 holds
              3-5 Oct) -- the column wins over the name. "31-07-2026 to
              30-09-2026" is a back-history workbook (tab "Master SOH": Date,
              City, SKU Name, Units) with NO SKU code: codes are filled from the
              names in the dailies and in mp_sales after every run.
  Blinkit     feeder warehouse x item: backend_inv_qty (the feeder) +
              frontend_inv_qty (the dark stores it serves). created_at is the
              date. No city column: city is the facility name's town
              ("Jaipur J3 - Feeder Warehouse" -> Jaipur).
  Instamart   warehouse x SKU "WarehouseQtyAvailable" + open PO quantity.
              ItemInventory_<epoch ms> (<day> <Mon>).csv: the epoch is the
              export time, so it gives the date with its year.
  Big Basket  workbook with "DC Level SOH" and "Store Level SOH". Only the DC
              tab is read: its DS SOH is exactly the sum of the store tab
              (checked on 06-10-2026), and the store tab repeats the DC total
              on every store row, so summing it overstates stock ~20x.
  Flipkart    city x product_id "Total_Inv" (Flipkart Minutes). The date is
              only in the name ("30th Sep INV Flipkart.csv"); the year comes
              from the upload.
  Amazon, Milk Basket   empty folders so far. A file in a folder with no
              reader is reported, never guessed at.

Every file is matched on its HEADER NAMES; a file missing a needed column is
recorded as an error in the ledger and writes nothing.

Ledger public.mp_soh_files: one row per Drive file with its modifiedTime and
the days it holds. A file is re-read when Drive's modifiedTime moves. When two
files hold the same platform-day (a re-upload, or Zepto's multi-day files) the
one modified LAST wins: public.mp_soh_days names the winning file per
platform-day, rebuilt at the end of every run, and Birbal reads only winning
rows. A file that disappears from Drive (deleted or trashed) loses its rows --
guarded: a listing with no files at all deletes nothing.
"""
from __future__ import annotations

import argparse
import datetime as dt
import io
import logging
import os
import re
import sys
from typing import Callable, Dict, List, Optional, Sequence, Tuple

import pandas as pd
from googleapiclient.http import MediaIoBaseDownload

from . import dbadmin
from .drive import PROJECT, FOLDER_MIME, _list_children, build_service, is_junk
from .sink import load_dotenv

logger = logging.getLogger("mp.soh")

#: Drive folder "Platform Wise SOH Data".
SOH_ROOT_FOLDER_ID = "13ZaE7_1Eu__HESm9fiBRtFwSHEEnJ-qR"

IST = dt.timezone(dt.timedelta(hours=5, minutes=30))
GSHEET_MIME = "application/vnd.google-apps.spreadsheet"
XLSX_MIME = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
COLUMNS = ["platform", "snapshot_date", "sku_code", "sku_name", "city", "location",
           "qty", "qty_warehouse", "qty_store", "open_po_qty", "source_file", "drive_file_id"]
COLUMN_TYPES = {c: "text" for c in COLUMNS} | {
    "snapshot_date": "date", "qty": "numeric", "qty_warehouse": "numeric",
    "qty_store": "numeric", "open_po_qty": "numeric"}
CHUNK = 20000


class Unreadable(Exception):
    """The file is not a shape this loader knows: recorded, nothing written."""


# --------------------------------------------------------------------------- #
# Parsing helpers                                                              #
# --------------------------------------------------------------------------- #

def _norm(h) -> str:
    return re.sub(r"[^a-z0-9]+", "_", str(h).strip().lower()).strip("_")


def _num(v) -> Optional[float]:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    s = str(v).replace(",", "").strip()
    if s in ("", "-", "nan", "NaN", "None"):
        return None
    try:
        return float(s)
    except ValueError:
        return None


def _text(v) -> Optional[str]:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    s = str(v).strip()
    return s or None


_MONTHS = {m: i for i, m in enumerate(
    ["jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"], 1)}


def _date(v) -> Optional[dt.date]:
    """A date cell. Every SOH export is day-first."""
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    if isinstance(v, (dt.datetime, pd.Timestamp)):
        return v.date()
    if isinstance(v, dt.date):
        return v
    s = str(v).strip()
    m = re.match(r"^(\d{4})-(\d{2})-(\d{2})", s)                      # 2026-10-15[ 00:00:00]
    if m:
        return dt.date(int(m[1]), int(m[2]), int(m[3]))
    m = re.match(r"^(\d{1,2})[-/.](\d{1,2})[-/.](\d{4})$", s)         # 31-07-2026
    if m:
        return dt.date(int(m[3]), int(m[2]), int(m[1]))
    m = re.match(r"^(\d{1,2})[- ]([A-Za-z]{3})[A-Za-z]*[- ](\d{2,4})$", s)   # 01-oct-2026
    if m and m[2].lower() in _MONTHS:
        y = int(m[3]) + (2000 if len(m[3]) == 2 else 0)
        return dt.date(y, _MONTHS[m[2].lower()], int(m[1]))
    return None


def _date_from_name(name: str, uploaded: dt.date) -> Optional[dt.date]:
    """The snapshot day a file NAME gives, or None.

    Instamart: ItemInventory_<epoch ms> -> the export day in IST. Otherwise a
    full date (31-07-2026, 01-oct-2026), else a day + month with no year
    ("30th Sep", "(7th Oct)"), whose year is the upload's -- one year back if
    that would put it after the upload.
    """
    m = re.search(r"ItemInventory_(\d{13})", name)
    if m:
        return dt.datetime.fromtimestamp(int(m[1]) / 1000, IST).date()
    stem = re.sub(r"\.(csv|xlsx?|xls)$", "", name, flags=re.I)
    for m in re.finditer(r"\d{1,2}[-/. ](?:\d{1,2}|[A-Za-z]{3,9})[-/. ]\d{2,4}", stem):
        d = _date(m[0].replace(" ", "-"))
        if d:
            return d
    m = re.search(r"(\d{1,2})(?:st|nd|rd|th)?[\s-]+([A-Za-z]{3})[A-Za-z]*", stem)
    if m and m[2].lower() in _MONTHS:
        d = dt.date(uploaded.year, _MONTHS[m[2].lower()], int(m[1]))
        return d.replace(year=d.year - 1) if d > uploaded + dt.timedelta(days=1) else d
    return None


def _pick(cols: Sequence[str], *names: str) -> Optional[str]:
    for n in names:
        if n in cols:
            return n
    return None


def _need(df: pd.DataFrame, what: str, *names: str) -> str:
    c = _pick(df.columns, *names)
    if c is None:
        raise Unreadable(f"no {what} column (looked for {', '.join(names)}; "
                         f"file has {', '.join(df.columns[:20])})")
    return c


def _snapshot_dates(df: pd.DataFrame, date_col: Optional[str], name_date: Optional[dt.date]) -> pd.Series:
    if date_col:
        d = df[date_col].map(_date)
        if name_date is not None:
            d = d.fillna(name_date)
        return d
    if name_date is None:
        raise Unreadable("no date column and no date in the file name")
    return pd.Series([name_date] * len(df), index=df.index)


# --------------------------------------------------------------------------- #
# Per-platform readers. Each takes the file's sheets (normalised headers) and   #
# returns rows keyed by COLUMNS minus platform / provenance.                   #
# --------------------------------------------------------------------------- #

def _first_sheet_with(sheets: Dict[str, pd.DataFrame], *cols: str) -> pd.DataFrame:
    for df in sheets.values():
        if all(c in df.columns for c in cols):
            return df
    found = {k: list(v.columns[:12]) for k, v in sheets.items()}
    raise Unreadable(f"no sheet has {', '.join(cols)}: {found}")


def read_zepto(sheets, name_date) -> List[dict]:
    df = _first_sheet_with(sheets, "city", "sku_name", "units")
    dates = _snapshot_dates(df, _pick(df.columns, "date"), name_date)
    code = _pick(df.columns, "sku_code", "sku_number")
    return [{"snapshot_date": d, "sku_code": _text(r[code]) if code else None,
             "sku_name": _text(r["sku_name"]), "city": _text(r["city"]), "location": None,
             "qty": _num(r["units"])} for d, (_, r) in zip(dates, df.iterrows())]


_BLINKIT_PREFIX = re.compile(r"^\s*(?:super\s+store|ss|bulk\s+perishable\s*-)\s+", re.I)


def blinkit_city(facility: Optional[str]) -> Optional[str]:
    """The facility's town, its first word once the store-type prefix is off:
    'Jaipur J3 - Feeder Warehouse', 'SS Jaipur PC - Warehouse' -> 'Jaipur';
    'Bulk Perishable - Bengaluru Doddabanalli PC2' -> 'Bengaluru'. All 66
    facility names of 15-10-2026 come out as a town this way."""
    if not facility:
        return None
    m = re.match(r"[A-Za-z]+", _BLINKIT_PREFIX.sub("", facility))
    return m[0] if m else None


def read_blinkit(sheets, name_date) -> List[dict]:
    df = _first_sheet_with(sheets, "item_id", "backend_inv_qty", "frontend_inv_qty")
    fac = _need(df, "facility", "backend_facility_name", "facility_name")
    dates = _snapshot_dates(df, _pick(df.columns, "created_at", "date"), name_date)
    out = []
    for d, (_, r) in zip(dates, df.iterrows()):
        back, front = _num(r["backend_inv_qty"]), _num(r["frontend_inv_qty"])
        out.append({"snapshot_date": d, "sku_code": _text(r["item_id"]),
                    "sku_name": _text(r.get("item_name")), "city": blinkit_city(_text(r[fac])),
                    "location": _text(r[fac]),
                    "qty": None if back is None and front is None else (back or 0) + (front or 0),
                    "qty_warehouse": back, "qty_store": front})
    return out


def read_instamart(sheets, name_date) -> List[dict]:
    df = _first_sheet_with(sheets, "skucode", "warehouseqtyavailable")
    dates = _snapshot_dates(df, _pick(df.columns, "date", "snapshot_date"), name_date)
    return [{"snapshot_date": d, "sku_code": _text(r["skucode"]),
             "sku_name": _text(r.get("skudescription")), "city": _text(r.get("city")),
             "location": _text(r.get("facilityname")),
             "qty": _num(r["warehouseqtyavailable"]), "qty_warehouse": _num(r["warehouseqtyavailable"]),
             "open_po_qty": _num(r.get("openpoquantity"))}
            for d, (_, r) in zip(dates, df.iterrows())]


def read_bigbasket(sheets, name_date) -> List[dict]:
    # the DC tab only -- see the module docstring
    df = _first_sheet_with(sheets, "sku", "dc_soh", "ds_soh")
    total = _need(df, "total", "final_dc_ds_soh")
    dates = _snapshot_dates(df, _pick(df.columns, "date"), name_date)
    return [{"snapshot_date": d, "sku_code": _text(r["sku"]),
             "sku_name": _text(r.get("sku_full_description")), "city": _text(r.get("city")),
             "location": _text(r.get("location")),
             "qty": _num(r[total]), "qty_warehouse": _num(r["dc_soh"]), "qty_store": _num(r["ds_soh"])}
            for d, (_, r) in zip(dates, df.iterrows())]


def read_flipkart(sheets, name_date) -> List[dict]:
    df = _first_sheet_with(sheets, "product_id", "total_inv")
    dates = _snapshot_dates(df, _pick(df.columns, "date"), name_date)
    return [{"snapshot_date": d, "sku_code": _text(r["product_id"]),
             "sku_name": _text(r.get("product_title")), "city": _text(r.get("city")), "location": None,
             "qty": _num(r["total_inv"])} for d, (_, r) in zip(dates, df.iterrows())]


#: Drive sub-folder (lower case) -> (platform label, reader). Labels are the
#: ones warehouse.sales_daily and warehouse.ref_platform_party use.
READERS: Dict[str, Tuple[str, Optional[Callable]]] = {
    "zepto": ("Zepto", read_zepto),
    "blinkit": ("Blinkit", read_blinkit),
    "instamart": ("Instamart", read_instamart),
    "scootsy": ("Instamart", read_instamart),
    "big basket": ("Big Basket", read_bigbasket),
    "bigbasket": ("Big Basket", read_bigbasket),
    "flipkart": ("Flipkart", read_flipkart),
    "amazon": ("Amazon", None),
    "milk basket": ("Milkbasket", None),
    "milkbasket": ("Milkbasket", None),
}


# --------------------------------------------------------------------------- #
# Drive                                                                        #
# --------------------------------------------------------------------------- #

LIST_FIELDS = "nextPageToken, files(id,name,mimeType,size,createdTime,modifiedTime)"


def list_files(svc) -> List[dict]:
    """Every file under the SOH root, with its platform folder."""
    out = []

    def walk(fid: str, top: Optional[str], path: str):
        page = None
        while True:
            resp = svc.files().list(q=f"'{fid}' in parents and trashed = false", fields=LIST_FIELDS,
                                    pageSize=1000, pageToken=page, supportsAllDrives=True,
                                    includeItemsFromAllDrives=True).execute()
            for f in resp.get("files", []):
                if f["mimeType"] == FOLDER_MIME:
                    walk(f["id"], top or f["name"], f"{path}/{f['name']}")
                elif not is_junk(f["name"]):
                    f["folder"] = top
                    f["path"] = f"{path}/{f['name']}"
                    out.append(f)
            page = resp.get("nextPageToken")
            if not page:
                return

    walk(SOH_ROOT_FOLDER_ID, None, "")
    return out


def fetch(svc, f: dict) -> Dict[str, pd.DataFrame]:
    """Download a file into memory and return its sheets with normalised headers."""
    if f["mimeType"] == GSHEET_MIME:
        req = svc.files().export_media(fileId=f["id"], mimeType=XLSX_MIME)
        kind = "xlsx"
    else:
        req = svc.files().get_media(fileId=f["id"], supportsAllDrives=True)
        name = f["name"].lower()
        kind = ("xlsx" if name.endswith((".xlsx", ".xlsm")) or "spreadsheetml" in f["mimeType"]
                else "xls" if name.endswith(".xls") or f["mimeType"] == "application/vnd.ms-excel"
                else "csv")
    buf = io.BytesIO()
    dl = MediaIoBaseDownload(buf, req, chunksize=16 * 1024 * 1024)
    done = False
    while not done:
        _, done = dl.next_chunk(num_retries=3)
    buf.seek(0)
    if kind == "csv":
        raw = buf.getvalue()
        try:
            text = raw.decode("utf-8-sig")
        except UnicodeDecodeError:
            text = raw.decode("latin-1")
        sheets = {"csv": pd.read_csv(io.StringIO(text), dtype=str, keep_default_na=False)}
    else:
        sheets = pd.read_excel(buf, sheet_name=None, dtype=object,
                               engine="xlrd" if kind == "xls" else None)
    for df in sheets.values():
        df.columns = [_norm(c) for c in df.columns]
    return sheets


def _ts(s: str) -> dt.datetime:
    return dt.datetime.fromisoformat(s.replace("Z", "+00:00"))


def parse(svc, f: dict) -> Tuple[str, List[dict]]:
    key = (f.get("folder") or "").strip().lower()
    if key not in READERS:
        raise Unreadable(f"folder '{f.get('folder')}' is not a known platform")
    platform, reader = READERS[key]
    if reader is None:
        raise Unreadable(f"no reader for {platform} yet: send the file to the data team")
    uploaded = _ts(f["createdTime"]).astimezone(IST).date()
    rows = reader(fetch(svc, f), _date_from_name(f["name"], uploaded))
    rows = [r for r in rows if r.get("sku_code") or r.get("sku_name")]
    if not rows:
        raise Unreadable("no data rows")
    bad = sum(1 for r in rows if r["snapshot_date"] is None)
    if bad:
        raise Unreadable(f"{bad} rows have no readable date")
    for r in rows:
        r["platform"] = platform
        r["source_file"] = f["path"].lstrip("/")
        r["drive_file_id"] = f["id"]
    return platform, rows


# --------------------------------------------------------------------------- #
# Database                                                                     #
# --------------------------------------------------------------------------- #

def _write_file(cur, f: dict, platform: Optional[str], rows: List[dict], status: str, detail: str):
    cur.execute("delete from public.mp_soh where drive_file_id = %s", [f["id"]])
    # one statement per chunk of column arrays: per-row parameters cost a round
    # trip per 1,000 rows over the pooler (~20 s a file)
    for i in range(0, len(rows), CHUNK):
        part = rows[i:i + CHUNK]
        cur.execute(f"insert into public.mp_soh ({', '.join(COLUMNS)}) select * from unnest("
                    + ", ".join(f"%s::{COLUMN_TYPES[c]}[]" for c in COLUMNS) + ")",
                    [[r.get(c) for r in part] for c in COLUMNS])
    days = sorted({r["snapshot_date"] for r in rows})
    cur.execute("""
        insert into public.mp_soh_files
            (drive_file_id, platform, folder, file_name, path, created_time, modified_time,
             snapshot_dates, rows_loaded, qty_total, status, detail, loaded_at)
        values (%s, %s, %s, %s, %s, %s, %s, %s::date[], %s, %s, %s, %s, now())
        on conflict (drive_file_id) do update set
            platform = excluded.platform, folder = excluded.folder, file_name = excluded.file_name,
            path = excluded.path, created_time = excluded.created_time,
            modified_time = excluded.modified_time, snapshot_dates = excluded.snapshot_dates,
            rows_loaded = excluded.rows_loaded, qty_total = excluded.qty_total,
            status = excluded.status, detail = excluded.detail, loaded_at = now()""",
                [f["id"], platform, f.get("folder"), f["name"], f["path"].lstrip("/"),
                 _ts(f["createdTime"]), _ts(f["modifiedTime"]),
                 "{" + ",".join(d.isoformat() for d in days) + "}",
                 len(rows), sum(r["qty"] or 0 for r in rows), status, detail])


def _finish(cur, live_ids: List[str]) -> Dict[str, int]:
    """Drop files gone from Drive, fill Zepto codes by name, rebuild the winners."""
    out = {}
    if live_ids:
        cur.execute("""with gone as (
                           delete from public.mp_soh_files where not (drive_file_id = any(%s::text[]))
                           returning drive_file_id)
                       delete from public.mp_soh s using gone where s.drive_file_id = gone.drive_file_id""",
                    ["{" + ",".join(live_ids) + "}"])
        out["rows_of_removed_files"] = cur.rowcount
    # the Zepto back-history carries names only: take each name's code from the
    # dailies, else from the sell-out feed (latest day wins)
    cur.execute("""
        update public.mp_soh s set sku_code = m.sku_code
          from (select distinct on (sku_name) sku_name, sku_code
                  from (select sku_name, sku_code, snapshot_date d from public.mp_soh
                         where platform = 'Zepto' and sku_code is not null
                        union all
                        select sku_name, sku_code, sale_date from public.mp_sales
                         where platform = 'Zepto' and sku_code is not null
                           and sku_name in (select sku_name from public.mp_soh
                                             where platform = 'Zepto' and sku_code is null)) x
                 order by sku_name, d desc) m
         where s.platform = 'Zepto' and s.sku_code is null and s.sku_name = m.sku_name""")
    out["zepto_codes_filled"] = cur.rowcount
    cur.execute("delete from public.mp_soh_days where true")
    cur.execute("""
        insert into public.mp_soh_days (platform, snapshot_date, drive_file_id, modified_time)
        select distinct on (f.platform, d) f.platform, d, f.drive_file_id, f.modified_time
          from public.mp_soh_files f, unnest(f.snapshot_dates) d
         where f.status = 'loaded'
         order by f.platform, d, f.modified_time desc, f.drive_file_id""")
    out["platform_days"] = cur.rowcount
    return out


def run(write: bool, force: bool) -> int:
    svc = build_service()
    files = sorted(list_files(svc), key=lambda f: f["modifiedTime"])
    logger.info("Drive: %d files under Platform Wise SOH Data", len(files))
    conn = dbadmin.connect(timeout=300) if write else None
    cur = conn.cursor() if conn else None
    seen = {}
    if cur:
        cur.execute("set statement_timeout = '600000'")
        cur.execute("select drive_file_id, modified_time from public.mp_soh_files")
        seen = dict(cur.fetchall())
    failures = 0
    for f in files:
        mtime = _ts(f["modifiedTime"])
        if write and not force and f["id"] in seen and seen[f["id"]] is not None and seen[f["id"]] >= mtime:
            continue
        try:
            platform, rows = parse(svc, f)
            days = sorted({r["snapshot_date"] for r in rows})
            detail = f"{len(rows)} rows, {len(days)} day(s) {days[0]}..{days[-1]}"
            logger.info("%-55s %-10s %s, qty %.0f", f["path"][:55], platform, detail,
                        sum(r["qty"] or 0 for r in rows))
            if cur:
                _write_file(cur, f, platform, rows, "loaded", detail)
                conn.commit()
        except Unreadable as exc:
            logger.warning("%s: NOT LOADED -- %s", f["path"], exc)
            if cur:
                conn.rollback()
                plat = READERS.get((f.get("folder") or "").lower(), (None,))[0]
                _write_file(cur, f, plat, [], "error", str(exc)[:500])
                conn.commit()
        except Exception as exc:                   # one bad file must not stop the rest
            failures += 1
            logger.error("%s: FAILED, nothing written: %s", f["path"], exc)
            if cur:
                conn.rollback()
    if cur:
        stats = _finish(cur, [f["id"] for f in files])
        conn.commit()
        cur.execute("""select platform, min(snapshot_date), max(snapshot_date), count(*)
                         from public.mp_soh_days group by 1 order by 1""")
        for p, lo, hi, n in cur.fetchall():
            logger.info("  %-11s %s .. %s  (%d days)", p, lo, hi, n)
        logger.info("finish: %s", stats)
        conn.close()
    return failures


def main(argv: Optional[Sequence[str]] = None) -> int:
    load_dotenv(os.path.join(PROJECT, ".env"))     # real environment variables win
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    mode = ap.add_mutually_exclusive_group(required=True)
    mode.add_argument("--check", action="store_true", help="read and parse, write nothing")
    mode.add_argument("--run", action="store_true", help="load files that are new or edited")
    ap.add_argument("--force", action="store_true", help="re-read every file")
    a = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    return 1 if run(write=a.run, force=a.force) else 0


if __name__ == "__main__":
    sys.exit(main())
