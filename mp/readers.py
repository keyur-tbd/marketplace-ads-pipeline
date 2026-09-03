"""Turn a downloaded Drive file into normalized row dicts.

Every platform exports differently, and the awkward parts are all here:

  * Flipkart daily CSVs open with two preamble lines ("Start Time, <ts>" /
    "End Time, <ts>") before the real header. That preamble is also the most
    reliable report date, so it is parsed rather than skipped.
  * Big Basket, Zepto and the Flipkart dailies have no date column at all - the
    file name is the only date.
  * Blinkit ships one workbook per month with 5-7 sheets, each a different ad
    format with its own columns.
  * Some months of the split files carry a blank leading row, so the header row
    is found rather than assumed.
  * Instamart CSVs run to hundreds of MB, so files are streamed in chunks.

Column names are normalized to snake_case once, here, so the rest of the
pipeline and the Postgres tables never see 'Click-Thru Rate (CTR)' or
'14 Day Total Sales '.
"""

import csv
import datetime as dt
import logging
import math
import os
import re
from typing import Any, Dict, Iterator, List, Optional, Tuple

import pandas as pd

from .sources import FILENAME, PLATFORM_ALIASES, SALES_COLUMNS, ComposeDate, Source

logger = logging.getLogger("mp.readers")

BATCH = 5000


class WrongShape(Exception):
    """A projected source's file has none of the mapped columns.

    For sales sources the mapped columns are the report's signature. A file
    in that folder lacking them is a different report filed in the wrong place,
    and loading it would produce rows of nulls. Raised so the pipeline records
    the file as misfiled instead of loading it.
    """

_MONTHS = {
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6, "jul": 7,
    "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
    "january": 1, "february": 2, "march": 3, "april": 4, "june": 6,
    "july": 7, "august": 8, "september": 9, "sept": 9, "october": 10,
    "november": 11, "december": 12,
}


# --------------------------------------------------------------------------- #
# Column names                                                                 #
# --------------------------------------------------------------------------- #

def normalize_column(name: Any, position: int = 0) -> str:
    """'Click-Thru Rate (CTR)' -> 'click_thru_rate_ctr'.

    Postgres identifiers cannot start with a digit, so '14 Day Total Sales'
    becomes 'x_14_day_total_sales' rather than something that needs quoting
    everywhere it is used.
    """
    text = "" if name is None else str(name)
    if text.strip().lower() in ("", "nan", "none", "unnamed: 0"):
        return f"col_{position}"
    text = re.sub(r"[₹$€£%]", " ", text)
    text = re.sub(r"[^0-9A-Za-z]+", "_", text.strip().lower())
    text = re.sub(r"_+", "_", text).strip("_")
    if not text:
        return f"col_{position}"
    if text[0].isdigit():
        text = "x_" + text
    return text[:60]


def normalize_columns(names) -> List[str]:
    """Normalize a header row, disambiguating any repeats."""
    out: List[str] = []
    seen: Dict[str, int] = {}
    for i, raw in enumerate(names):
        col = normalize_column(raw, i)
        n = seen.get(col, 0)
        seen[col] = n + 1
        out.append(col if n == 0 else f"{col}_{n + 1}")
    return out


# --------------------------------------------------------------------------- #
# Values                                                                       #
# --------------------------------------------------------------------------- #

def clean_value(value: Any) -> Any:
    """pandas/numpy scalar -> something json + PostgREST can carry."""
    if value is None:
        return None
    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            return None
        return value
    if isinstance(value, (dt.datetime, pd.Timestamp)):
        if pd.isna(value):
            return None
        return value.isoformat()
    if isinstance(value, dt.date):
        return value.isoformat()
    if value is pd.NaT:
        return None
    if hasattr(value, "item"):          # numpy scalar
        try:
            return clean_value(value.item())
        except Exception:               # noqa: BLE001
            return str(value)
    if isinstance(value, str):
        s = value.strip()
        return None if s == "" or s.lower() in ("nan", "null", "#n/a") else s
    return value


def canonical_platform(value: Any) -> Optional[str]:
    """Map a platform label to its canonical name.

    Carries the user's rule that 'Scootsy' means Instamart.
    """
    v = clean_value(value)
    if v is None:
        return None
    # Collapse internal whitespace first: these labels are typed by hand and
    # 'Big  Basket' should not miss the alias table over a double space.
    text = re.sub(r"\s+", " ", str(v)).strip()
    return PLATFORM_ALIASES.get(text.lower(), text.upper())


# --------------------------------------------------------------------------- #
# Dates                                                                        #
# --------------------------------------------------------------------------- #

def _yy(year: str) -> int:
    y = int(year)
    if y >= 100:
        return y
    return 2000 + y if y < 70 else 1900 + y


def date_from_filename(name: str) -> Tuple[Optional[str], str]:
    """Best-effort report date from a file name.

    Returns (iso_date, grain) where grain is 'day', 'month' or 'none'.
    Handles 08-Aug-26.xlsx, 26-July-26.xlsx, 12-feb-26.csv, Aug-26.xlsx.
    Ranges like 'Feb To Dec-25' are deliberately not guessed at.
    """
    stem = os.path.splitext(os.path.basename(name))[0]
    if re.search(r"\bto\b", stem, re.I):
        # A range: the file spans many dates, so per-row dates must come from
        # the data itself. Do not invent one.
        return None, "none"

    m = re.search(r"(\d{1,2})[-_. ]+([A-Za-z]{3,9})[-_. ']+(\d{2,4})", stem)
    if m and m.group(2).lower() in _MONTHS:
        d, mon, y = int(m.group(1)), _MONTHS[m.group(2).lower()], _yy(m.group(3))
        try:
            return dt.date(y, mon, d).isoformat(), "day"
        except ValueError:
            pass

    m = re.search(r"\b([A-Za-z]{3,9})[-_. ']+(\d{2,4})\b", stem)
    if m and m.group(1).lower() in _MONTHS:
        mon, y = _MONTHS[m.group(1).lower()], _yy(m.group(2))
        return dt.date(y, mon, 1).isoformat(), "month"

    m = re.search(r"\b(\d{4})[-_.](\d{1,2})[-_.](\d{1,2})\b", stem)
    if m:
        try:
            return dt.date(int(m.group(1)), int(m.group(2)),
                           int(m.group(3))).isoformat(), "day"
        except ValueError:
            pass
    return None, "none"


#: Excel serial dates are days since 1899-12-30. Bounded to roughly 2010-2064 so
#: an ordinary number in a date column is not silently turned into a date.
_EXCEL_EPOCH = dt.date(1899, 12, 30)
_EXCEL_MIN, _EXCEL_MAX = 40000, 60000


def _excel_serial(v: Any) -> Optional[str]:
    """'45824' -> '2025-06-12'.

    Amazon's budget export writes part of a file's Date column as formatted text
    ('Feb 2, 2025') and part as the raw Excel serial, in the same file. Without
    this, those rows lost their date entirely - 508 of 8,204 in one file.
    """
    try:
        num = float(v)
    except (TypeError, ValueError):
        return None
    if not (_EXCEL_MIN <= num <= _EXCEL_MAX) or num != int(num):
        return None
    return (_EXCEL_EPOCH + dt.timedelta(days=int(num))).isoformat()


def parse_date(value: Any) -> Optional[str]:
    """Parse the date formats these exports actually use, to an ISO date."""
    v = clean_value(value)
    if v is None:
        return None
    if isinstance(v, (int, float)) and not isinstance(v, bool):
        return _excel_serial(v)
    if isinstance(v, str):
        if v.strip().isdigit():
            serial = _excel_serial(v.strip())
            if serial:
                return serial
        s = v.strip()
        for fmt in ("%Y-%m-%d", "%d-%m-%Y", "%d/%m/%Y", "%Y-%m-%dT%H:%M:%S",
                    "%Y-%m-%d %H:%M:%S", "%d-%b-%y", "%d-%b-%Y", "%b-%y",
                    "%d-%B-%y", "%m/%d/%Y"):
            try:
                return dt.datetime.strptime(s[:len(fmt) + 6], fmt).date().isoformat()
            except ValueError:
                continue
        try:
            parsed = pd.to_datetime(s, dayfirst=True, errors="coerce")
            return None if pd.isna(parsed) else parsed.date().isoformat()
        except Exception:  # noqa: BLE001
            return None
    if isinstance(v, (dt.datetime, pd.Timestamp)):
        return v.date().isoformat()
    if isinstance(v, dt.date):
        return v.isoformat()
    return None


def parse_number(value: Any) -> Optional[float]:
    v = clean_value(value)
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return float(v)
    s = re.sub(r"[,\s₹$€£]", "", str(v))
    if s.endswith("%"):
        s = s[:-1]
    if s in ("", "-", "--", "NA", "N/A"):
        return None
    try:
        return float(s)
    except ValueError:
        return None


# --------------------------------------------------------------------------- #
# Header detection                                                             #
# --------------------------------------------------------------------------- #

def _looks_like_header(cells: List[Any]) -> bool:
    filled = [c for c in cells if clean_value(c) is not None]
    if len(filled) < 2:
        return False
    texty = sum(1 for c in filled if isinstance(c, str)
                and not re.fullmatch(r"-?[\d.,]+", c.strip()))
    return texty >= max(2, int(len(filled) * 0.7))


def find_header_row(frame: pd.DataFrame, limit: int = 10) -> int:
    """First row that reads like a header. Falls back to row 0."""
    best_width = max((sum(1 for c in frame.iloc[i].tolist()
                          if clean_value(c) is not None)
                      for i in range(min(limit, len(frame)))), default=0)
    for i in range(min(limit, len(frame))):
        cells = frame.iloc[i].tolist()
        width = sum(1 for c in cells if clean_value(c) is not None)
        if width >= max(2, best_width * 0.8) and _looks_like_header(cells):
            return i
    return 0


def find_anchored_header(rows: List[List[Any]], anchors: List[str],
                         limit: int = 25) -> Optional[int]:
    """First row whose normalized cells contain every anchor column.

    For projected sources the mapped columns ARE the header signature, which
    makes detection exact rather than heuristic: Amazon Vendor Central's daily
    workbooks open with a one-row metadata block ('Programme=[Retail]',
    'Currency=[INR]', ...) that looks header-like to a generic scorer but never
    contains 'asin', so it is skipped and the real header on row 1 is found.
    Returns None if no row qualifies, so the caller can fall back.
    """
    wanted = set(anchors)
    if not wanted:
        return None
    for i, cells in enumerate(rows[:limit]):
        cols = set(normalize_columns(cells))
        if wanted <= cols:
            return i
    return None


def _excel_serial_parts(v: Any) -> Optional[int]:
    try:
        return int(float(v))
    except (TypeError, ValueError):
        return None


def compose_date(row: Dict[str, Any], rule: ComposeDate) -> Optional[str]:
    """Day / month / year columns -> ISO date. Any part missing -> None."""
    d, m, y = (_excel_serial_parts(row.get(c)) for c in rule.columns)
    if d is None or m is None or y is None:
        return None
    if y < 100:
        y += 2000
    try:
        return dt.date(y, m, d).isoformat()
    except ValueError:
        return None


def project_row(row: Dict[str, Any], source: Source, meta: dict,
                file_date: Optional[str]) -> Dict[str, Any]:
    """Keep only the spec's columns. Everything else in `row` is dropped here.

    Output keys are exactly: platform, the SALES_COLUMNS (absent ones null),
    source_file and drive_file_id. No raw_data - the requirement is that only
    the named columns reach the database.
    """
    out: Dict[str, Any] = {"platform": source.platform}
    # Every spec column is always present. A platform the spec gives no
    # sub-city for (Amazon Vendor Central) still yields sub_city=None, so all
    # rows share one shape and the hash never depends on which keys exist.
    for target in SALES_COLUMNS:
        rule = source.select.get(target)
        if rule is None:
            out[target] = None
        elif rule == FILENAME:
            out[target] = file_date
        elif isinstance(rule, ComposeDate):
            out[target] = compose_date(row, rule)
        else:
            out[target] = row.get(rule)
    out["source_file"] = meta["name"]
    out["drive_file_id"] = meta["id"]
    return out


# --------------------------------------------------------------------------- #
# Flipkart preamble                                                            #
# --------------------------------------------------------------------------- #

def _first_lines(path: str, count: int = 6) -> List[List[str]]:
    rows = []
    try:
        with open(path, "r", encoding="utf-8-sig", errors="replace") as fh:
            for _ in range(count):
                line = fh.readline()
                if not line:
                    break
                rows.append(next(csv.reader([line]), []))
    except OSError:
        pass
    return rows


def detect_csv_header_row(path: str) -> int:
    """Find the header line, skipping any 'Start Time'/'End Time' preamble.

    Flipkart's daily exports carry that preamble; the same reports pulled as
    back-history do not. Detecting it per file means one source handles both,
    instead of needing a near-duplicate registry entry per report type.
    """
    rows = _first_lines(path)
    for i, parts in enumerate(rows):
        if len(parts) >= 2 and re.match(r"^(start|end)\s*time$",
                                        parts[0].strip(), re.I):
            continue
        if len(parts) >= 2:
            return i
    return 0


def csv_preamble_date(path: str) -> Optional[str]:
    """Read 'Start Time, 2026-07-06 00:00:00' from a Flipkart daily export."""
    for parts in _first_lines(path):
        if len(parts) >= 2 and re.match(r"^start\s*time$", parts[0].strip(), re.I):
            return parse_date(parts[1])
    return None


# --------------------------------------------------------------------------- #
# Reading                                                                      #
# --------------------------------------------------------------------------- #

def _sheet_name(xl: pd.ExcelFile, wanted: Optional[str]) -> Optional[str]:
    """Resolve a sheet name tolerantly.

    Amazon truncates sheet names to 31 chars ('Sponsored_Products_Advertised_p'),
    and a sheet a source expects may simply be absent from an older month.
    """
    if wanted is None:
        return xl.sheet_names[0]
    for name in xl.sheet_names:
        if name.strip().lower() == wanted.strip().lower():
            return name
    for name in xl.sheet_names:
        a, b = name.strip().lower(), wanted.strip().lower()
        if a.startswith(b[:28]) or b.startswith(a[:28]):
            return name
    # A single-sheet workbook can only be the data, whatever the tab is called:
    # Zepto's back-history names it 'DATA' while the dailies use 'Sheet1'.
    # Multi-sheet workbooks get no such benefit of the doubt - guessing there
    # would load one Blinkit ad format's rows into another format's table.
    if len(xl.sheet_names) == 1:
        return xl.sheet_names[0]
    return None


def _frame_to_rows(frame: pd.DataFrame, columns: List[str]) -> Iterator[Dict[str, Any]]:
    frame = frame.where(pd.notna(frame), None)
    for record in frame.itertuples(index=False, name=None):
        row = {}
        for col, val in zip(columns, record):
            cleaned = clean_value(val)
            if cleaned is not None:
                row[col] = cleaned
        if row:
            yield row


def read_excel(path: str, source: Source) -> Iterator[List[Dict[str, Any]]]:
    try:
        xl = pd.ExcelFile(path)
    except Exception as exc:  # noqa: BLE001
        logger.error("[READ] cannot open %s: %s", os.path.basename(path), exc)
        return

    sheet = _sheet_name(xl, source.sheet)
    if sheet is None:
        logger.info("[READ] %s has no sheet '%s' (has %s) - skipped",
                    os.path.basename(path), source.sheet, xl.sheet_names)
        return

    raw = xl.parse(sheet, header=None, dtype=object)
    if raw.empty:
        return
    if source.header_row is not None:
        hrow = source.header_row
    else:
        hrow = None
        if source.projected:
            hrow = find_anchored_header(
                [raw.iloc[i].tolist() for i in range(min(25, len(raw)))],
                source.anchor_columns)
            if hrow is None:
                raise WrongShape(
                    f"{os.path.basename(path)}: none of the first 25 rows "
                    f"contains the mapped columns {source.anchor_columns}")
        if hrow is None:
            hrow = find_header_row(raw)
    if hrow >= len(raw):
        return
    columns = normalize_columns(raw.iloc[hrow].tolist())
    body = raw.iloc[hrow + 1:]
    if body.empty:
        return

    batch: List[Dict[str, Any]] = []
    for row in _frame_to_rows(body, columns):
        batch.append(row)
        if len(batch) >= BATCH:
            yield batch
            batch = []
    if batch:
        yield batch


def read_csv(path: str, source: Source) -> Iterator[List[Dict[str, Any]]]:
    if source.header_row is not None:
        skip = source.header_row
    else:
        skip = None
        if source.projected:
            skip = find_anchored_header(_first_lines(path, 25), source.anchor_columns)
            if skip is None:
                raise WrongShape(
                    f"{os.path.basename(path)}: none of the first 25 lines "
                    f"contains the mapped columns {source.anchor_columns}")
        if skip is None:
            skip = detect_csv_header_row(path)
    try:
        reader = pd.read_csv(
            path, skiprows=skip, dtype=object, chunksize=BATCH,
            encoding="utf-8", encoding_errors="replace",
            on_bad_lines="warn", low_memory=False,
        )
    except Exception as exc:  # noqa: BLE001
        logger.error("[READ] cannot open %s: %s", os.path.basename(path), exc)
        return

    columns: Optional[List[str]] = None
    for chunk in reader:
        if columns is None:
            columns = normalize_columns(list(chunk.columns))
        rows = list(_frame_to_rows(chunk, columns))
        if rows:
            yield rows


def read_file(path: str, source: Source, meta: dict) -> Iterator[List[Dict[str, Any]]]:
    """Yield batches of normalized rows, each tagged with provenance.

    Every row gets source_file / drive_file_id / platform / report_date so the
    table is self-describing and a bad file can be found and re-loaded.
    """
    ext = os.path.splitext(meta["name"])[1].lower()
    if ext in (".xlsx", ".xls", ".xlsm"):
        stream = read_excel(path, source)
    elif ext in (".csv", ".txt", ".tsv"):
        stream = read_csv(path, source)
    else:
        logger.info("[READ] unsupported type %s: %s", ext, meta["name"])
        return

    file_date, grain = date_from_filename(meta["name"])
    if ext in (".csv", ".txt", ".tsv"):
        # The preamble timestamp beats the file name when both are present.
        preamble = csv_preamble_date(path)
        if preamble:
            file_date, grain = preamble, "day"

    # Which in-file column, if any, carries the row's own date.
    date_col = source.date_columns[0] if source.date_columns else None

    for batch in stream:
        for row in batch:
            row["source_file"] = meta["name"]
            row["drive_file_id"] = meta["id"]
            row["source_path"] = meta["path"]

            platform = None
            for key in ("platform", "platform_name"):
                if key in row:
                    platform = canonical_platform(row[key])
                    row[key] = platform
                    break
            row["platform"] = platform or source.platform or None

            # Which date wins depends on the source, and getting this backwards
            # is silently destructive. Big Basket's campaign reports declare
            # `campaign_creation_date` in date_columns purely so it is typed as
            # a date - it is a static attribute, the same in every daily file.
            # Treating it as the report date stamped every snapshot with the
            # campaign's creation date, so two days whose metrics happened to
            # match became identical rows and collapsed on the hash: a wrong
            # date and 511 lost rows in one table alone.
            #
            # So: when the file name carries the date, it wins. The in-file
            # column is only the fallback, which is what the bulk back-history
            # files need - their names are ranges, so file_date is None and
            # their own Date column is the only per-row date available.
            if source.date_from_filename:
                report_date = file_date
                if report_date is None and date_col:
                    report_date = parse_date(row.get(date_col))
            else:
                report_date = parse_date(row.get(date_col)) if date_col else None
                if report_date is None:
                    report_date = file_date

            row["report_date"] = report_date
            row["date_grain"] = grain if report_date == file_date else "day"

        if source.projected:
            # Replace every row with its projection. Done last so the date
            # logic above has already resolved file_date for FILENAME rules.
            batch[:] = [project_row(r, source, meta, file_date) for r in batch]
        yield batch
