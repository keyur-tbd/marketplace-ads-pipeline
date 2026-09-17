"""Publish the GL vs MIS sales reconciliation to its Google Sheet, split by month.

    python -m mp.gl_reco_sheet --check             # read the snapshot, write nothing
    python -m mp.gl_reco_sheet --run               # rewrite the sheet
    python -m mp.gl_reco_sheet --create            # make a new sheet, print its id

The reconciliation itself lives in the database (Birbal migration 078,
public.mv_gl_sales_reco, refreshed by pg_cron at 09:25 and 14:25 IST): one row
per document x month comparing the G/L sales accounts -- the figure in the
financial statements -- with the management MIS, and a rule-based remark saying
what each difference is for. This module only lays it out for finance, in the
shape of their own "Sales Reco" workbook, with every month in its own column.
The /mis page shows the same numbers as period totals.

The sheet is OUTPUT ONLY: every run clears and rewrites its tabs, so an edit
made in it is lost on the next run. Written as marketing@thebakersdozen.in (the
same token.json as the Drive sync); share the file from that account.

Amounts are ex-GST and SALES-POSITIVE on both sides (a return is negative);
Difference = GL - MIS.
"""
from __future__ import annotations

import argparse
import datetime as dt
import logging
import os
import sys
import time
from collections import defaultdict
from typing import Dict, List, Optional, Sequence

from . import dbadmin
from .drive import PROJECT
from .sheets import _clients
from .sink import load_dotenv

logger = logging.getLogger("mp.gl_reco_sheet")

SHEET_ID = os.environ.get("GL_RECO_SHEET_ID") or "1MuIIoOM3jtezc-6lEZSf1RFMHu_sGa0FwvzjGU5PC_4"
TITLE = "GL vs MIS Sales Reco (warehouse)"
TABS = ["Summary", "Reco", "Partywise provision", "GL by account", "Read me"]
CHUNK = 5000                                  # rows per values.update call

MATCH = "Match"
CHECK_DOC_TYPE = "Check"


def _month_label(m: dt.date) -> str:
    return m.strftime("%b-%y")


def _num(v) -> float:
    return float(v) if v is not None else 0.0


# --------------------------------------------------------------------------- #
# Database                                                                    #
# --------------------------------------------------------------------------- #

def read_snapshot(cur) -> Dict[str, list]:
    cur.execute("""
        select month, document_no, document_type, platform, party, gl_accounts,
               gl_amount, mis_amount, difference, remark, refreshed_at
          from public.mv_gl_sales_reco
         order by month, remark, abs(difference) desc, document_no""")
    reco = cur.fetchall()
    if not reco:
        raise SystemExit("public.mv_gl_sales_reco is empty -- run public.gl_sales_reco_refresh() first")

    cur.execute("""
        select date_trunc('month', g."Posting_Date")::date, g."G_L_Account_No",
               max(a.account_name), max(coalesce(a.account_remark, '')),
               count(*), -sum(g."Amount")
          from public.bc_general_ledger_entries g
          join public.ref_gl_sales_accounts a on a.account_no = g."G_L_Account_No"
         where g."Posting_Date" >= date '2026-04-01'
         group by 1, 2
         order by 2, 1""")
    accounts = cur.fetchall()

    cur.execute("""
        select date_trunc('month', "Posting_Date")::date, count(*)
          from public.bc_general_ledger_entries
         where "Posting_Date" >= date '2026-04-01'
         group by 1 order by 1""")
    gl_rows = cur.fetchall()
    return {"reco": reco, "accounts": accounts, "gl_rows": gl_rows}


# --------------------------------------------------------------------------- #
# Layout                                                                      #
# --------------------------------------------------------------------------- #

def _remark_order(totals: Dict[str, float], remarks: Sequence[str]) -> List[str]:
    """Reco items by size, then the residual check, then Match."""
    def key(r):
        if r == MATCH:
            return (2, 0)
        if r.startswith("MIS allocation residual"):
            return (1, 0)
        return (0, -abs(totals.get(r, 0.0)))
    return sorted(remarks, key=key)


def build_summary(reco, months: List[dt.date], refreshed) -> List[list]:
    gl = defaultdict(float)             # month -> GL total
    mis = defaultdict(float)
    diff = defaultdict(float)           # (remark, month) -> GL - MIS
    rgl = defaultdict(float)            # (remark, month) -> GL
    rmis = defaultdict(float)
    docs = defaultdict(int)
    for (m, doc, _dt, _pl, _party, _acc, g, s, d, remark, _ref) in reco:
        gl[m] += _num(g)
        mis[m] += _num(s)
        diff[(remark, m)] += _num(d)
        rgl[(remark, m)] += _num(g)
        rmis[(remark, m)] += _num(s)
        if doc:
            docs[(remark, m)] += 1
    remarks = sorted({r[9] for r in reco})
    totals = {r: sum(diff[(r, m)] for m in months) for r in remarks}
    order = _remark_order(totals, remarks)

    labels = [_month_label(m) for m in months]
    out = [
        [TITLE],
        [f"Snapshot {refreshed:%d-%b-%Y %H:%M} UTC. Ex-GST, sales positive (a return is negative). "
         "GL = G/L sales accounts by posting month; MIS = mis_sales Net Sales by its own month. "
         "Each line below is GL minus MIS for that reason, so Sales as per GL less the lines = Sales as per MIS."],
        [],
        ["Particulars"] + labels + ["Total"],
        ["Sales as per GL"] + [round(gl[m]) for m in months] + [round(sum(gl.values()))],
    ]
    for r in order:
        if r == MATCH:
            continue
        out.append(["Less: " + r] + [round(diff[(r, m)]) for m in months] + [round(totals[r])])
    out.append(["Sales as per MIS"] + [round(mis[m]) for m in months] + [round(sum(mis.values()))])
    check = [round(gl[m] - sum(diff[(r, m)] for r in remarks) - mis[m]) for m in months]
    out.append(["Check (should be 0)"] + check + [sum(check)])
    out += [[], ["Documents by remark"] + labels + ["Total"]]
    for r in order:
        out.append([r] + [docs[(r, m)] for m in months] + [sum(docs[(r, m)] for m in months)])
    out += [[], ["GL and MIS by remark"], ["Remark", "Month", "Documents", "GL", "MIS", "Difference"]]
    for r in order:
        for m in months:
            g, s = rgl[(r, m)], rmis[(r, m)]
            if docs[(r, m)] or g or s:
                out.append([r, _month_label(m), docs[(r, m)], round(g, 2), round(s, 2), round(g - s, 2)])
    return out


def build_reco(reco) -> List[list]:
    out = [["Month", "Document No", "Document Type", "Platform", "Party", "GL Accounts",
            "GL Amount", "MIS Amount", "Difference", "Remark"]]
    for (m, doc, dtype, plat, party, acc, g, s, d, remark, _ref) in reco:
        out.append([_month_label(m), doc or "", dtype or "", plat or "", party or "", acc or "",
                    round(_num(g), 2), round(_num(s), 2), round(_num(d), 2), remark])
    return out


def build_provisions(reco, months: List[dt.date]) -> List[list]:
    """The MIS amounts no GL entry carries yet, by party and month: the PRN and
    capping provisions and the pending sales return orders."""
    amt = defaultdict(float)
    keys = set()
    for (m, _doc, dtype, plat, party, _acc, _g, s, _d, remark, _ref) in reco:
        if dtype in ("PRN provision", "Capping provision") or remark.startswith("SRO provision"):
            kind = ("SRO" if remark.startswith("SRO provision")
                    else "Capping RTV provision" if dtype == "Capping provision"
                    else "RTV provision (PRN)")
            who = plat or party or "(unmapped)"
            keys.add((kind, who))
            amt[(kind, who, m)] += _num(s)
    out = [["MIS provisions not yet in the GL (negative = reduces sales)"], [],
           ["Provision", "Party"] + [_month_label(m) for m in months] + ["Total"]]
    for kind, who in sorted(keys):
        row = [round(amt[(kind, who, m)], 2) for m in months]
        out.append([kind, who] + row + [round(sum(row), 2)])
    tot = [round(sum(amt[(k, w, m)] for k, w in keys), 2) for m in months]
    out.append(["Total", ""] + tot + [round(sum(tot), 2)])
    return out


def build_accounts(accounts, gl_rows, months: List[dt.date]) -> List[list]:
    by = defaultdict(float)
    names = {}
    for (m, acc, name, remark, _n, amount) in accounts:
        by[(acc, m)] += _num(amount)
        names[acc] = (name, remark)
    out = [["G/L sales accounts, sales positive (credit = +)"], [],
           ["Account", "Name", "Treatment in the reco"] + [_month_label(m) for m in months] + ["Total"]]
    for acc in sorted(names):
        row = [round(by[(acc, m)], 2) for m in months]
        out.append([acc, names[acc][0], names[acc][1] or "Matched against the MIS"] + row + [round(sum(row), 2)])
    tot = [round(sum(by[(a, m)] for a in names), 2) for m in months]
    out.append(["Total", "", ""] + tot + [round(sum(tot), 2)])
    counts = dict(gl_rows)
    out += [[], ["G/L entries loaded (all accounts)", "", ""] + [counts.get(m, 0) for m in months]]
    return out


def build_readme(reco, gl_rows, refreshed) -> List[list]:
    counts = dict(gl_rows)
    thin = [m for m, n in counts.items() if n < 50000]
    lines = [
        ["GL vs MIS Sales Reco -- how to read it"],
        [],
        [f"Snapshot of public.mv_gl_sales_reco taken {refreshed:%d-%b-%Y %H:%M} UTC; refreshed 09:25 and 14:25 IST, "
         "this sheet rewritten after each. Edits made here are overwritten."],
        ["Built by Birbal migrations 077 (what the MIS includes) and 078 (the reco). The /mis page shows the same "
         "numbers as period totals."],
        ["GL: G/L entries on the SALES and SALES RETURN accounts plus Fill Rate Loss Recovery, by posting month, "
         "sign flipped so sales are positive."],
        ["MIS: what each document adds to mis_sales Net Sales (ex-GST), by the MIS's month -- a credit note's "
         "document date, a capping note's sales month."],
        ["Match: GL and MIS agree within Rs 2 for that document and month."],
        ["Provision rows (PRN, capping) carry no document: the MIS takes them, the GL has not booked them. "
         "Pending SROs are listed by SRO number."],
        ["Timing rows net to zero across months: the same document sits in different months on the two sides."],
        ["Unexplained / In GL only / In MIS only rows are the ones to look at."],
    ]
    if thin:
        lines += [[], ["WARNING: the G/L feed has very few entries for "
                       + ", ".join(_month_label(m) for m in sorted(thin))
                       + " -- those months will read as 'In MIS only' until the BC G/L sync has loaded them."]]
    return lines


# --------------------------------------------------------------------------- #
# Sheet                                                                       #
# --------------------------------------------------------------------------- #

def _retry(fn, tries=5):
    for i in range(tries):
        try:
            return fn()
        except Exception as exc:              # quota and 5xx are transient
            if i == tries - 1:
                raise
            wait = 10 * (i + 1)
            logger.warning("sheets call failed (%s), retrying in %ss", exc, wait)
            time.sleep(wait)


def ensure_tabs(sheets, sheet_id: str) -> Dict[str, int]:
    meta = _retry(lambda: sheets.spreadsheets().get(spreadsheetId=sheet_id,
                                                    fields="sheets.properties").execute())
    have = {s["properties"]["title"]: s["properties"]["sheetId"] for s in meta["sheets"]}
    add = [{"addSheet": {"properties": {"title": t}}} for t in TABS if t not in have]
    if add:
        _retry(lambda: sheets.spreadsheets().batchUpdate(spreadsheetId=sheet_id,
                                                         body={"requests": add}).execute())
        return ensure_tabs(sheets, sheet_id)
    return have


def write_tab(sheets, sheet_id: str, tab_id: int, tab: str, rows: List[list], freeze_rows: int,
              header_row: int, number_from_col: int):
    width = max(len(r) for r in rows)
    reqs = [
        {"updateCells": {"range": {"sheetId": tab_id}, "fields": "userEnteredValue,userEnteredFormat"}},
        {"updateSheetProperties": {
            "properties": {"sheetId": tab_id,
                           "gridProperties": {"rowCount": max(len(rows) + 10, 100),
                                              "columnCount": max(width + 2, 10),
                                              "frozenRowCount": freeze_rows}},
            "fields": "gridProperties(rowCount,columnCount,frozenRowCount)"}},
    ]
    _retry(lambda: sheets.spreadsheets().batchUpdate(spreadsheetId=sheet_id,
                                                     body={"requests": reqs}).execute())
    for start in range(0, len(rows), CHUNK):
        part = rows[start:start + CHUNK]
        rng = f"'{tab}'!A{start + 1}"
        _retry(lambda: sheets.spreadsheets().values().update(
            spreadsheetId=sheet_id, range=rng, valueInputOption="RAW",
            body={"values": part}).execute())
    fmt = []
    if number_from_col < width:               # a text-only tab has no number columns
        fmt.append(
            {"repeatCell": {"range": {"sheetId": tab_id, "startRowIndex": header_row,
                                      "startColumnIndex": number_from_col},
                            "cell": {"userEnteredFormat": {"numberFormat": {"type": "NUMBER", "pattern": "#,##0;[Red]-#,##0"}}},
                            "fields": "userEnteredFormat.numberFormat"}})
    fmt += [
        {"repeatCell": {"range": {"sheetId": tab_id, "startRowIndex": header_row - 1 if header_row else 0,
                                  "endRowIndex": header_row if header_row else 1},
                        "cell": {"userEnteredFormat": {"textFormat": {"bold": True}}},
                        "fields": "userEnteredFormat.textFormat.bold"}},
        {"autoResizeDimensions": {"dimensions": {"sheetId": tab_id, "dimension": "COLUMNS",
                                                 "startIndex": 0, "endIndex": width}}},
    ]
    _retry(lambda: sheets.spreadsheets().batchUpdate(spreadsheetId=sheet_id,
                                                     body={"requests": fmt}).execute())
    logger.info("%s: %d rows", tab, len(rows))


def create(sheets) -> str:
    body = {"properties": {"title": TITLE}, "sheets": [{"properties": {"title": t}} for t in TABS]}
    res = _retry(lambda: sheets.spreadsheets().create(body=body, fields="spreadsheetId").execute())
    return res["spreadsheetId"]


def run(write: bool, sheet_id: str) -> int:
    conn = dbadmin.connect(timeout=300)
    cur = conn.cursor()
    cur.execute("set statement_timeout = '600000'")
    snap = read_snapshot(cur)
    conn.close()

    reco = snap["reco"]
    refreshed = max(r[10] for r in reco)
    months = sorted({r[0] for r in reco})
    tabs = {
        "Summary": (build_summary(reco, months, refreshed), 4, 4, 1),
        "Reco": (build_reco(reco), 1, 1, 6),
        "Partywise provision": (build_provisions(reco, months), 3, 3, 2),
        "GL by account": (build_accounts(snap["accounts"], snap["gl_rows"], months), 3, 3, 3),
        "Read me": (build_readme(reco, snap["gl_rows"], refreshed), 1, 0, 99),
    }
    for name, (rows, *_rest) in tabs.items():
        logger.info("%s: %d rows built", name, len(rows))
    if not write:
        return 0
    if not sheet_id:
        raise SystemExit("No sheet id: set GL_RECO_SHEET_ID (python -m mp.gl_reco_sheet --create makes one).")
    sheets, _drive = _clients()
    ids = ensure_tabs(sheets, sheet_id)
    for name, (rows, freeze, header, numcol) in tabs.items():
        write_tab(sheets, sheet_id, ids[name], name, rows, freeze, header, numcol)
    logger.info("https://docs.google.com/spreadsheets/d/%s rewritten from the %s snapshot",
                sheet_id, refreshed)
    return 0


def main(argv: Optional[Sequence[str]] = None) -> int:
    load_dotenv(os.path.join(PROJECT, ".env"))
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    mode = ap.add_mutually_exclusive_group(required=True)
    mode.add_argument("--check", action="store_true", help="read and lay out, write nothing")
    mode.add_argument("--run", action="store_true", help="rewrite the sheet")
    mode.add_argument("--create", action="store_true", help="create a new sheet and print its id")
    ap.add_argument("--sheet-id", default=os.environ.get("GL_RECO_SHEET_ID", SHEET_ID))
    a = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    if a.create:
        sheets, _drive = _clients()
        print(create(sheets))
        return 0
    return run(write=a.run, sheet_id=a.sheet_id)


if __name__ == "__main__":
    sys.exit(main())
