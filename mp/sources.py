"""Registry of every Drive report type that flows into Supabase.

One entry per report type -> one Supabase table, following the same
one-table-per-source convention the GRN schedulers use.

Ordering follows the numbered Drive folders, because the user asked for the
data to be processed in the sequence the folder names give:

    1) Discount Split Data     -> processed/"split" offers data
    2) Off Invoice Split Data  -> processed/"split" off-invoice data
    3) Ads Split Data          -> processed/"split" ads spend data
    4) Ads Raw Data            -> per-platform raw platform exports

"split" files are the processed end files; everything under "4) Ads Raw Data"
is the untouched platform export. Both are loaded: the split tables are the
reporting layer, the raw tables are the audit/drill-down layer.

Two Drive roots are walked (see ROOTS). The ads / offers / off-invoice sources
live under "Market Place Data". The sales sources read the uploader's own
"New Power BI Format Daily Sales" folder directly, so nobody has to copy files
into a second folder for Supabase; the old "5) Sales Raw Data" copy is frozen
and excluded. Per-root folder names in ROOTS are never read, at any depth.

Columns are NOT hand-listed here. Headers vary between months and platforms
change them without notice, so `discover.py` samples real files and derives the
typed columns; anything it has not seen still lands in raw_data (jsonb) and is
queryable as raw_data->>'key'. What this file pins down is the part that cannot
be inferred: which folder, which sheet, where the header row is, where the
report date comes from, and which numeric-looking columns must stay text.
"""

from typing import Dict, List, Optional, Sequence

#: Drive folder id of "Market Place Data" (ads, offers, off-invoice).
ROOT_FOLDER_ID = "188ROEXBYrkMUybnrvJwlkLSxxLS_cl5e"

#: Drive folder id of "New Power BI Format Daily Sales". This is the folder the
#: uploader already maintains for Power BI; the sales sources read it directly.
SALES_ROOT_FOLDER_ID = "1ylLL0RFDXBZA28ttErGsy8beLUClWnw-"

#: Every root that is walked, with the folder NAMES that are skipped at index
#: time under that root, at any depth (compared case-insensitively). A skipped
#: folder's files never appear in the file list at all.
#:
#: Under the sales root the skipped folders are the history Power BI keeps that
#: Supabase does not need: the "2024" year folders (monthly rollups from before
#: the daily exports), "Old Data" (duplicate copies of the 2025 Amazon rollups)
#: and "All Master" (a SKU master workbook, not sales).
ROOTS: Dict[str, List[str]] = {
    ROOT_FOLDER_ID: ["Not to be uploaded in Supabase",
                     # Frozen copy of the sales folder; superseded by
                     # SALES_ROOT_FOLDER_ID on 2026-09-07.
                     "5) Sales Raw Data"],
    SALES_ROOT_FOLDER_ID: ["2024", "Old Data", "All Master"],
}


def excluded_folders(root_id: str) -> List[str]:
    return [x.lower() for x in ROOTS.get(root_id, [])]


#: Files that are Windows/Drive noise rather than data.
JUNK_NAMES = {"desktop.ini", ".ds_store", "thumbs.db"}
#: ...and by extension: a stray Windows shortcut sits in BB Instant Nov-25.
JUNK_SUFFIXES = (".lnk", ".ini", ".tmp")

#: Platform aliases. The user's instruction: wherever a file says "Scootsy",
#: it means Instamart. Applied to platform-bearing values, not just paths.
#:
#: This also absorbs spelling drift in the split files, which are typed by hand:
#: 'FLIPKART MIINUTES' appears in the Ads split and 'FLIPKART MINUTES' in the
#: Discount split, and without this they would land as two platforms.
#:
#: Flipkart Minutes is deliberately NOT folded into Flipkart - it is the quick
#: commerce channel, separate from the marketplace ads in 4) Ads Raw Data.
PLATFORM_ALIASES = {
    "scootsy": "INSTAMART",
    "swiggy instamart": "INSTAMART",
    "instamart": "INSTAMART",
    "amazon fresh": "AMAZON",
    "amazon": "AMAZON",
    "blinkit": "BLINKIT",
    "zepto": "ZEPTO",
    "flipkart": "FLIPKART",
    "flipkart minutes": "FLIPKART MINUTES",
    "flipkart miinutes": "FLIPKART MINUTES",
    "flipkart minute": "FLIPKART MINUTES",
    "big basket": "BIGBASKET",
    "bigbasket": "BIGBASKET",
    "bb": "BIGBASKET",
    "milk basket": "MILKBASKET",
    "milkbasket": "MILKBASKET",
    "first club": "FIRSTCLUB",
    "firstclub": "FIRSTCLUB",
}

#: Columns that look numeric but are identifiers. Forced to text so long ids do
#: not become 3.23E+12 or lose leading zeros. Matched on the normalized name.
ID_COLUMNS = {
    "item_id", "sku_code", "campaign_id", "ad_group_id", "adgroup_id",
    "brand_id", "fsn_id", "advertised_asin", "asin", "portfolio_id",
    "manufacturer_id", "product_id", "keyword_id", "booking_id",
}


class Source:
    """One report type: where it lives in Drive and how to read it.

    table       Supabase table name.
    root        Drive folder id the source is read from (a key of ROOTS).
    folder      Drive path prefix, relative to that root.
    sheet       Worksheet to read for spreadsheets. None -> first sheet.
                A workbook with several meaningful sheets gets one Source per
                sheet (Blinkit), so each lands in its own table.
    header_row  0-based row holding the column names, or None to auto-detect.
                Leave it None for CSVs: Flipkart's preamble is two lines on most
                exports but four on the newer keyword ones (two "Start Time"/
                "End Time" lines plus two explanatory notes), so pinning a row
                number silently truncates those files to a single row.
    date_from_filename
                True when the export has no date column and the file name is the
                only date (Big Basket, Zepto, Flipkart dailies). The parsed date
                goes into `report_date`.
    date_columns
                Normalized names to coerce to date, on top of what discovery
                infers. Also the first of these is copied into `report_date`.
    text_columns
                Extra columns to force to text.
    platform    Platform tag written into every row of this source.
    exclude     Path fragments under `folder` to skip.
    select      Projection mode. {target_column: source_column | FILENAME |
                ComposeDate(...)}. When set, ONLY these columns are read - every
                other column in the file is dropped, there is no raw_data, and
                the table schema is fixed (see `fixed_types`) rather than
                discovered. Header detection anchors on the mapped source
                columns, so a metadata preamble above the header is skipped
                automatically. Used for 5) Sales Raw Data, whose spec workbook
                names exactly six columns per platform.
    """

    def __init__(self, key: str, order: str, table: str, folder: str,
                 platform: str, report: str,
                 sheet: Optional[str] = None,
                 header_row: Optional[int] = None,
                 date_from_filename: bool = False,
                 date_columns: Sequence[str] = (),
                 text_columns: Sequence[str] = (),
                 exclude: Sequence[str] = (),
                 note: str = "",
                 select: Optional[Dict[str, object]] = None,
                 only: Sequence[str] = (),
                 root: str = ROOT_FOLDER_ID):
        self.key = key
        self.order = order
        self.table = table
        self.root = root
        self.folder = folder
        self.platform = platform
        self.report = report
        self.sheet = sheet
        self.header_row = header_row
        self.date_from_filename = date_from_filename
        self.date_columns = list(date_columns)
        self.text_columns = list(text_columns)
        self.exclude = list(exclude)
        self.note = note
        self.select = dict(select) if select else None
        #: Path fragments a file MUST contain to belong here. Lets two sources
        #: share a folder that mixes two report shapes (Vendor Central dailies
        #: alongside its 'Power BI Upload' monthlies).
        self.only = list(only)

    @property
    def projected(self) -> bool:
        return self.select is not None

    @property
    def anchor_columns(self) -> List[str]:
        """Plain source columns the header row must contain (projection mode)."""
        if not self.select:
            return []
        out = []
        for rule in self.select.values():
            if isinstance(rule, str) and rule != FILENAME:
                out.append(rule)
            elif isinstance(rule, ComposeDate):
                out.extend(rule.columns)
        return out

    def fixed_types(self) -> Dict[str, str]:
        """Column types for a projected source: the target columns, typed by
        SALES_TYPES, plus the minimal provenance the ledger and dedup need."""
        if not self.select:
            return {}
        out = {"platform": "text"}
        for target in SALES_COLUMNS:
            out[target] = SALES_TYPES.get(target, "text")
        out["source_file"] = "text"
        out["drive_file_id"] = "text"
        return out

    def __repr__(self) -> str:
        return f"<Source {self.key} -> {self.table}>"


#: Projection sentinel: the value comes from the file name, not a column.
FILENAME = "@filename"


class ComposeDate:
    """A date assembled from separate day / month / year columns.

    Amazon PI exports the order date as three integer columns
    (orderDay=27, orderMonth=7, orderYear=2026) rather than one field.
    """

    def __init__(self, day: str, month: str, year: str):
        self.columns = [day, month, year]

    def __repr__(self) -> str:
        return f"ComposeDate({', '.join(self.columns)})"


#: The unified sales columns, in table order, straight from the spec workbook:
#: SKU Code, SKU Name, Date, QTY, Sub City.
SALES_COLUMNS = ["sku_code", "sku_name", "sale_date", "qty", "sub_city"]
#: Types of those columns. Everything not listed is text.
SALES_TYPES = {"sale_date": "date", "qty": "numeric"}


# --------------------------------------------------------------------------- #
# 1-3: the processed "split" files. These are the reporting layer.             #
# --------------------------------------------------------------------------- #

_SPLIT: List[Source] = [
    Source(
        key="discount_split", order="1",
        table="mp_discount_split",
        folder="1) Discount Split Data/Discounting Data",
        platform="", report="Discount / Offers split",
        sheet="Discount Data",
        date_columns=["month"],
        text_columns=["item_id", "sku_name", "category", "platform"],
        note="Offers/discount spend per SKU per month per platform. Some months "
             "carry a blank leading row, so the header row is auto-detected.",
    ),
    Source(
        key="off_invoice_split", order="2",
        table="mp_off_invoice_split",
        folder="2) Off Invoice Split Data/Off Invoice Data",
        platform="", report="Off-Invoice split",
        sheet="Off Invoice Data",
        date_columns=["month"],
        text_columns=["sku_code", "sku_name", "category", "platform"],
    ),
    Source(
        key="ads_split", order="3",
        table="mp_ads_split",
        folder="3) Ads Split Data/Ads Data",
        platform="", report="Ads spend split",
        sheet="ADs Data",
        date_columns=["month"],
        text_columns=["sku_name", "category", "platform"],
        note="No item_id in this set - SKU Name is the only product key.",
    ),
]

# --------------------------------------------------------------------------- #
# 4: raw platform exports, by platform.                                        #
# --------------------------------------------------------------------------- #

_RAW = "4) Ads Raw Data"

_AMAZON: List[Source] = [
    Source("amz_advertised_product", "4.1", "amz_sp_advertised_product",
           f"{_RAW}/Amazon/Amazon Visibility Data/SP Advertised Product",
           "AMAZON", "SP advertised product", date_columns=["date"]),
    Source("amz_budget", "4.1", "amz_sp_budget_report",
           f"{_RAW}/Amazon/Amazon Visibility Data/SP Buduget Report",
           "AMAZON", "SP budget", date_columns=["date", "start_date", "end_date"],
           note="Folder name is misspelled in Drive ('Buduget'); kept verbatim "
                "so the path matches."),
    Source("amz_campaign", "4.1", "amz_sp_campaign_report",
           f"{_RAW}/Amazon/Amazon Visibility Data/SP Campaign Report",
           "AMAZON", "SP campaign", date_columns=["start_date", "end_date"],
           note="Has no single Date column - the range is Start/End Date, and "
                "report_date takes Start Date."),
    Source("amz_campaign_daily", "4.1", "amz_sp_campaign_daily",
           f"{_RAW}/Amazon/Amazon Visibility Data/SP Campaign report Daily",
           "AMAZON", "SP campaign daily", date_columns=["date"]),
    Source("amz_placement", "4.1", "amz_sp_placement_report",
           f"{_RAW}/Amazon/Amazon Visibility Data/SP Placement Report",
           "AMAZON", "SP placement", date_columns=["date"]),
    Source("amz_search_term", "4.1", "amz_sp_search_term_report",
           f"{_RAW}/Amazon/Amazon Visibility Data/SP Search Term Report",
           "AMAZON", "SP search term", date_columns=["date"]),
    Source("amz_search_term_impression", "4.1", "amz_search_term_impression",
           f"{_RAW}/Amazon/Amazon Visibility Data/Search term Impression",
           "AMAZON", "Search term impression share", date_columns=["date"]),
    Source("amz_kw_type", "4.1", "amz_keyword_type_master",
           f"{_RAW}/Amazon/Amazon Visibility Data/Master",
           "AMAZON", "Keyword type master", sheet="Export", header_row=1,
           note="Row 0 is a stray 'Month' label above the real header."),
]

_BIGBASKET: List[Source] = [
    Source("bb_auction", "4.2", "bb_auction_level",
           f"{_RAW}/Big Basket/BB Visibility Data/Auction Level Report",
           "BIGBASKET", "Auction booking performance",
           sheet="Auction Booking Perf Report",
           date_columns=["date", "flight_start_date", "flight_end_date"]),
    Source("bb_campaign", "4.2", "bb_campaign_level",
           f"{_RAW}/Big Basket/BB Visibility Data/Campaign Level Data",
           "BIGBASKET", "Campaign performance",
           sheet="Campaign Performance Report", date_from_filename=True,
           date_columns=["campaign_creation_date"],
           note="No date inside the file - the file name (08-Aug-26.xlsx) is "
                "the report date."),
    Source("bb_campaign_awareness", "4.2", "bb_campaign_level_awareness",
           f"{_RAW}/Big Basket/BB Visibility Data/Campaign Level Data Awareness",
           "BIGBASKET", "Campaign performance (awareness)",
           sheet="Campaign Performance Report", date_from_filename=True,
           date_columns=["campaign_creation_date", "campaign_end_date"],
           note="Same sheet name as Campaign Level Data but a different column "
                "set, so it gets its own table."),
    Source("bb_shopper_search", "4.2", "bb_shopper_level_search",
           f"{_RAW}/Big Basket/BB Visibility Data/Shopper Level Search",
           "BIGBASKET", "Search query performance",
           sheet="Search Query Performance Report", date_from_filename=True),
]

# Blinkit ships one workbook per month with several sheets; each sheet is a
# different ad format with its own columns, so each becomes its own table.
_BLINKIT_DURATION_SHEETS = [
    ("mtd_claimables", "MTD Claimables", "blinkit_mtd_claimables", True),
    ("keyword_targeting", "Keyword Targeting", "blinkit_keyword_targeting", False),
    ("category_targeting", "Category Targeting", "blinkit_category_targeting", False),
    ("listing_spotlight", "Listing Spotlight", "blinkit_listing_spotlight", False),
    ("product_recommendation", "Product Recommendation",
     "blinkit_product_recommendation", False),
    ("visual_diy", "Visual DIY", "blinkit_visual_diy", False),
    ("product_shelf", "Product Shelf", "blinkit_product_shelf", False),
]

_BLINKIT_VISIBILITY_SHEETS = [
    ("masthead", "MASTHEAD", "blinkit_vis_masthead"),
    ("banner_listing", "BANNER_LISTING", "blinkit_vis_banner_listing"),
    ("product_listing", "PRODUCT_LISTING", "blinkit_vis_product_listing"),
    ("product_recommendation", "PRODUCT_RECOMMENDATION",
     "blinkit_vis_product_recommendation"),
    ("product_shelf", "PRODUCT_SHELF", "blinkit_vis_product_shelf"),
]

_BLINKIT: List[Source] = [
    Source(f"blinkit_dur_{k}", "4.3", table,
           f"{_RAW}/Blinkit/Blinkit Campaign Duration Data",
           "BLINKIT", f"Campaign duration - {sheet}",
           sheet=sheet,
           date_from_filename=month_only,
           date_columns=[] if month_only else ["date_ist"],
           note="MTD Claimables is a month total with no per-day column, so its "
                "report_date comes from the file name."
                if month_only else "")
    for k, sheet, table, month_only in _BLINKIT_DURATION_SHEETS
] + [
    Source(f"blinkit_vis_{k}", "4.3", table,
           f"{_RAW}/Blinkit/Blinkit Visibility Data",
           "BLINKIT", f"Visibility - {sheet}",
           sheet=sheet, date_columns=["date"])
    for k, sheet, table in _BLINKIT_VISIBILITY_SHEETS
]

# Flipkart splits everything into PCA and PLA, each with the same three report
# shapes. The daily CSVs carry two preamble lines before the header.
_FLIPKART: List[Source] = []
for _prog in ("PCA", "PLA"):
    _lp = _prog.lower()
    _FLIPKART += [
        Source(f"fk_{_lp}_consolidated", "4.4", f"fk_{_lp}_consolidated_fsn",
               f"{_RAW}/Flipkart/Flipkart Visibility Data/{_prog}/"
               "Monthly - Consolidated FSN report",
               "FLIPKART", f"{_prog} consolidated FSN",
               date_from_filename=True, date_columns=["date"]),
        Source(f"fk_{_lp}_keyword", "4.4", f"fk_{_lp}_keyword",
               f"{_RAW}/Flipkart/Flipkart Visibility Data/{_prog}/"
               "Monthly - Keyword wise Format",
               "FLIPKART", f"{_prog} keyword wise",
               date_from_filename=True, date_columns=["date"]),
        Source(f"fk_{_lp}_placement", "4.4", f"fk_{_lp}_placement",
               f"{_RAW}/Flipkart/Flipkart Visibility Data/{_prog}/"
               "Monthly - Placement report",
               "FLIPKART", f"{_prog} placement",
               date_from_filename=True, date_columns=["date"]),
    ]

# date_columns=["date"] matters for the bulk back-history files: their names
# are ranges ('keyword Wise - Feb to Dec-25.csv') so there is no date to take
# from the file name, and without this every one of their rows lands with a null
# report_date. The dailies have no Date column and fall back to the file name.

# Back-history for Flipkart lives under 'PLA/Feb-25 To Dec-25 Data/<report>/'.
# files_for() matches folder segments as an ordered subsequence, so those files
# land in the same table as the dailies; readers detect the missing preamble
# per file, so no separate source is needed for them.

_INSTAMART: List[Source] = [
    Source("instamart_visibility", "4.5", "instamart_ads_performance",
           f"{_RAW}/Instamart/Instamart Visibility Data",
           "INSTAMART", "Campaign/keyword performance",
           date_columns=["metrics_date", "start_date", "end_date"],
           note="Largest source by far (~3.5 GB of monthly CSVs). Streamed in "
                "chunks rather than read whole."),
]

_ZEPTO: List[Source] = [
    Source("zepto_visibility", "4.6", "zepto_campaign_performance",
           f"{_RAW}/Zepto/Zepto Visibility Data",
           "ZEPTO", "Campaign performance", sheet="Sheet1",
           date_from_filename=True,
           note="~1073 one-day workbooks with no date column; the file name "
                "(19-Jan-26.xlsx) is the report date."),
]

# --------------------------------------------------------------------------- #
# 5: Sales raw exports -> ONE table, six columns, per the spec workbook.       #
# --------------------------------------------------------------------------- #
#
# "Sales Column Name for All Platform.xlsx" names, for each platform, which
# source column feeds each of: SKU Code, SKU Name, Date, QTY, Sub City. Only
# those are loaded. Everything else in the files - MRP, GMV, categories, store
# ids, brand, EAN - is dropped at read time and never reaches Postgres.
#
# `platform` is the spec's own label ("BB Gamma Sales", "Instamart", ...) so the
# table matches the spec exactly. Scootsy is Instamart, per the standing rule.

SALES_TABLE = "mp_sales"


def _sales(key, order, folder, platform, report, select, **kw):
    """A sales source. Folders are relative to the Power BI sales root, whose
    layout is <platform>/[<report>/]<year>/<Mon-YY>/<file>. The year folder
    sits between the report folder and the files, which the ordered-subsequence
    folder match in drive.files_for takes in its stride."""
    return Source(key, order, SALES_TABLE, folder, platform, report,
                  select=select, root=SALES_ROOT_FOLDER_ID, **kw)


_SALES_SOURCES: List[Source] = [
    # -- Amazon PI: two category folders, same shape. Date is day/month/year.
    _sales("sales_amazon_pi_bakery", "5.1", "Amazon PI/Grocery - Bakery",
           "Amazon PI", "PI grocery sales - bakery",
           {"sku_code": "asin", "sku_name": "itemname",
            "sale_date": ComposeDate("orderday", "ordermonth", "orderyear"),
            "qty": "netunits", "sub_city": "city"}),
    _sales("sales_amazon_pi_snacks", "5.1", "Amazon PI/Grocery - Snacks Food",
           "Amazon PI", "PI grocery sales - snacks",
           {"sku_code": "asin", "sku_name": "itemname",
            "sale_date": ComposeDate("orderday", "ordermonth", "orderyear"),
            "qty": "netunits", "sub_city": "city"}),

    # -- Amazon Vendor Central: daily workbooks with a metadata row above the
    #    header. The header anchors on 'asin', so that row is skipped. The spec
    #    lists no sub city for this platform.
    _sales("sales_amazon_vendor", "5.2",
           "Amazon Vendor Central/Amazon Daily Sales",
           "Amazon Vendor Central", "Vendor Central shipped units",
           {"sku_code": "asin", "sku_name": "product_title",
            "sale_date": FILENAME, "qty": "shipped_units"},
           exclude=["Power BI Upload"]),
    # The 'Power BI Upload' monthly files are a different report (ordered
    # units by city). Not in the spec; mapped by analogy so the 2025 history
    # before the daily exports is not lost. CONFIRM this mapping.
    _sales("sales_amazon_vendor_powerbi", "5.2",
           "Amazon Vendor Central/Amazon Last Year Sales",
           "Amazon Vendor Central", "Vendor Central monthly (Power BI upload)",
           {"sku_code": "asin", "sku_name": "item_name",
            "sale_date": "order_day", "qty": "finla_qty", "sub_city": "city"},
           only=["Power BI Upload"],
           note="Inferred mapping - not in the spec workbook. 'ordered_units' "
                "is empty in these files; 'finla_qty' is the populated one."),

    # -- Big Basket: three daily report shapes, plus monthly Power BI rollups.
    _sales("sales_bb_daily", "5.3", "Big Basket/BB Daily Sales",
           "BB Daily", "BB daily sales",
           {"sku_code": "source_product_id", "sku_name": "sku_name",
            "sale_date": FILENAME, "qty": "quantity", "sub_city": "city_name"}),
    _sales("sales_bb_gamma", "5.3", "Big Basket/BB Gamma Sales",
           "BB Gamma Sales", "BB gamma sales",
           {"sku_code": "source_sku_id", "sku_name": "sku_description",
            "sale_date": FILENAME, "qty": "total_quantity",
            "sub_city": "source_city_name"}),
    _sales("sales_bb_instant", "5.3", "Big Basket/BB Instant Sales",
           "BB Instant Sales", "BB instant sales",
           {"sku_code": "source_sku_id", "sku_name": "sku_description",
            "sale_date": FILENAME, "qty": "quantity", "sub_city": "location_city"}),
    _sales("sales_bb_powerbi", "5.3", "Big Basket/BB Last Year Sales",
           "BB Daily", "BB monthly (Power BI upload), Apr-Sep 25",
           {"sku_code": "sku", "sku_name": "product_name", "sale_date": "date",
            "qty": "quantity", "sub_city": "sub_city"},
           note="Inferred mapping - not in the spec workbook. Back-history for "
                "the months before the daily exports begin."),

    # -- Blinkit
    _sales("sales_blinkit", "5.4", "Blinkit", "Blinkit", "Blinkit daily sales",
           {"sku_code": "item_id", "sku_name": "item_name", "sale_date": "date",
            "qty": "qty_sold", "sub_city": "city_name"}),

    # -- Flipkart: dailies (no date column) plus monthly Power BI rollups.
    _sales("sales_flipkart_daily", "5.5", "Flipkart/Flipkart Daily Sales",
           "Flipkart", "Flipkart daily sales",
           {"sku_code": "sku_code", "sku_name": "sku_name", "sale_date": FILENAME,
            "qty": "quantity", "sub_city": "city"}),
    _sales("sales_flipkart_powerbi", "5.5",
           "Flipkart/Flipkart Last year Sales",
           "Flipkart", "Flipkart monthly (Power BI upload), Apr-Sep 25",
           {"sku_code": "sku_code", "sale_date": "date", "qty": "quantity",
            "sub_city": "city"},
           note="Inferred mapping - not in the spec workbook. These files carry "
                "no SKU name, so sku_name is null for them."),

    # -- Instamart, filed as Scootsy in Drive.
    _sales("sales_instamart", "5.6", "Scootsy", "Instamart", "Instamart daily sales",
           {"sku_code": "item_code", "sku_name": "product_name",
            "sale_date": "ordered_date", "qty": "units_sold", "sub_city": "city"},
           note="Scootsy is Instamart. The folder keeps the old name."),

    # -- Zepto
    _sales("sales_zepto", "5.7", "Zepto", "Zepto", "Zepto daily sales",
           {"sku_code": "sku_number", "sku_name": "sku_name", "sale_date": "date",
            "qty": "sales_qty_units", "sub_city": "city"}),

    # -- First Club
    _sales("sales_firstclub", "5.8", "First Club", "First Club",
           "First Club daily sales",
           {"sku_code": "sku_code", "sku_name": "product_name",
            "sale_date": "sale_date", "qty": "sum_of_units_sold", "sub_city": "city"}),
]

SOURCES: Dict[str, Source] = {
    s.key: s for s in _SPLIT + _AMAZON + _BIGBASKET + _BLINKIT + _FLIPKART
    + _INSTAMART + _ZEPTO + _SALES_SOURCES
}


def ordered_sources() -> List[Source]:
    """Every source in the numbered order the Drive folders imply."""
    return sorted(SOURCES.values(), key=lambda s: (s.order, s.key))


def get_source(key: str) -> Source:
    try:
        return SOURCES[key]
    except KeyError:
        raise SystemExit(
            f"Unknown source '{key}'. Known: {', '.join(sorted(SOURCES))}")


def tables() -> List[str]:
    """Distinct target tables (several sources can share one)."""
    seen, out = set(), []
    for s in ordered_sources():
        if s.table not in seen:
            seen.add(s.table)
            out.append(s.table)
    return out
