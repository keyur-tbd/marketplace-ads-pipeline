# Market Place Data → Supabase

Pushes the marketplace ads / offers / off-invoice data from the Google Drive
folder **Market Place Data** into Supabase, so the platform portals never have
to be integrated directly. The data keeps flowing into Drive exactly as it does
today; this pipeline picks it up from there.

The folder **"Not to be uploaded in Supabase"** is never read — it is skipped at
index time, so its files never even appear in the file list.

## Order

Sources are processed in the sequence the numbered Drive folders give:

| # | Drive folder | What it is | Tables |
|---|---|---|---|
| 1 | `1) Discount Split Data` | Offers/discount, processed end file | `mp_discount_split` |
| 2 | `2) Off Invoice Split Data` | Off-invoice, processed end file | `mp_off_invoice_split` |
| 3 | `3) Ads Split Data` | Ads spend, processed end file | `mp_ads_split` |
| 4 | `4) Ads Raw Data` | Raw platform exports, 6 platforms | 32 tables |

Files with **"split"** in the name are the processed end files — they are the
reporting layer. Everything under `4) Ads Raw Data` is the untouched platform
export, kept as the audit / drill-down layer.

**Scootsy is Instamart.** The rename is applied to platform *values* inside the
data, not just to folder names, via `PLATFORM_ALIASES` in `mp/sources.py`.

### 5) Sales Raw Data — one table, six columns

The sales folder is handled differently from the ads folders, on purpose. Rather
than one table per report with every column discovered, all eight platforms
land in **one table, `mp_sales`**, with exactly the columns named in
`Sales Column Name for All Platform.xlsx`:

| Column | Source, per platform |
|---|---|
| `platform` | the spec's label — `Amazon PI`, `BB Daily`, `BB Gamma Sales`, `BB Instant Sales`, `Blinkit`, `Flipkart`, `Instamart`, `Zepto`, `First Club`, `Amazon Vendor Central` |
| `sku_code` | asin / source_product_id / source_sku_id / item_id / sku_code / item_code / sku_number … |
| `sku_name` | itemName / sku_name / sku_description / item_name / product_name / product_title … |
| `sale_date` | a date column, the file name, or (Amazon PI) composed from orderDay + orderMonth + orderYear |
| `qty` | netUnits / quantity / total_quantity / qty_sold / units_sold / shipped_units … |
| `sub_city` | city / city_name / source_city_name / location_city (none for Vendor Central) |

**Everything else in those files is dropped at read time** — MRP, GMV,
categories, store ids, brand, EAN never reach Postgres, and there is no
`raw_data` on this table. The mapping lives in `mp/sources.py` as a `select`
dict per source (projection mode), so it is identical locally and on GitHub
Actions. Scootsy is loaded as `platform = 'Instamart'`.

Header detection for these sources anchors on the mapped columns themselves,
which is what lets Amazon Vendor Central's metadata row be skipped and makes a
file with none of the mapped columns get reported as misfiled instead of loaded
as nulls.

Three "Power BI Upload" monthly rollups (BB, Flipkart, Amazon — Apr–Sep 2025,
before the daily exports begin) are not in the spec and are mapped by analogy;
see the `note` on each in `mp/sources.py`.

**No store column, by decision.** Only Instamart exports a store identifier
(`store_id`, `area_name`); every other platform's finest geography is city
(First Club's `fcn` is a sequential order reference, Amazon PI's `postalcode`
is the delivery pincode, Amazon Vendor's `spoke` is empty). A column present
for one platform of ten was judged not worth adding. Two consequences:

- On Instamart, `COUNT(*)` counts sale lines across stores, not distinct
  SKU-days — 74% of its rows are identical on the six columns because they are
  different stores. `SUM(qty)` is correct; that is what the repeats are for.
- Adding a column later means a **purge and reload** of the affected platform,
  not a backfill: dropped columns are gone at read time (no `raw_data` here),
  and a new hashed column changes every row's identity. Instamart is ~2 hours.

## Setup

`.env` next to this README (real environment variables always win):

```
SUPABASE_URL=https://xxxxxxxx.supabase.co
SUPABASE_SERVICE_ROLE_KEY=eyJhbGci...     # service role: bypasses RLS for writes
SUPABASE_LOG_TABLE=workflow_logs
```

`token.json` is a Google OAuth token with the `drive` scope, for an account the
Drive folder is shared with (currently `marketing@thebakersdozen.in`).

## Usage

```bash
python -m mp.pipeline --list-sources     # every source and its table
python -m mp.pipeline --refresh-index    # re-walk Drive (needed when months are added)
python -m mp.pipeline --index            # per-source file counts, from cache
python -m mp.pipeline --discover         # sample real files -> column types
python -m mp.pipeline --print-schema     # writes schema.sql; paste into Supabase
python -m mp.pipeline --check            # credentials, tables, Drive access
python -m mp.pipeline --run --dry-run    # parse everything, write nothing
python -m mp.pipeline --run --source ads_split
python -m mp.pipeline --run              # full backfill
```

Useful flags: `--source KEY` (repeatable; accepts a key, a prefix, or a platform),
`--limit N` files per source, `--since YYYY-MM`, `--reload` to re-read files
already loaded, `--purge-cache`.

## How it holds up

- **Idempotent, without losing repeated line items.** This is the subtle part.
  Two different things get called "a duplicate" and they need opposite handling:

  - *The same row arriving twice* — a re-run, or a day that appears both in a
    daily export and inside a bulk back-history file covering that period.
    Must not be stored twice.
  - *A genuinely repeated line* — one SKU with four separate discount events in
    a month that happen to share an amount. Must all be kept, because they sum.
    Collapsing them silently understates the total.

  `row_hash` is a sha256 of the row's **content plus which copy it is** within
  its file. Provenance (`source_file`, `drive_file_id`, `source_path`) is
  deliberately excluded, so the same row from a daily and from a bulk file
  collides and the upsert keeps one. `processed_at` is excluded too, or every
  run would fork every row. Replaying a file reproduces the same copy numbers,
  so re-running is a no-op.

  Verified: the split tables were loaded twice, the second time with `--reload`
  forcing a full re-read, and both the row counts and the summed amounts were
  identical to the source files.

  The `REPEATS` column in the run summary counts rows that were the 2nd or later
  identical copy. They are kept, not dropped — the count is there so an
  unexpected value is visible.
- **Nothing is silently dropped.** Columns without a typed column still land in
  `raw_data` (jsonb) and are queryable as `raw_data->>'key'`. To promote one,
  re-run `--discover` and apply the `alter table` it emits.
- **Writes are verified, not assumed.** `upsert` counts a batch as written the
  moment the HTTP call returns success — which is not the same as the rows being
  there. Two Amazon files were once reported as fully written, logged no error,
  and landed nothing; 1,906 rows that only surfaced during reconciliation. After
  each file, one row hash per batch is probed against the table, and a file that
  fails is left out of the ledger so the next run retries it.
- **Safe resume.** `mp_loaded_files` records a file only after every one of its
  batches succeeded and was verified, so an interrupted run re-reads exactly the
  files that did not finish. Skipping on "does this table already have rows from
  that file name" is not good enough — a file killed part-way looks loaded.
- **Identifiers stay text.** `campaign_id`, `item_id`, `fsn_id`, ASINs and
  friends are forced to text so long ids never become `3.23E+12`.
- Each run writes one `workflow_logs` record.

## The awkward bits, and where they are handled

All in `mp/readers.py` unless noted:

- **No date column at all.** Big Basket, Zepto and the Flipkart dailies date
  their data only in the file name (`08-Aug-26.xlsx`). Parsed by
  `date_from_filename`, which also copes with `May -25.xlsx` and refuses to
  guess at ranges like `Feb To Dec-25`.
- **Flipkart preamble.** The daily CSVs open with `Start Time` / `End Time`
  lines before the header; the back-history pulls do not. `detect_csv_header_row`
  finds the header per file, and the preamble timestamp is preferred over the
  file name as the report date.
- **Back-history in range-named folders.** `PLA/Feb-25 To Dec-25 Data/<report>/`
  sits *between* the platform folder and the report folder. `drive.files_for`
  matches folder segments as an ordered subsequence so those files still reach
  the right table — a plain prefix match missed 41 MB of Flipkart history.
- **Multi-sheet workbooks.** Blinkit ships one workbook a month with 5–7 sheets,
  each a different ad format with its own columns; each sheet is its own source
  and its own table.
- **Blank leading rows.** Some split-file months start with a blank row, so
  `find_header_row` locates the header instead of assuming row 0.
- **Truncated sheet names.** Amazon cuts sheet names to 31 chars
  (`Sponsored_Products_Advertised_p`), so sheets are matched tolerantly.
- **Size.** Instamart is ~3.5 GB across 18 monthly CSVs — read in chunks rather
  than loaded whole.
- **A misfiled export.** `PLA/Monthly - Keyword wise Format/Aug-26/23-Aug-26.csv`
  is actually a *placement* report. Loading it would put 365 rows of the wrong
  shape into the keyword table with every real column null. Any file sharing
  less than `SHAPE_MIN_OVERLAP` of its source's core columns is skipped and
  reported, both when discovering the schema and when loading. **This one should
  be moved to the placement folder in Drive** — the guard is a safety net, not a
  fix.

## Storage

The full load is ~7.3M rows. As first written that was ~15 GB of Postgres;
it is ~5.2 GB now. Two decisions account for the difference:

- `raw_data` holds **only overflow** — keys with no typed column of their own,
  and NULL when there are none. Storing the whole row there duplicated every
  value that already had a column.
- **No GIN index on `raw_data`.** It is the most expensive index available here
  and it would be indexing a column that is usually NULL. If something in
  `raw_data` needs querying, promote it to a typed column (`--discover`) rather
  than adding the index.

Zepto is ~60% of the total on its own (~5M rows from 1,073 daily workbooks).

## Running it on GitHub Actions

`.github/workflows/marketplace-sync.yml` runs the sync monthly (02:30 UTC on the
2nd, i.e. 08:00 IST) and on demand.

**Where does it get the files?** From the same Google Drive folder, over the
Drive API. Drive is the source whether it runs here or on your laptop — nothing
about the data path changes. What differs is that a runner is discarded
afterwards, so `cache/` does not survive between runs. That is fine: the
`mp_loaded_files` ledger means a run downloads only files it has never loaded,
so a monthly sync pulls the new month rather than the 5 GB of back-history.

Set `MP_KEEP_CACHE=0` there (the workflow does) so each file is deleted once
processed — a runner has ~14 GB of disk and Instamart's monthly CSVs are ~350 MB
each.

Secrets required:

| Secret | Purpose |
|---|---|
| `GOOGLE_TOKEN_JSON` | contents of `token.json`; the OAuth refresh token works headlessly |
| `SUPABASE_URL`, `SUPABASE_SERVICE_ROLE_KEY` | the load itself |
| `PGHOST`, `PGPORT`, `PGDATABASE`, `PGUSER`, `PGPASSWORD` | optional, for the size report and `--apply-schema` |

**Do the first backfill locally.** It moved ~22M rows over about two days, which
does not fit in a job (6h ceiling). Backfill once from a workstation, then let
the workflow keep it current. The workflow never passes `--reload` for the same
reason.

## Source data worth fixing in Drive

Found while loading; the pipeline copes with all of them, but they are real:

- **A misfiled export.** `PLA/Monthly - Keyword wise Format/Aug-26/23-Aug-26.csv`
  is a *placement* report. It is skipped (28% column match) rather than loaded
  into the keyword table; move it to the placement folder and it will load.
- **A typo.** The Ads split says `FLIPKART MIINUTES` where the Discount split
  says `FLIPKART MINUTES`. Both normalise to `FLIPKART MINUTES`.
- **Instamart same-period pairs.** `Jan-26 Part 1/2`, `Aug-26-1/2` and
  `APR 1-30`/`APR 2 -30` are complementary splits, not duplicates — only ~7
  campaigns of ~200 are shared. **Sum across both**, or those months are
  understated by roughly half.
- **Triplicated Zepto files.** `26-Sep-2025.xlsx` exists under `PCA`, `PDA` and
  `KBA Raw Files`, two byte-identical. Dedup handles it; the folder layout
  appears to duplicate uploads.
- **Excel serial dates.** One Amazon budget export writes part of its Date column
  as text (`Feb 2, 2025`) and part as raw serials (`45824`). Both are parsed.

## Layout

```
mp/sources.py    the registry: folder, sheet, header, date rule, per source
mp/drive.py      Drive auth, one cached tree walk, download cache
mp/readers.py    file -> normalized rows (the messy-format handling)
mp/schema.py     samples files -> column types -> CREATE TABLE SQL
mp/sink.py       Supabase: typed rows, row_hash, upsert, workflow_logs
mp/pipeline.py   CLI
index.json       cached Drive tree
discovered.json  cached column types per table
schema.sql       generated; paste into the Supabase SQL editor
cache/           downloaded files (safe to delete)
```

## Disk guard (shared across every pipeline)

This repo writes to a Supabase volume shared with the Business Central sync and
the GRN schedulers. Before it writes, it asks the database whether it is
allowed. **If you get an email titled `[WARN]` or `[STOP] Supabase disk`, start
here.**

```sql
-- this pipeline genuinely needs more room, and the volume has space:
UPDATE etl_disk_policy SET budget_gb = 30 WHERE pipeline = 'marketplace';

-- you resized the Supabase volume (do this EVERY time you resize):
UPDATE etl_disk_policy SET budget_gb = 100 WHERE pipeline = '_disk';

-- someone else should get the emails:
UPDATE etl_alert_config SET recipients = ARRAY['birbal@thebakersdozen.in'];
```

A `[STOP]` means this pipeline is refusing to write until you do one of those.
Nothing is lost: it stops before writing, and the next run continues.

`etl_alerts.py` is **identical in every pipeline repo** - do not add per-repo
logic to it. Everything configurable lives in Postgres (`etl_disk_policy`,
`etl_alert_config`), so budgets, thresholds and recipients change with an
`UPDATE` and no deploy, for all pipelines at once.

Two behaviours worth knowing:

- **It fails OPEN.** If the guard cannot run - no credentials in that step, the
  database unreachable - it logs an error and lets the pipeline continue. A
  guard that breaks a working pipeline is worse than one that cannot check.
  Grep the logs for `Disk guard could not run` if you suspect it is asleep.
- **Budgets grow themselves** into genuinely unallocated volume space, so a
  pipeline that is legitimately growing is not blocked by a number somebody
  guessed months ago. It can never grow past the volume ceiling, so this is
  not a way of turning the guard off.

Full documentation, including how the budgets were sized:
https://github.com/keyur-tbd/bc-supabase-sync#disk-alerts-and-auto-budgeting---start-here-if-you-got-an-email
