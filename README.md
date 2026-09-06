# Databricks Medallion Architecture — E-Commerce Order Pipeline

Batch ETL pipeline implementing Bronze → Silver → Gold medallion architecture using PySpark + Delta Lake. Built to demonstrate production-grade data engineering patterns: schema enforcement, lineage tracking, incremental processing, and layered data quality.

**Local dev note:** 
This repo runs on open-source Spark + Delta Lake locally (`configure_spark_with_delta_pip`). On actual Databricks, Delta ships pre-installed on the runtime, so no local JAR bootstrap is needed. The architecture is identical either way — only the Spark session init differs.

## Status: In Progress

- [x] **Bronze layer** — raw ingestion, schema enforcement, lineage metadata
- [x] **Silver layer** — cleaning, dedup, joins, quarantine-based quality gates
- [x] **Gold layer** — 3 business marts, window functions (RANK, LAG, MIN)
- [ ] **Tests** — pytest regression suite (in progress)
- [ ] **Orchestration + docs** — single entry point, architecture diagram

## Architecture

data/raw/ → bronze/ → silver/ → gold/
(source) (raw+meta) (clean) (marts)
↓
_quarantine/
(rejected, reason-coded)


**Bronze principle:** 
Schema-on-read, zero business logic, immutable append-only history. Dirty data (nulls, negative quantities, duplicates) is preserved as-is — cleaning happens downstream in Silver, never in Bronze. This separation is deliberate: Bronze is the audit trail, so if a cleaning rule turns out wrong later, the raw truth is still recoverable.

## Project Structure

.
├── data/
│ ├── generate_sample_data.py # synthetic e-commerce data generator
│ └── raw/ # generated raw files (gitignored)
├── bronze/
│ └── ingest_bronze.py # raw → bronze delta tables
├── silver/
│ └── process_silver.py # bronze → silver: clean, dedup, join, quarantine
├── gold/
│ └── build_gold.py # silver → gold: 3 business marts
├── requirements.txt
├── LICENSE
└── .gitignore


## Setup

```bash
pip install -r requirements.txt
python data/generate_sample_data.py   # generates customers.csv, products.csv, orders.jsonl
python bronze/ingest_bronze.py        # ingests raw → bronze Delta tables
python silver/process_silver.py       # bronze → silver: clean, dedup, join
python gold/build_gold.py             # silver → gold: business marts
```

## Bronze Layer Design

| Table | Source format | Schema enforcement | Notes |
|---|---|---|---|
| `bronze.customers` | CSV | Explicit `StructType` | ~3% synthetic duplicates injected |
| `bronze.products` | CSV | Explicit `StructType` | Clean dimension table |
| `bronze.orders` | JSONL | Explicit `StructType` | ~2% null customer_id, ~4% invalid qty (data entry error simulation) |

Every bronze table carries identical lineage columns:
`_ingest_timestamp`, `_ingest_date`, `_batch_id`, `_source_system`, `_source_file`

Partitioned by `_ingest_date` for efficient incremental downstream reads.

**Why explicit schemas over `inferSchema=True`:** inferred schemas are a real production failure mode. One stray malformed value in a large file can silently flip a column's inferred type, breaking every downstream job that depends on it. Explicit schemas fail loudly and immediately instead.

## Silver Layer Design

**Quarantine over deletion.** Bad rows are never silently dropped — they're written to `silver/_quarantine/orders` tagged with `_reject_reason`. Six months from now, when someone asks why total revenue is lower than expected, the answer is queryable, not lost.

| Quality gate | Rule | Rationale |
|---|---|---|
| Invalid quantity | `quantity <= 0` → quarantine | Data entry error simulation; negative/zero units is not a valid order line |
| Null customer_id | `customer_id IS NULL` → quarantine | Can't attribute revenue to an unknown customer; not deletable, not usable downstream |
| Referential integrity | Inner join to `customers` + `products`, miss count logged | Orphaned foreign keys signal upstream data contract violations — must be visible, not silently filtered |

**Dedup strategy:** `ROW_NUMBER()` window over `customer_id`, ordered by `_ingest_timestamp DESC` — keeps the most recently ingested version per natural key. Verified against synthetic 3% dupe injection: 206 raw → 200 deduped (6 removed, exact match).

**Verified pipeline math (sample run):** 5000 raw orders → 204 invalid quantity + 109 null customer + 0 referential misses = 313 quarantined → 4687 final silver rows. `5000 − 313 = 4687` reconciles exactly, with no silent row loss and no join fan-out.

**Type casts applied:** `signup_date`, `order_date` → `date` type (bronze keeps these as strings deliberately, since bronze does zero business logic).

**Derived column:** `line_total = quantity × unit_price`, computed once in silver so every downstream Gold aggregation reads a pre-validated number instead of recomputing it inconsistently in multiple places.

## Gold Layer Design

Three business marts, each demonstrating a distinct Spark window-function pattern:

| Mart | Business question answered | Window function |
|---|---|---|
| `customer_revenue_mart` | Who are the highest-value customers, ranked within region? | `RANK()` — ties share rank, deliberately not `ROW_NUMBER()` |
| `category_performance_mart` | How is each category trending month over month? | `LAG()` — pulls prior month into same row, no self-join needed |
| `customer_cohort_mart` | Who are repeat vs one-time buyers, and when did they first convert? | `MIN()` — first-order-date in a single pass, no groupBy+join-back |

Gold reads **only** from Silver, never Bronze — business logic lives in exactly one place. All 3 marts filter to `order_status = 'COMPLETED'`, since cancelled, returned, or pending orders shouldn't count toward revenue metrics.

**Verified output (sample run):** 200/200 customers had at least one completed order, plausible at roughly 25 orders per customer on average. Revenue rank correctly produced exactly one rank-1 winner per region with no ties collision. Category MoM correctly returned `NULL` (not `0`) for each category's first observed month, since there's no valid prior-period comparison. Repeat-buyer rate came out to 96.5% (193/200), consistent with high per-customer order volume in the synthetic dataset.

**Bug caught and fixed during testing:** raw `SUM()` aggregations produced float-precision artifacts (e.g. `19714.710000000003` instead of `19714.71`) — standard IEEE 754 floating-point accumulation noise, not a logic error. Fixed by rounding at the aggregation step rather than the display step, so every consumer of the Gold table gets clean values without re-deriving the fix downstream.

## Coming Next

- Pytest regression suite covering the dedup, quarantine, and window-function logic above
- `run_pipeline.py` — single entry point running bronze → silver → gold in sequence
- Architecture diagram

## Known Limitations

- **Batch only, not streaming.**
Each run does a full read of its input layer — there's no incremental or watermark logic. Rerunning bronze twice without clearing prior output will append duplicate batches, by design, since bronze is append-only history. Silver and gold must always run against a deliberately chosen bronze snapshot, never one assumed to be "today's data only."

- **Local dev, not cluster-tested.**
Built and validated on local PySpark with Delta Lake bootstrapped via `configure_spark_with_delta_pip`. The same code runs on Databricks-managed Delta without modification — only the Spark session initialization differs (see the note at the top of this README).

- **Synthetic data only.**
`data/generate_sample_data.py` produces plausible but fabricated e-commerce records. There's no production data sensitivity, but also no real-world data messiness beyond what's deliberately injected (dupes, nulls, invalid quantities).

## License

MIT — see [LICENSE](LICENSE).