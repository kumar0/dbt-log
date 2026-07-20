# `headers.processedAt` bigint → timestamp migration (dbt-glue)

dbt macros that migrate a nested `headers.processedAt` field from **bigint (epoch
milliseconds)** to a proper **timestamp** across every table in a Glue database —
safely, incrementally, and while the tables keep receiving new data.

The migration is split into **three independently runnable phases** plus a
**rollback**, coordinated through a control table in a dedicated backup database.
Phase 2 (convert) can be run any number of times; each run only touches the
delta that arrived since the previous run.

```
macros/
├── original/
│   └── migrate_processed_at_original.sql   # the original one-shot macros (reference only — keep OUT of macro-paths)
├── deploy/
│   ├── migration_helpers.sql               # shared helpers used by all phases
│   ├── 1_migration_prepare.sql             # PHASE 1: backup db + control table + backups + add column
│   ├── 2_migration_convert.sql             # PHASE 2: delta backup + delta convert + verify (re-runnable)
│   └── 3_migration_finalize.sql            # PHASE 3: verify → drop old column → rename → report
└── rollback/
    └── migration_rollback.sql              # state-aware rollback
```

---

## How it works

### The problem

`headers.processedAt` is a bigint holding epoch millis inside the `headers`
struct. It must become a real `timestamp` column with the same name — but the
tables are **live**: rows keep arriving between and during migration runs, so a
one-shot "backup, convert, swap" is not safe.

### The approach

Each table moves through a small state machine recorded in the control table:

```
            prepare              convert (×N)             finalize
 (none) ──────────────► prepared ───────────► converted ───────────► finalized
                            │    ▲       │                   │
                            │    └───────┘                   │
                            │   converting                   │
                            │  (delta still                  │
                            │   arriving)                    │
                            └──────────► failed ◄────────────┘
                                     (verification error)

  rollback (any state) ──► rolled_back
```

During phases 1–2 the source table has **both** columns:

| column                    | type      | role                                   |
|---------------------------|-----------|----------------------------------------|
| `headers.processedAt`     | bigint    | original value — untouched until finalize |
| `headers.processedAt_ts`  | timestamp | new value, backfilled via `timestamp_millis()` |

Finalize (phase 3) drops the bigint and renames `processedAt_ts` →
`processedAt`. Until that moment every step is non-destructive and re-runnable.

### Why the column is added *before* the backup

Phase 1 adds `headers.processedAt_ts` to the source **first**, then takes the
backup (`CREATE TABLE ... AS SELECT`). The backup therefore has the identical
schema, so later delta rows can be appended with a plain
`INSERT INTO backup SELECT * FROM source WHERE ...`. The backup still contains
every original bigint value (its `processedAt_ts` is simply NULL for the rows
captured at backup time), which is exactly what verification and rollback need.

---

## The control table

`<source_db>_backup.migration_control` — created by phase 1, one row per table:

| column              | type      | meaning                                                    |
|---------------------|-----------|------------------------------------------------------------|
| `table_name`        | string    | table in the source database                               |
| `status`            | string    | `prepared` → `converting` → `converted` → `finalized`; also `failed`, `rolled_back` |
| `source_count`      | bigint    | source row count at last touch                             |
| `backup_count`      | bigint    | backup row count                                           |
| `converted_count`   | bigint    | rows with a non-null `processedAt_ts`                      |
| `pending_count`     | bigint    | rows with a bigint but no timestamp yet (should reach 0)   |
| `last_delta_rows`   | bigint    | rows appended to the backup in the most recent convert run |
| `prepare_time`      | timestamp | when phase 1 finished for this table                       |
| `last_convert_time` | timestamp | stamped on every phase-2 run                               |
| `finalize_time`     | timestamp | when phase 3 finished for this table                       |
| `last_error`        | string    | last verification failure / informational note             |
| `updated_at`        | timestamp | stamped on every write                                     |

---

## Usage

All macros read the same vars:

| var                  | required                | meaning                                             |
|----------------------|-------------------------|-----------------------------------------------------|
| `source_db_name`     | always                  | the Glue database being migrated                    |
| `backup_s3_location` | first `prepare` run only| S3 location for the backup database                 |
| `tables`             | optional                | `all` (default), one name, comma list, or YAML list |
| `rollback_confirm`   | rollback restores only  | must be `true` to allow a destructive restore       |

### Phase 1 — prepare

```bash
# whole database — auto-discovers every table whose headers struct still has
# processedAt:bigint (or a leftover processedAt_ts)
dbt run-operation migration_prepare \
  --vars '{source_db_name: mydb, backup_s3_location: "s3://my-bucket/backups/mydb"}'

# subset
dbt run-operation migration_prepare \
  --vars '{source_db_name: mydb, backup_s3_location: "s3://my-bucket/backups/mydb", tables: "orders,events"}'
```

Per table: ensures backup DB + control table exist, adds
`headers.processedAt_ts` to the source, takes the CTAS backup (an existing
backup is **never overwritten** — including one made by the original macro,
whose schema is patched in place), verifies counts, marks `prepared`.

Idempotent: re-running skips tables already `prepared`/`converting`/`converted`.
A count check where **backup > source** is a hard failure; **backup < source**
is fine (the source grew — phase 2 backs up the difference).

### Phase 2 — convert (run as many times as you like)

```bash
dbt run-operation migration_convert --vars '{source_db_name: mydb}'                  # all registered tables
dbt run-operation migration_convert --vars '{source_db_name: mydb, tables: orders}'  # one table
dbt run-operation migration_convert --vars '{source_db_name: mydb, tables: "a,b,c"}' # subset
```

Each run, per table:

1. **Delta backup** — appends to the backup only the rows that arrived since
   the last backup:
   - non-null `processedAt`: watermark — rows with
     `headers.processedAt > MAX(headers.processedAt)` already in the backup;
   - **NULL `processedAt`**: invisible to a watermark, so they are captured
     exactly with `EXCEPT ALL` over the NULL-processedAt subset
     (duplicate-safe, scans only that subset).
2. **Delta convert** — `UPDATE ... SET headers.processedAt_ts =
   timestamp_millis(headers.processedAt)` restricted to rows not yet converted.
   Rows with NULL `processedAt` are intentionally untouched — NULL is their
   correct converted value.
3. **Verification** (single round trip):
   - `corrupt` — a converted timestamp that does **not** round-trip
     (`unix_millis(ts) != bigint`) → table marked `failed`, run aborts;
   - `unconverted` rows or backup count/sum drift → interpreted as rows that
     arrived *during* the run: the table stays `converting` (with the reason in
     `last_error`) and the next run picks them up — not an error on a live table;
   - clean → `converted`, counts and `last_delta_rows` recorded.

The reverse check compares **count and sum of the bigints** in the source
against the backup, plus **NULL-row and total-row counts**, so both value drift
and missing rows (including NULL-processedAt rows) are caught.

### Phase 3 — finalize

```bash
dbt run-operation migration_finalize --vars '{source_db_name: mydb}'
dbt run-operation migration_finalize --vars '{source_db_name: mydb, tables: "orders,events"}'
```

Per table (only if control status is `converted`):

1. **Fresh pre-flight re-verification** — recomputed from the live data, never
   trusted from stored counts: zero corrupt rows, zero unconverted rows, bigint
   count/sum and NULL/total counts match the backup. Any failure → table marked
   `failed`, columns untouched, the run continues with the next table.
2. `ALTER TABLE ... DROP COLUMN headers.processedAt` (the old bigint)
3. `ALTER TABLE ... RENAME COLUMN headers.processedAt_ts TO processedAt`
4. Post-check via `DESCRIBE`, then `finalized`.

A table interrupted between drop and rename (only `processedAt_ts` present) is
recognized and completed. The run ends with a **verification report** — the full
control table formatted to the log, with per-status totals:

```
==========================================================================
MIGRATION VERIFICATION REPORT - mydb
==========================================================================
table_name           status       src_cnt   bkp_cnt  converted  pending ...
--------------------------------------------------------------------------
events               finalized      10422     10422      10390        0 ...
orders               finalized       8113      8113       8113        0 ...
--------------------------------------------------------------------------
Totals: finalized=2
==========================================================================
```

(`converted` counts non-null timestamps, so it is `src_cnt` minus the table's
NULL-`processedAt` rows — a difference there is expected, not an error.)

### Rollback

```bash
# non-destructive rollback (old bigint still present) — no confirmation needed
dbt run-operation migration_rollback --vars '{source_db_name: mydb, tables: orders}'

# destructive restore (table already finalized) — must be confirmed
dbt run-operation migration_rollback --vars '{source_db_name: mydb, tables: orders, rollback_confirm: true}'

# everything
dbt run-operation migration_rollback --vars '{source_db_name: mydb, rollback_confirm: true}'
```

State-aware — it inspects the **live schema**, not just the control table:

| table state                            | action                                                            | data loss |
|----------------------------------------|-------------------------------------------------------------------|-----------|
| old bigint still present (phases 1–2)  | drop the added `headers.processedAt_ts` field                     | none      |
| old bigint gone (finalized/interrupted)| `CREATE OR REPLACE TABLE ... AS SELECT * FROM backup`, then drop the residual `processedAt_ts` field; restored count verified against the backup | **rows written after the last delta backup are lost** — hence `rollback_confirm: true` |

Backups and the backup database are **never dropped** by any macro — clean them
up manually once the migration is verified.

---

## NULL `processedAt` handling (summary)

| concern | handling |
|---|---|
| conversion | only non-null rows are updated; NULL rows keep a NULL `processedAt_ts` (correct value) |
| delta backup | watermark can never see NULL rows → dedicated `EXCEPT ALL` leg captures them exactly |
| verification | NULL counts and total counts compared source vs backup — a missing NULL row blocks finalize |

## Assumptions & limitations

- **Watermark monotonicity**: newly arriving rows are assumed to carry a
  `headers.processedAt` **greater** than the current backup maximum. A late row
  whose value *equals* the max is not delta-picked; verification then keeps the
  table in `converting` with a visible count mismatch (never a silent pass).
- **`EXCEPT ALL` comparability**: the NULL-row delta compares full rows, which
  requires comparable column types — tables containing **map** columns cannot
  use it (Spark cannot compare maps).
- **Table format**: tables must support row-level `UPDATE`, nested-field
  `ADD/DROP/RENAME COLUMN`, and `CREATE OR REPLACE TABLE` (e.g. Iceberg on
  Glue — the same operations the original macros already used).
- **Concurrent writers**: phases never lock the table; verification is what
  guarantees correctness. If a table keeps landing rows continuously, run
  `migration_convert` until it reports `converted`, then finalize promptly.
- **Row counts** use `COUNT`/`SUM` comparisons, not row-by-row diffs; the
  forward round-trip check (`unix_millis(ts) == bigint` per row) is what proves
  value-level correctness.

## Troubleshooting

| symptom | meaning | fix |
|---|---|---|
| `S3 location must be provided...` | first prepare run needs the backup DB location | pass `backup_s3_location` |
| table stuck in `converting` | rows keep arriving between backup/convert/verify, or a tie on the watermark | re-run `migration_convert`; if it never converges, check `last_error` and compare bigint count/sum source vs backup manually |
| status `failed`, `last_error` mentions round-trip | a converted timestamp doesn't match its bigint — data problem, not timing | inspect the offending rows (`unix_millis(headers.processedAt_ts) != headers.processedAt`); nothing has been dropped |
| `ambiguous state - headers has both ...` | a table has `processedAt:timestamp` **and** `processedAt_ts` | resolve manually — the macros refuse to guess |
| finalize skipped a table | control status wasn't `converted` | run `migration_convert` for it first |
| rollback aborts asking for confirmation | the table needs a destructive restore from backup | re-run with `rollback_confirm: true` (accepting loss of rows after the last delta backup) |

## Notes

- `macros/original/` is the pre-split one-shot implementation, transcribed
  verbatim for reference. It defines `dp_run_query`, which is why the new
  helpers use the name `mig_run_query` — but keep the original file out of your
  dbt `macro-paths` anyway to avoid confusion.
- All helper macros are prefixed `mig_` and live in
  `deploy/migration_helpers.sql`; the rollback macro depends on them too, so
  deploy both folders together.
