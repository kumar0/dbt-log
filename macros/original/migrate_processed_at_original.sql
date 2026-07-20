{#
  ORIGINAL MACROS — verbatim transcription from screenshots (reference only).

  Contains:
    - migrate_all_processed_at()          orchestrator: backup DB + control table + discovery + per-table loop
    - dp_run_query(sql)                   run_query wrapper that raises when no result set is returned
    - migrate_processed_at(table_name)    per-table: backup, verify, add column, backfill, verify

  These have been superseded by the split 3-phase macros in macros/deploy/
  (migration_prepare / migration_convert / migration_finalize) and
  macros/rollback/migration_rollback. Keep this file OUT of the dbt
  macro-paths if the deploy macros are installed, to avoid confusion.
#}

{% macro migrate_all_processed_at() %}
  {% if execute %}

    {% set src_db    = var('source_db_name') %}
    {% set backup_db = src_db ~ '_backup' %}
    {% set control   = backup_db ~ '.tables_to_migrate' %}
    {% set s3_location = var('backup_s3_location', none) %}  {# Get S3 location from command line #}

    {# ---------- 0. Ensure backup database exists ---------- #}
    {% set db_exists = dp_run_query("SHOW DATABASES LIKE '" ~ backup_db ~ "'").rows | length > 0 %}
    {% if not db_exists %}
      {% do log("Creating backup database: " ~ backup_db, info=True) %}
      {% if s3_location %}
        {% do log("Using S3 location: " ~ s3_location, info=True) %}
        {% do adapter.execute("CREATE DATABASE " ~ backup_db ~ " LOCATION '" ~ s3_location ~ "'") %}
      {% else %}
        {% do exceptions.raise_compiler_error("S3 location must be provided via --vars 'backup_s3_location: s3://your-bucket/path'") %}
      {% endif %}
      {% do log("Backup database created", info=True) %}
    {% else %}
      {% do log("Backup database already exists: " ~ backup_db, info=True) %}
    {% endif %}

    {# ---------- 1. Ensure control table exists with timing and record count columns ---------- #}
    {% do log("Control table: " ~ control, info=True) %}
    {# transcription note: line truncated in source image after "record_c" — completed as record_count bigint #}
    {% do adapter.execute("CREATE TABLE IF NOT EXISTS " ~ control ~ " (table_name string, status string, start_time timestamp, end_time timestamp, record_count bigint)") %}

    {# ---------- 2. Read progress: done + known tables ---------- #}
    {% set done_tables  = [] %}
    {% set known_tables = [] %}
    {% for prow in dp_run_query("SELECT table_name, status FROM " ~ control).rows %}
      {% if prow[0] not in known_tables %}
        {% do known_tables.append(prow[0]) %}
      {% endif %}
      {% if prow[1] == 'done' and prow[0] not in done_tables %}
        {% do done_tables.append(prow[0]) %}
      {% endif %}
    {% endfor %}
    {% do log("Already completed (will be skipped): " ~ done_tables | length ~ " tables", info=True) %}

    {# ---------- 3. Discover candidate tables (skip done ones) ---------- #}
    {% set candidates = [] %}
    {% set all_tables = dp_run_query("SHOW TABLES IN " ~ src_db) %}
    {% do log("Scanning " ~ all_tables.rows | length ~ " tables in " ~ src_db, info=True) %}

    {% for trow in all_tables.rows %}
      {% set tname = trow[1] %}   {# SHOW TABLES -> (namespace, tableName, isTemporary) #}
      {% if tname != 'tables_to_migrate' and tname not in done_tables %}
        {% set found = namespace(hit=false) %}
        {% for crow in dp_run_query("DESCRIBE TABLE " ~ src_db ~ "." ~ tname).rows %}
          {% if not found.hit and crow[0] == 'headers' %}
            {% set htype = crow[1] | string %}
            {# processedAt_ts catches tables interrupted after the old
               bigint column was dropped but before the rename #}
            {% if 'processedAt:bigint' in htype or 'processedAt_ts:' in htype %}
              {% set found.hit = true %}
            {% endif %}
          {% endif %}
        {% endfor %}
        {% if found.hit %}
          {% do candidates.append(tname) %}
        {% endif %}
      {% endif %}
    {% endfor %}

    {% set candidates = candidates | sort %}

    {# ---------- 4. Record newly discovered tables as pending ---------- #}
    {% set new_pending = [] %}
    {% for t in candidates %}
      {% if t not in known_tables %}
        {% do new_pending.append("('" ~ t ~ "', 'pending', NULL, NULL, NULL)") %}
      {% endif %}
    {% endfor %}
    {% if new_pending | length > 0 %}
      {% do adapter.execute("INSERT INTO " ~ control ~ " VALUES " ~ new_pending | join(', ')) %}
    {% endif %}
    {% do log("Pending this run: " ~ candidates | length ~ " tables (" ~
              new_pending | length ~ " newly discovered)", info=True) %}

    {# ---------- 5. Migrate each candidate, marking progress with timing and record count ---------- #}
    {% for t in candidates %}
      {% do log("[" ~ loop.index ~ "/" ~ candidates | length ~ "] " ~ t, info=True) %}
      {% do adapter.execute("UPDATE " ~ control ~ " SET status = 'started', start_time = current_timestamp() WHERE table_name = '" ~ t ~ "' AND status = 'pending'") %}

      {# Get record count before migration #}
      {% set count_result = dp_run_query("SELECT COUNT(*) as cnt FROM " ~ src_db ~ "." ~ t) %}
      {% set record_count = count_result.rows[0][0] %}

      {% do migrate_processed_at(t) %}
      {# transcription note: line truncated in source image after "AND status = 'starte" — completed as 'started' #}
      {% do adapter.execute("UPDATE " ~ control ~ " SET status = 'done', end_time = current_timestamp(), record_count = " ~ record_count ~ " WHERE table_name = '" ~ t ~ "' AND status = 'started'") %}
      {% do log("  > Migrated " ~ record_count ~ " records", info=True) %}
    {% endfor %}

    {% do log("All migrations complete.", info=True) %}

  {% endif %}
{% endmacro %}


{% macro dp_run_query(sql) %}
  {% set res = run_query(sql) %}
  {% if res is none %}
    {% do exceptions.raise_compiler_error("Query returned no result set (dbt-glue): " ~ sql) %}
  {% endif %}
  {{ return(res) }}
{% endmacro %}


{% macro migrate_processed_at(table_name) %}

  {% set src_db      = var('source_db_name') %}
  {% set backup_db   = src_db ~ '_backup' %}
  {% set src_table   = src_db ~ '.' ~ table_name %}
  {% set bkp_table   = backup_db ~ '.' ~ table_name %}

  {% do log("Starting migration: " ~ src_table, info=True) %}

  {# ---------- 0. Read current state of headers ---------- #}
  {% set headers_type = namespace(val='') %}
  {% for crow in dp_run_query("DESCRIBE TABLE " ~ src_table).rows %}
    {% if crow[0] == 'headers' and headers_type.val == '' %}
      {% set headers_type.val = crow[1] | string %}
    {% endif %}
  {% endfor %}

  {% set has_bigint = 'processedAt:bigint'      in headers_type.val %}
  {% set has_ts_col = 'processedAt_ts:'         in headers_type.val %}
  {% set has_final  = 'processedAt:timestamp'   in headers_type.val %}

  {% if has_final and not has_bigint and not has_ts_col %}
    {% do log("  already migrated (processedAt is timestamp), skipping", info=True) %}
    {{ return('') }}
  {% elif has_final and has_ts_col %}
    {% do exceptions.raise_compiler_error(
        "ABORT " ~ src_table ~ ": ambiguous state - headers has both " ~
        "processedAt:timestamp and processedAt_ts. Resolve manually.") %}
  {% elif not has_bigint and not has_ts_col %}
    {% do exceptions.raise_compiler_error(
        "ABORT " ~ src_table ~ ": headers has no processedAt bigint or " ~
        "processedAt_ts field (headers type: " ~ headers_type.val ~ ")") %}
  {% endif %}
  {% do log("  state -> has_bigint=" ~ has_bigint ~ ", has_ts_col=" ~ has_ts_col, info=True) %}

  {# ---------- 1. Backup: create table as select (skip if it exists) ---------- #}
  {% set bkp_exists = dp_run_query("SHOW TABLES IN " ~ backup_db ~ " LIKE '" ~ table_name ~ "'").rows | length > 0 %}
  {% if bkp_exists %}
    {% do log("  backup already exists: " ~ bkp_table ~ " (keeping it, NOT overwriting)", info=True) %}
  {% else %}
    {% do log("  creating backup: " ~ bkp_table, info=True) %}
    {% do adapter.execute("CREATE TABLE " ~ bkp_table ~ " AS SELECT * FROM " ~ src_table) %}
    {% do log("  backup created", info=True) %}
  {% endif %}

  {# ---------- 2. Verify row counts source vs backup (single round trip) ---------- #}
  {% set counts = dp_run_query(
      "SELECT s.c AS src_cnt, b.c AS bkp_cnt FROM " ~
      " (SELECT COUNT(*) AS c FROM " ~ src_table ~ ") s" ~
      " CROSS JOIN " ~
      " (SELECT COUNT(*) AS c FROM " ~ bkp_table ~ ") b"
    ).rows[0] %}
  {% do log("  count check -> source: " ~ counts[0] ~ " | backup: " ~ counts[1], info=True) %}
  {% if counts[0] != counts[1] %}
    {% do exceptions.raise_compiler_error(
        "ABORT " ~ src_table ~ ": backup count mismatch (source=" ~ counts[0] ~
        ", backup=" ~ counts[1] ~ "). Inspect/refresh backup manually. Old column NOT dropped.") %}
  {% endif %}

  {# ---------- 3. Add column (skip if it exists) ---------- #}
  {% if not has_ts_col %}
    {% do log("  adding processedAt_ts", info=True) %}
    {% do adapter.execute("ALTER TABLE " ~ src_table ~ " ADD COLUMN headers.processedAt_ts timestamp") %}
    {% do log("  added processedAt_ts", info=True) %}
  {% else %}
    {% do log("  processedAt_ts already present, skipping add", info=True) %}
  {% endif %}

  {% if has_bigint %}

    {# ---------- 4. Backfill values (idempotent, safe to re-run) ---------- #}
    {% do adapter.execute("UPDATE " ~ src_table ~
        " SET headers.processedAt_ts = timestamp_millis(headers.processedAt)") %}
    {% do log("  backfilled values", info=True) %}

    {# ---------- 5. Verification (single round trip) ----------
       a) forward: every non-null bigint must round-trip
          unix_millis(new_ts) == old bigint  (within source table)
       b) reverse: count/sum of new timestamps (as millis) in source must
          match count/sum of original bigints in the backup            #}
    {% set v = dp_run_query(
        "SELECT s.fwd_mismatch, s.ts_cnt, s.ts_sum, b.old_cnt, b.old_sum FROM " ~
        " (SELECT" ~
        "     SUM(CASE WHEN headers.processedAt IS NOT NULL" ~
        "              AND (headers.processedAt_ts IS NULL" ~
        "                   OR unix_millis(headers.processedAt_ts) != headers.processedAt)" ~
        "         THEN 1 ELSE 0 END) AS fwd_mismatch," ~
        "     COUNT(headers.processedAt_ts) AS ts_cnt," ~
        "     SUM(unix_millis(headers.processedAt_ts)) AS ts_sum" ~
        "   FROM " ~ src_table ~ ") s" ~
        " CROSS JOIN " ~
        " (SELECT COUNT(headers.processedAt) AS old_cnt," ~
        "         SUM(headers.processedAt) AS old_sum" ~
        "   FROM " ~ bkp_table ~ ") b"
      ).rows[0] %}

    {% do log("  forward check (bigint -> timestamp) mismatches: " ~ (v[0] | int), info=True) %}
    {% if v[0] | int != 0 %}
      {% do exceptions.raise_compiler_error(
          "ABORT " ~ src_table ~ ": " ~ v[0] ~
          " rows failed bigint->timestamp verification. Old column NOT dropped.") %}
    {% endif %}

    {% do log("  reverse check vs backup -> source(cnt=" ~ v[1] ~ ", sum=" ~ v[2] ~
              ") | backup(cnt=" ~ v[3] ~ ", sum=" ~ v[4] ~ ")", info=True) %}
    {% if v[1] != v[3] or v[2] != v[4] %}
      {% do exceptions.raise_compiler_error(
          "ABORT " ~ src_table ~ ": reverse verification vs backup failed. Old column NOT dropped.") %}
    {% endif %}

  {% endif %}

  {% do log("Completed: " ~ src_table, info=True) %}

{% endmacro %}
