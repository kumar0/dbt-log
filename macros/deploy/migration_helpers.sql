{#
  Shared helpers for the 3-phase headers.processedAt bigint -> timestamp migration.

  Used by:
    macros/deploy/1_migration_prepare.sql    -> migration_prepare()
    macros/deploy/2_migration_convert.sql    -> migration_convert()
    macros/deploy/3_migration_finalize.sql   -> migration_finalize()
    macros/rollback/migration_rollback.sql   -> migration_rollback()

  Conventions:
    - source db      : var('source_db_name')
    - backup db      : <source_db_name>_backup   (created with var 'backup_s3_location')
    - control table  : <source_db_name>_backup.migration_control
    - table selection: var('tables', 'all') — 'all', a comma-separated string,
                       or a YAML list. Applies to every phase and to rollback.

  Control table lifecycle per table:
    prepared -> converting -> converted -> finalized
    (plus 'failed' on verification errors and 'rolled_back' after rollback)
#}


{% macro mig_run_query(sql) %}
  {% set res = run_query(sql) %}
  {% if res is none %}
    {% do exceptions.raise_compiler_error("Query returned no result set (dbt-glue): " ~ sql) %}
  {% endif %}
  {{ return(res) }}
{% endmacro %}


{% macro mig_config() %}
  {% set src_db = var('source_db_name') %}
  {% set backup_db = src_db ~ '_backup' %}
  {{ return({
      'src_db': src_db,
      'backup_db': backup_db,
      'control': backup_db ~ '.migration_control'
  }) }}
{% endmacro %}


{# Columns of the control table, in DDL order (table_name and updated_at handled separately) #}
{% macro mig_control_columns() %}
  {{ return(['status', 'source_count', 'backup_count', 'converted_count',
             'pending_count', 'last_delta_rows', 'prepare_time',
             'last_convert_time', 'finalize_time', 'last_error']) }}
{% endmacro %}


{% macro mig_create_control_table(cfg) %}
  {% do adapter.execute("CREATE TABLE IF NOT EXISTS " ~ cfg.control ~ " (" ~
      "table_name string, " ~
      "status string, " ~
      "source_count bigint, " ~
      "backup_count bigint, " ~
      "converted_count bigint, " ~
      "pending_count bigint, " ~
      "last_delta_rows bigint, " ~
      "prepare_time timestamp, " ~
      "last_convert_time timestamp, " ~
      "finalize_time timestamp, " ~
      "last_error string, " ~
      "updated_at timestamp)") %}
{% endmacro %}


{# Returns the control-table status for a table, or none if the table has no row yet #}
{% macro mig_control_status(cfg, table_name) %}
  {% set rows = mig_run_query("SELECT status FROM " ~ cfg.control ~
      " WHERE table_name = '" ~ table_name ~ "'").rows %}
  {% if rows | length == 0 %}
    {{ return(none) }}
  {% endif %}
  {{ return(rows[0][0]) }}
{% endmacro %}


{#
  Upsert into the control table.
  `assignments` is a dict of column -> SQL literal string, e.g.
    {'status': "'prepared'", 'source_count': '123', 'prepare_time': 'current_timestamp()'}
  updated_at is always stamped.
#}
{% macro mig_control_set(cfg, table_name, assignments) %}
  {% set exists = mig_run_query("SELECT COUNT(*) AS c FROM " ~ cfg.control ~
      " WHERE table_name = '" ~ table_name ~ "'").rows[0][0] | int > 0 %}
  {% if exists %}
    {% set sets = [] %}
    {% for k, v in assignments.items() %}
      {% do sets.append(k ~ " = " ~ v) %}
    {% endfor %}
    {% do sets.append("updated_at = current_timestamp()") %}
    {% do adapter.execute("UPDATE " ~ cfg.control ~ " SET " ~ sets | join(', ') ~
        " WHERE table_name = '" ~ table_name ~ "'") %}
  {% else %}
    {% set vals = ["'" ~ table_name ~ "'"] %}
    {% for c in mig_control_columns() %}
      {% do vals.append(assignments.get(c, 'NULL')) %}
    {% endfor %}
    {% do vals.append('current_timestamp()') %}
    {% do adapter.execute("INSERT INTO " ~ cfg.control ~ " VALUES (" ~ vals | join(', ') ~ ")") %}
  {% endif %}
{% endmacro %}


{# Escape a message for embedding in a single-quoted SQL literal #}
{% macro mig_sql_str(msg) %}
  {{ return("'" ~ (msg | string) | replace("'", "''") ~ "'") }}
{% endmacro %}


{#
  Resolve which tables a phase operates on.
    var('tables', 'all'):
      - 'all' (default)          -> discover
      - 'orders,events' / list   -> use as given
    discover='control' -> every table registered in the control table
    discover='scan'    -> scan SHOW TABLES in src_db for a headers struct that
                          still has processedAt:bigint or processedAt_ts
                          (same discovery logic as the original macro)
#}
{% macro mig_resolve_tables(cfg, discover='control') %}
  {% set tables_var = var('tables', 'all') %}
  {% if tables_var is not string %}
    {{ return(tables_var | list) }}
  {% elif tables_var | trim | lower != 'all' %}
    {% set tlist = [] %}
    {% for t in tables_var.split(',') %}
      {% if t | trim != '' %}
        {% do tlist.append(t | trim) %}
      {% endif %}
    {% endfor %}
    {{ return(tlist) }}
  {% endif %}

  {% if discover == 'control' %}
    {% set out = [] %}
    {% for r in mig_run_query("SELECT DISTINCT table_name FROM " ~ cfg.control ~
        " ORDER BY table_name").rows %}
      {% do out.append(r[0]) %}
    {% endfor %}
    {{ return(out) }}
  {% else %}
    {% set candidates = [] %}
    {% set all_tables = mig_run_query("SHOW TABLES IN " ~ cfg.src_db) %}
    {% do log("Scanning " ~ all_tables.rows | length ~ " tables in " ~ cfg.src_db, info=True) %}
    {% for trow in all_tables.rows %}
      {% set tname = trow[1] %}   {# SHOW TABLES -> (namespace, tableName, isTemporary) #}
      {% if tname not in ('migration_control', 'tables_to_migrate') %}
        {% set found = namespace(hit=false) %}
        {% for crow in mig_run_query("DESCRIBE TABLE " ~ cfg.src_db ~ "." ~ tname).rows %}
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
    {{ return(candidates | sort) }}
  {% endif %}
{% endmacro %}


{# Current shape of the headers struct on a table #}
{% macro mig_headers_state(full_table) %}
  {% set headers_type = namespace(val='') %}
  {% for crow in mig_run_query("DESCRIBE TABLE " ~ full_table).rows %}
    {% if crow[0] == 'headers' and headers_type.val == '' %}
      {% set headers_type.val = crow[1] | string %}
    {% endif %}
  {% endfor %}
  {{ return({
      'htype': headers_type.val,
      'has_bigint': 'processedAt:bigint' in headers_type.val,
      'has_ts_col': 'processedAt_ts:' in headers_type.val,
      'has_final': 'processedAt:timestamp' in headers_type.val
  }) }}
{% endmacro %}


{#
  Full verification snapshot in a single round trip.
  NULL processedAt rows are counted separately on both sides: they are never
  converted (processedAt_ts stays NULL, which is correct) but they MUST be
  present in the backup, which total/null count equality proves.

  Returns:
    corrupt        rows where a converted timestamp does NOT round-trip to the bigint
    unconverted    rows with a bigint but no timestamp yet (new arrivals / pending delta)
    ts_cnt/ts_sum  converted timestamps in source (as epoch millis)
    src_bigint_cnt/src_bigint_sum   non-null bigints in source
    bkp_bigint_cnt/bkp_bigint_sum   non-null bigints in backup
    src_null / bkp_null             NULL-processedAt rows on each side
    src_total / bkp_total           row counts
#}
{% macro mig_verify_counts(src_table, bkp_table) %}
  {% set v = mig_run_query(
      "SELECT s.corrupt_cnt, s.unconverted_cnt, s.ts_cnt, s.ts_sum," ~
      "       s.bigint_cnt, s.bigint_sum, s.null_cnt, s.total_cnt," ~
      "       b.old_cnt, b.old_sum, b.null_cnt, b.total_cnt FROM " ~
      " (SELECT" ~
      "     COALESCE(SUM(CASE WHEN headers.processedAt IS NOT NULL" ~
      "                        AND headers.processedAt_ts IS NOT NULL" ~
      "                        AND unix_millis(headers.processedAt_ts) != headers.processedAt" ~
      "                  THEN 1 ELSE 0 END), 0) AS corrupt_cnt," ~
      "     COALESCE(SUM(CASE WHEN headers.processedAt IS NOT NULL" ~
      "                        AND headers.processedAt_ts IS NULL" ~
      "                  THEN 1 ELSE 0 END), 0) AS unconverted_cnt," ~
      "     COUNT(headers.processedAt_ts) AS ts_cnt," ~
      "     COALESCE(SUM(unix_millis(headers.processedAt_ts)), 0) AS ts_sum," ~
      "     COUNT(headers.processedAt) AS bigint_cnt," ~
      "     COALESCE(SUM(headers.processedAt), 0) AS bigint_sum," ~
      "     COALESCE(SUM(CASE WHEN headers.processedAt IS NULL THEN 1 ELSE 0 END), 0) AS null_cnt," ~
      "     COUNT(*) AS total_cnt" ~
      "   FROM " ~ src_table ~ ") s" ~
      " CROSS JOIN " ~
      " (SELECT COUNT(headers.processedAt) AS old_cnt," ~
      "         COALESCE(SUM(headers.processedAt), 0) AS old_sum," ~
      "         COALESCE(SUM(CASE WHEN headers.processedAt IS NULL THEN 1 ELSE 0 END), 0) AS null_cnt," ~
      "         COUNT(*) AS total_cnt" ~
      "   FROM " ~ bkp_table ~ ") b"
    ).rows[0] %}
  {{ return({
      'corrupt':        v[0] | int,
      'unconverted':    v[1] | int,
      'ts_cnt':         v[2] | int,
      'ts_sum':         v[3] | int,
      'src_bigint_cnt': v[4] | int,
      'src_bigint_sum': v[5] | int,
      'src_null':       v[6] | int,
      'src_total':      v[7] | int,
      'bkp_bigint_cnt': v[8] | int,
      'bkp_bigint_sum': v[9] | int,
      'bkp_null':       v[10] | int,
      'bkp_total':      v[11] | int
  }) }}
{% endmacro %}


{# Formatted report of the whole control table, plus totals by status #}
{% macro mig_report(cfg, title) %}
  {% do log("", info=True) %}
  {% do log("=" * 130, info=True) %}
  {% do log(title, info=True) %}
  {% do log("=" * 130, info=True) %}
  {% do log("%-40s %-12s %10s %10s %10s %8s %-20s %s" | format(
      'table_name', 'status', 'src_cnt', 'bkp_cnt', 'converted', 'pending', 'updated_at', 'last_error'), info=True) %}
  {% do log("-" * 130, info=True) %}
  {% set totals = {} %}
  {% for r in mig_run_query(
      "SELECT table_name, status, source_count, backup_count, converted_count," ~
      "       pending_count, updated_at, last_error FROM " ~ cfg.control ~
      " ORDER BY table_name").rows %}
    {% do log("%-40s %-12s %10s %10s %10s %8s %-20s %s" | format(
        r[0], r[1], r[2] if r[2] is not none else '-', r[3] if r[3] is not none else '-',
        r[4] if r[4] is not none else '-', r[5] if r[5] is not none else '-',
        r[6] | string, r[7] if r[7] is not none else '') , info=True) %}
    {% do totals.update({r[1]: totals.get(r[1], 0) + 1}) %}
  {% endfor %}
  {% do log("-" * 130, info=True) %}
  {% set parts = [] %}
  {% for s, n in totals.items() %}
    {% do parts.append(s ~ "=" ~ n) %}
  {% endfor %}
  {% do log("Totals: " ~ parts | join(', '), info=True) %}
  {% do log("=" * 130, info=True) %}
{% endmacro %}
