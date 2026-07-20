{#
  PHASE 2 — migration_convert
  ---------------------------
  Re-runnable as many times as needed while tables keep receiving data.
  Each run, per table:
    1. DELTA BACKUP — append to the backup only the rows that arrived since
       the last backup:
         - non-null processedAt: watermark, rows with
           headers.processedAt > MAX(headers.processedAt) already in backup
         - NULL processedAt: watermark can never see these, so they are
           captured exactly with EXCEPT ALL over the NULL-processedAt subset
    2. DELTA CONVERT — UPDATE only rows not yet converted:
         headers.processedAt IS NOT NULL AND (processedAt_ts IS NULL OR
         unix_millis(processedAt_ts) != processedAt)
       Rows with NULL processedAt are intentionally untouched:
       their processedAt_ts stays NULL, which is the correct converted value.
    3. VERIFY (single round trip):
         - corrupt: converted timestamps that do not round-trip -> hard FAIL
         - unconverted rows / count-sum drift vs backup: treated as delta
           that arrived during the run -> table stays 'converting', re-run
         - clean: status 'converted'

  Assumptions / limitations (both are caught by verification, never silent):
    - watermark uses strict '>': a late row whose processedAt EQUALS the
      current backup max is not delta-picked; verification then keeps the
      table in 'converting' with a count mismatch for manual inspection
    - EXCEPT ALL (NULL-row delta) requires comparable column types; tables
      with map-type columns cannot use it (Spark cannot compare maps)

  Usage:
    dbt run-operation migration_convert --vars '{source_db_name: mydb}'                    # all prepared tables
    dbt run-operation migration_convert --vars '{source_db_name: mydb, tables: orders}'    # one table
    dbt run-operation migration_convert --vars '{source_db_name: mydb, tables: "a,b,c"}'   # subset
#}

{% macro migration_convert() %}
  {% if execute %}

    {% set cfg = mig_config() %}
    {% set tables = mig_resolve_tables(cfg, discover='control') %}
    {% if tables | length == 0 %}
      {% do exceptions.raise_compiler_error(
          "No tables to convert. Run migration_prepare first (control table " ~
          cfg.control ~ " is empty).") %}
    {% endif %}
    {% do log("Converting " ~ tables | length ~ " table(s): " ~ tables | join(', '), info=True) %}

    {% set converted = [] %}
    {% set pending   = [] %}
    {% set skipped   = [] %}

    {% for t in tables %}
      {% set src_table = cfg.src_db ~ '.' ~ t %}
      {% set bkp_table = cfg.backup_db ~ '.' ~ t %}
      {% do log("[" ~ loop.index ~ "/" ~ tables | length ~ "] convert " ~ src_table, info=True) %}

      {% set status = mig_control_status(cfg, t) %}
      {% if status is none %}
        {% do exceptions.raise_compiler_error(
            "ABORT " ~ src_table ~ ": not registered in control table. Run migration_prepare first.") %}
      {% elif status == 'finalized' %}
        {% do log("  already finalized, skipping", info=True) %}
        {% do skipped.append(t) %}
      {% else %}

        {% set state = mig_headers_state(src_table) %}
        {% if not state.has_bigint %}
          {% do log("  bigint column already gone (interrupted finalize?) - nothing to convert, run migration_finalize", info=True) %}
          {% do skipped.append(t) %}
        {% elif not state.has_ts_col %}
          {% do exceptions.raise_compiler_error(
              "ABORT " ~ src_table ~ ": headers.processedAt_ts missing. Run migration_prepare first.") %}
        {% else %}

          {% do mig_control_set(cfg, t, {'status': "'converting'", 'last_convert_time': 'current_timestamp()'}) %}

          {# ----- 1a. Delta backup: non-null processedAt via watermark ----- #}
          {% set bstats = mig_run_query(
              "SELECT COUNT(*), MAX(headers.processedAt) FROM " ~ bkp_table).rows[0] %}
          {% set bkp_rows = bstats[0] | int %}
          {% set wm = bstats[1] %}
          {% if wm is none %}
            {# empty backup, or backup holds only NULL-processedAt rows: every non-null row is new #}
            {% set delta_cond = "headers.processedAt IS NOT NULL" %}
          {% else %}
            {% set delta_cond = "headers.processedAt > " ~ wm %}
          {% endif %}
          {% set delta_cnt = mig_run_query("SELECT COUNT(*) FROM " ~ src_table ~
              " WHERE " ~ delta_cond).rows[0][0] | int %}
          {% if delta_cnt > 0 %}
            {% do log("  delta backup: appending " ~ delta_cnt ~ " new row(s) (watermark " ~
                (wm if wm is not none else 'none') ~ ")", info=True) %}
            {% do adapter.execute("INSERT INTO " ~ bkp_table ~
                " SELECT * FROM " ~ src_table ~ " WHERE " ~ delta_cond) %}
          {% else %}
            {% do log("  delta backup: no new non-null rows", info=True) %}
          {% endif %}

          {# ----- 1b. Delta backup: NULL processedAt rows (invisible to the watermark) ----- #}
          {% set null_delta = mig_run_query(
              "SELECT COUNT(*) FROM (" ~
              "  SELECT * FROM " ~ src_table ~ " WHERE headers.processedAt IS NULL" ~
              "  EXCEPT ALL" ~
              "  SELECT * FROM " ~ bkp_table ~ " WHERE headers.processedAt IS NULL)").rows[0][0] | int %}
          {% if null_delta > 0 %}
            {% do log("  delta backup: appending " ~ null_delta ~ " new NULL-processedAt row(s)", info=True) %}
            {% do adapter.execute("INSERT INTO " ~ bkp_table ~
                " SELECT * FROM " ~ src_table ~ " WHERE headers.processedAt IS NULL" ~
                " EXCEPT ALL" ~
                " SELECT * FROM " ~ bkp_table ~ " WHERE headers.processedAt IS NULL") %}
          {% endif %}

          {# ----- 2. Delta convert (idempotent: only unconverted/drifted rows are touched) ----- #}
          {% set to_convert = mig_run_query("SELECT COUNT(*) FROM " ~ src_table ~
              " WHERE headers.processedAt IS NOT NULL" ~
              "   AND (headers.processedAt_ts IS NULL" ~
              "        OR unix_millis(headers.processedAt_ts) != headers.processedAt)").rows[0][0] | int %}
          {% if to_convert > 0 %}
            {% do log("  converting " ~ to_convert ~ " row(s) bigint -> timestamp", info=True) %}
            {% do adapter.execute("UPDATE " ~ src_table ~
                " SET headers.processedAt_ts = timestamp_millis(headers.processedAt)" ~
                " WHERE headers.processedAt IS NOT NULL" ~
                "   AND (headers.processedAt_ts IS NULL" ~
                "        OR unix_millis(headers.processedAt_ts) != headers.processedAt)") %}
          {% else %}
            {% do log("  nothing to convert (all rows already converted)", info=True) %}
          {% endif %}

          {# ----- 3. Verification ----- #}
          {% set v = mig_verify_counts(src_table, bkp_table) %}
          {% do log("  verify -> corrupt=" ~ v.corrupt ~ ", unconverted=" ~ v.unconverted ~
              ", src(bigint cnt=" ~ v.src_bigint_cnt ~ ", sum=" ~ v.src_bigint_sum ~
              ", null=" ~ v.src_null ~ ", total=" ~ v.src_total ~ ")" ~
              " | bkp(bigint cnt=" ~ v.bkp_bigint_cnt ~ ", sum=" ~ v.bkp_bigint_sum ~
              ", null=" ~ v.bkp_null ~ ", total=" ~ v.bkp_total ~ ")", info=True) %}

          {% if v.corrupt > 0 %}
            {% do mig_control_set(cfg, t, {
                'status': "'failed'",
                'pending_count': v.unconverted | string,
                'last_error': mig_sql_str("convert: " ~ v.corrupt ~ " row(s) failed bigint->timestamp round-trip")}) %}
            {% do exceptions.raise_compiler_error(
                "ABORT " ~ src_table ~ ": " ~ v.corrupt ~
                " row(s) failed bigint->timestamp verification. Old column NOT dropped.") %}
          {% endif %}

          {% set backup_in_sync = (v.src_bigint_cnt == v.bkp_bigint_cnt and
                                   v.src_bigint_sum == v.bkp_bigint_sum and
                                   v.src_null == v.bkp_null and
                                   v.src_total == v.bkp_total) %}

          {% set common = {
              'source_count': v.src_total | string,
              'backup_count': v.bkp_total | string,
              'converted_count': v.ts_cnt | string,
              'pending_count': v.unconverted | string,
              'last_delta_rows': (delta_cnt + null_delta) | string,
              'last_convert_time': 'current_timestamp()'} %}

          {% if v.unconverted == 0 and backup_in_sync %}
            {% do common.update({'status': "'converted'", 'last_error': 'NULL'}) %}
            {% do mig_control_set(cfg, t, common) %}
            {% do converted.append(t) %}
            {% do log("  CONVERTED (delta backed up: " ~ (delta_cnt + null_delta) ~
                ", converted this run: " ~ to_convert ~ ")", info=True) %}
          {% else %}
            {# rows arrived between our backup/update/verify statements - not an
               error on a live table; stays 'converting', next run picks them up #}
            {% do common.update({'status': "'converting'",
                'last_error': mig_sql_str("convert: in-flight delta detected (unconverted=" ~
                    v.unconverted ~ ", backup_in_sync=" ~ backup_in_sync ~ ") - re-run migration_convert")}) %}
            {% do mig_control_set(cfg, t, common) %}
            {% do pending.append(t) %}
            {% do log("  STILL CONVERTING - new rows arrived during the run, re-run migration_convert", info=True) %}
          {% endif %}

        {% endif %}
      {% endif %}
    {% endfor %}

    {% do log("", info=True) %}
    {% do log("migration_convert complete: " ~ converted | length ~ " converted, " ~
        pending | length ~ " still converting" ~
        (" (" ~ pending | join(', ') ~ ")" if pending | length > 0 else "") ~ ", " ~
        skipped | length ~ " skipped", info=True) %}

  {% endif %}
{% endmacro %}
