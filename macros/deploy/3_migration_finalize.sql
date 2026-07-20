{#
  PHASE 3 — migration_finalize
  ----------------------------
  Per table, only after migration_convert reports 'converted':
    1. Fresh pre-flight re-verification (does NOT trust stored counts):
         - zero corrupt rows (every converted timestamp round-trips)
         - zero unconverted rows (every non-null bigint has its timestamp;
           NULL-processedAt rows are exempt - NULL is their correct value)
         - backup fully in sync (bigint count/sum, NULL count, total count)
       Any failure -> table marked 'failed', columns untouched, next table.
    2. ALTER TABLE ... DROP COLUMN headers.processedAt        (the old bigint)
    3. ALTER TABLE ... RENAME COLUMN headers.processedAt_ts TO processedAt
    4. Post-check via DESCRIBE, then status 'finalized'.
  Also recovers tables interrupted between drop and rename (only
  processedAt_ts present -> just does the rename).

  Ends with a full verification report from the control table.

  Usage:
    dbt run-operation migration_finalize --vars '{source_db_name: mydb}'
    dbt run-operation migration_finalize --vars '{source_db_name: mydb, tables: "orders,events"}'
#}

{% macro migration_finalize() %}
  {% if execute %}

    {% set cfg = mig_config() %}
    {% set tables = mig_resolve_tables(cfg, discover='control') %}
    {% if tables | length == 0 %}
      {% do exceptions.raise_compiler_error(
          "No tables to finalize. Control table " ~ cfg.control ~ " is empty.") %}
    {% endif %}
    {% do log("Finalizing " ~ tables | length ~ " table(s): " ~ tables | join(', '), info=True) %}

    {% set finalized = [] %}
    {% set skipped   = [] %}
    {% set failed    = [] %}

    {% for t in tables %}
      {% set src_table = cfg.src_db ~ '.' ~ t %}
      {% set bkp_table = cfg.backup_db ~ '.' ~ t %}
      {% do log("[" ~ loop.index ~ "/" ~ tables | length ~ "] finalize " ~ src_table, info=True) %}

      {% set status = mig_control_status(cfg, t) %}
      {% set state = mig_headers_state(src_table) %}
      {% set interrupted = state.has_ts_col and not state.has_bigint %}

      {% if state.has_final and not state.has_ts_col and not state.has_bigint %}
        {% do log("  already finalized in schema, skipping", info=True) %}
        {% if status != 'finalized' %}
          {% do mig_control_set(cfg, t, {'status': "'finalized'", 'finalize_time': 'current_timestamp()'}) %}
        {% endif %}
        {% do skipped.append(t) %}

      {% elif status != 'converted' and not interrupted %}
        {% do log("  SKIPPED: control status is '" ~ status ~ "' (needs 'converted') - " ~
            "run migration_convert until it reports converted", info=True) %}
        {% do skipped.append(t) %}

      {% else %}

        {% if state.has_bigint %}
          {# ----- 1. Pre-flight re-verification (fresh) ----- #}
          {% set v = mig_verify_counts(src_table, bkp_table) %}
          {% do log("  pre-flight -> corrupt=" ~ v.corrupt ~ ", unconverted=" ~ v.unconverted ~
              ", src(bigint cnt=" ~ v.src_bigint_cnt ~ ", sum=" ~ v.src_bigint_sum ~
              ", null=" ~ v.src_null ~ ", total=" ~ v.src_total ~ ")" ~
              " | bkp(bigint cnt=" ~ v.bkp_bigint_cnt ~ ", sum=" ~ v.bkp_bigint_sum ~
              ", null=" ~ v.bkp_null ~ ", total=" ~ v.bkp_total ~ ")", info=True) %}

          {% set problems = [] %}
          {% if v.corrupt > 0 %}
            {% do problems.append(v.corrupt ~ " corrupt row(s) (timestamp does not round-trip)") %}
          {% endif %}
          {% if v.unconverted > 0 %}
            {% do problems.append(v.unconverted ~ " unconverted row(s)") %}
          {% endif %}
          {% if v.src_bigint_cnt != v.bkp_bigint_cnt or v.src_bigint_sum != v.bkp_bigint_sum %}
            {% do problems.append("bigint count/sum mismatch vs backup") %}
          {% endif %}
          {% if v.src_null != v.bkp_null or v.src_total != v.bkp_total %}
            {% do problems.append("NULL/total count mismatch vs backup (src null=" ~ v.src_null ~
                "/total=" ~ v.src_total ~ ", bkp null=" ~ v.bkp_null ~ "/total=" ~ v.bkp_total ~ ")") %}
          {% endif %}

          {% if problems | length > 0 %}
            {% set msg = "finalize pre-flight failed: " ~ problems | join('; ') %}
            {% do log("  FAILED pre-flight: " ~ msg ~ " - columns NOT touched", info=True) %}
            {% do mig_control_set(cfg, t, {'status': "'failed'", 'last_error': mig_sql_str(msg)}) %}
            {% do failed.append(t) %}
            {% set state = none %}   {# signal: do not proceed with drop/rename #}
          {% endif %}
        {% else %}
          {% do log("  bigint column already dropped (interrupted finalize) - completing rename", info=True) %}
        {% endif %}

        {% if state is not none %}
          {# ----- 2. Drop the old bigint column ----- #}
          {% if state.has_bigint %}
            {% do log("  dropping headers.processedAt (bigint)", info=True) %}
            {% do adapter.execute("ALTER TABLE " ~ src_table ~ " DROP COLUMN headers.processedAt") %}
          {% endif %}

          {# ----- 3. Rename processedAt_ts -> processedAt ----- #}
          {% do log("  renaming headers.processedAt_ts -> headers.processedAt", info=True) %}
          {% do adapter.execute("ALTER TABLE " ~ src_table ~
              " RENAME COLUMN headers.processedAt_ts TO processedAt") %}

          {# ----- 4. Post-check ----- #}
          {% set post = mig_headers_state(src_table) %}
          {% if post.has_final and not post.has_ts_col and not post.has_bigint %}
            {% do mig_control_set(cfg, t, {
                'status': "'finalized'",
                'pending_count': '0',
                'finalize_time': 'current_timestamp()',
                'last_error': 'NULL'}) %}
            {% do finalized.append(t) %}
            {% do log("  FINALIZED", info=True) %}
          {% else %}
            {% do mig_control_set(cfg, t, {
                'status': "'failed'",
                'last_error': mig_sql_str("finalize post-check failed, headers type: " ~ post.htype)}) %}
            {% do failed.append(t) %}
            {% do exceptions.raise_compiler_error(
                "ABORT " ~ src_table ~ ": post-finalize schema check failed (headers type: " ~
                post.htype ~ "). Inspect manually before continuing.") %}
          {% endif %}
        {% endif %}

      {% endif %}
    {% endfor %}

    {% do log("", info=True) %}
    {% do log("migration_finalize complete: " ~ finalized | length ~ " finalized, " ~
        skipped | length ~ " skipped, " ~ failed | length ~ " failed" ~
        (" (" ~ failed | join(', ') ~ ")" if failed | length > 0 else ""), info=True) %}

    {# ---------- Verification report ---------- #}
    {% do mig_report(cfg, "MIGRATION VERIFICATION REPORT - " ~ cfg.src_db) %}

  {% endif %}
{% endmacro %}
