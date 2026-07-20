{#
  ROLLBACK — migration_rollback
  -----------------------------
  State-aware, per table (reads the live schema, not just the control table):

    A. Old bigint column still present (prepared / converting / converted):
       source data is intact - simply DROP the added headers.processedAt_ts
       field. No data movement. Status -> 'rolled_back'.

    B. Old bigint column gone (finalized, or interrupted mid-finalize):
       restore the table from its backup with CREATE OR REPLACE TABLE ... AS
       SELECT, then drop the residual processedAt_ts field so the table is
       back to its pristine pre-migration shape.
       *** Rows written AFTER the last delta backup are LOST. ***
       Because of that, restores require an explicit
         --vars 'rollback_confirm: true'

  Backups and the backup database are NEVER dropped by this macro.
  Ends with a rollback report from the control table.

  Usage:
    dbt run-operation migration_rollback --vars '{source_db_name: mydb, tables: orders}'
    dbt run-operation migration_rollback --vars '{source_db_name: mydb, tables: orders, rollback_confirm: true}'
    dbt run-operation migration_rollback --vars '{source_db_name: mydb, rollback_confirm: true}'   # all tables
#}

{% macro migration_rollback() %}
  {% if execute %}

    {% set cfg = mig_config() %}
    {% set confirm = var('rollback_confirm', false) %}
    {% set tables = mig_resolve_tables(cfg, discover='control') %}
    {% if tables | length == 0 %}
      {% do exceptions.raise_compiler_error(
          "Nothing to roll back. Control table " ~ cfg.control ~ " is empty.") %}
    {% endif %}
    {% do log("Rolling back " ~ tables | length ~ " table(s): " ~ tables | join(', '), info=True) %}

    {% set rolled_back = [] %}
    {% set skipped     = [] %}
    {% set failed      = [] %}

    {% for t in tables %}
      {% set src_table = cfg.src_db ~ '.' ~ t %}
      {% set bkp_table = cfg.backup_db ~ '.' ~ t %}
      {% do log("[" ~ loop.index ~ "/" ~ tables | length ~ "] rollback " ~ src_table, info=True) %}

      {% set status = mig_control_status(cfg, t) %}
      {% if status is none %}
        {% do log("  not in control table - never touched by the migration, skipping", info=True) %}
        {% do skipped.append(t) %}
      {% else %}
        {% set state = mig_headers_state(src_table) %}

        {% if state.has_bigint %}
          {# ----- A. Cheap rollback: original data untouched, just remove the added field ----- #}
          {% if state.has_ts_col %}
            {% do log("  old bigint intact - dropping added headers.processedAt_ts", info=True) %}
            {% do adapter.execute("ALTER TABLE " ~ src_table ~ " DROP COLUMN headers.processedAt_ts") %}
          {% else %}
            {% do log("  old bigint intact and no processedAt_ts present - nothing to undo", info=True) %}
          {% endif %}
          {% do mig_control_set(cfg, t, {
              'status': "'rolled_back'",
              'last_error': mig_sql_str("rolled back: dropped processedAt_ts, source data untouched")}) %}
          {% do rolled_back.append(t) %}
          {% do log("  ROLLED BACK (no data movement)", info=True) %}

        {% else %}
          {# ----- B. Destructive rollback: restore from backup ----- #}
          {% set bkp_exists = mig_run_query("SHOW TABLES IN " ~ cfg.backup_db ~
              " LIKE '" ~ t ~ "'").rows | length > 0 %}
          {% if not bkp_exists %}
            {% do log("  FAILED: bigint column is gone and no backup exists at " ~ bkp_table, info=True) %}
            {% do mig_control_set(cfg, t, {
                'status': "'failed'",
                'last_error': mig_sql_str("rollback failed: no backup table found")}) %}
            {% do failed.append(t) %}
          {% elif not confirm %}
            {% do exceptions.raise_compiler_error(
                "ABORT " ~ src_table ~ ": rollback requires restoring from backup " ~ bkp_table ~
                " and any rows written after the last delta backup will be LOST. " ~
                "Re-run with --vars 'rollback_confirm: true' to proceed.") %}
          {% else %}
            {% do log("  *** WARNING: restoring " ~ src_table ~ " from " ~ bkp_table ~
                " - rows written after the last delta backup will be LOST ***", info=True) %}
            {% do adapter.execute("CREATE OR REPLACE TABLE " ~ src_table ~
                " AS SELECT * FROM " ~ bkp_table) %}

            {# remove the residual migration field so the schema is pristine #}
            {% set post = mig_headers_state(src_table) %}
            {% if post.has_ts_col %}
              {% do adapter.execute("ALTER TABLE " ~ src_table ~ " DROP COLUMN headers.processedAt_ts") %}
            {% endif %}

            {# verify restored row count matches the backup #}
            {% set counts = mig_run_query(
                "SELECT s.c, b.c FROM " ~
                " (SELECT COUNT(*) AS c FROM " ~ src_table ~ ") s" ~
                " CROSS JOIN " ~
                " (SELECT COUNT(*) AS c FROM " ~ bkp_table ~ ") b").rows[0] %}
            {% do log("  restore count check -> source: " ~ counts[0] ~ " | backup: " ~ counts[1], info=True) %}
            {% if counts[0] | int != counts[1] | int %}
              {% do mig_control_set(cfg, t, {
                  'status': "'failed'",
                  'last_error': mig_sql_str("rollback restore count mismatch (source=" ~ counts[0] ~
                      ", backup=" ~ counts[1] ~ ")")}) %}
              {% do failed.append(t) %}
              {% do exceptions.raise_compiler_error(
                  "ABORT " ~ src_table ~ ": restored count (" ~ counts[0] ~
                  ") does not match backup (" ~ counts[1] ~ "). Inspect manually.") %}
            {% endif %}

            {% do mig_control_set(cfg, t, {
                'status': "'rolled_back'",
                'source_count': counts[0] | string,
                'last_error': mig_sql_str("rolled back: restored from backup; rows after last delta backup lost")}) %}
            {% do rolled_back.append(t) %}
            {% do log("  ROLLED BACK (restored " ~ counts[0] ~ " rows from backup)", info=True) %}
          {% endif %}
        {% endif %}
      {% endif %}
    {% endfor %}

    {% do log("", info=True) %}
    {% do log("migration_rollback complete: " ~ rolled_back | length ~ " rolled back, " ~
        skipped | length ~ " skipped, " ~ failed | length ~ " failed" ~
        (" (" ~ failed | join(', ') ~ ")" if failed | length > 0 else ""), info=True) %}
    {% do log("Backups in " ~ cfg.backup_db ~ " were kept - clean up manually once verified.", info=True) %}

    {% do mig_report(cfg, "ROLLBACK REPORT - " ~ cfg.src_db) %}

  {% endif %}
{% endmacro %}
