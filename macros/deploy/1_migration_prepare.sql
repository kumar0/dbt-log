{#
  PHASE 1 — migration_prepare
  ---------------------------
  For each target table:
    - ensure backup database + control table exist
    - add headers.processedAt_ts (timestamp) to the SOURCE table first
    - then take the backup (CTAS), so backup schema matches source and later
      delta rows can be appended with a plain INSERT ... SELECT *
    - verify the backup, register the table as 'prepared' in the control table

  Idempotent: safe to re-run; existing backups are never overwritten and
  tables already prepared (or fully migrated) are skipped.

  Usage:
    # whole database (auto-discovers tables whose headers struct has processedAt:bigint)
    dbt run-operation migration_prepare --vars '{source_db_name: mydb, backup_s3_location: "s3://bucket/path"}'

    # subset
    dbt run-operation migration_prepare --vars '{source_db_name: mydb, backup_s3_location: "s3://bucket/path", tables: "orders,events"}'
#}

{% macro migration_prepare() %}
  {% if execute %}

    {% set cfg = mig_config() %}
    {% set s3_location = var('backup_s3_location', none) %}

    {# ---------- 0. Ensure backup database exists ---------- #}
    {% set db_exists = mig_run_query("SHOW DATABASES LIKE '" ~ cfg.backup_db ~ "'").rows | length > 0 %}
    {% if not db_exists %}
      {% if s3_location %}
        {% do log("Creating backup database: " ~ cfg.backup_db ~ " at " ~ s3_location, info=True) %}
        {% do adapter.execute("CREATE DATABASE " ~ cfg.backup_db ~ " LOCATION '" ~ s3_location ~ "'") %}
      {% else %}
        {% do exceptions.raise_compiler_error(
            "S3 location must be provided via --vars 'backup_s3_location: s3://your-bucket/path'") %}
      {% endif %}
    {% else %}
      {% do log("Backup database already exists: " ~ cfg.backup_db, info=True) %}
    {% endif %}

    {# ---------- 1. Ensure control table exists ---------- #}
    {% do log("Control table: " ~ cfg.control, info=True) %}
    {% do mig_create_control_table(cfg) %}

    {# ---------- 2. Resolve target tables ---------- #}
    {% set tables = mig_resolve_tables(cfg, discover='scan') %}
    {% do log("Preparing " ~ tables | length ~ " table(s): " ~ tables | join(', '), info=True) %}

    {% set prepared = [] %}
    {% set skipped  = [] %}

    {% for t in tables %}
      {% set src_table = cfg.src_db ~ '.' ~ t %}
      {% set bkp_table = cfg.backup_db ~ '.' ~ t %}
      {% do log("[" ~ loop.index ~ "/" ~ tables | length ~ "] prepare " ~ src_table, info=True) %}

      {% set status = mig_control_status(cfg, t) %}
      {% set state = mig_headers_state(src_table) %}

      {# ----- state sanity ----- #}
      {% if state.has_final and state.has_ts_col %}
        {% do exceptions.raise_compiler_error(
            "ABORT " ~ src_table ~ ": ambiguous state - headers has both " ~
            "processedAt:timestamp and processedAt_ts. Resolve manually.") %}
      {% elif state.has_final and not state.has_bigint %}
        {% do log("  already fully migrated (processedAt is timestamp), skipping", info=True) %}
        {% do skipped.append(t) %}
        {% if status is none %}
          {% do mig_control_set(cfg, t, {'status': "'finalized'"}) %}
        {% endif %}
      {% elif not state.has_bigint and not state.has_ts_col %}
        {% do exceptions.raise_compiler_error(
            "ABORT " ~ src_table ~ ": headers has no processedAt bigint or " ~
            "processedAt_ts field (headers type: " ~ state.htype ~ ")") %}
      {% else %}

        {% if status in ['prepared', 'converting', 'converted'] %}
          {% do log("  already " ~ status ~ " in control table, skipping (backup kept as-is)", info=True) %}
          {% do skipped.append(t) %}
        {% else %}

          {# ----- 3. Add processedAt_ts to SOURCE first (so backup schema matches) ----- #}
          {% if not state.has_ts_col %}
            {% do log("  adding headers.processedAt_ts to source", info=True) %}
            {% do adapter.execute("ALTER TABLE " ~ src_table ~ " ADD COLUMN headers.processedAt_ts timestamp") %}
          {% else %}
            {% do log("  headers.processedAt_ts already present on source, skipping add", info=True) %}
          {% endif %}

          {# ----- 4. Backup (never overwrite an existing backup) ----- #}
          {% set bkp_exists = mig_run_query("SHOW TABLES IN " ~ cfg.backup_db ~
              " LIKE '" ~ t ~ "'").rows | length > 0 %}
          {% if bkp_exists %}
            {% do log("  backup already exists: " ~ bkp_table ~ " (keeping it, NOT overwriting)", info=True) %}
            {# backup may pre-date the ts column (e.g. taken by the original macro):
               align its schema so delta INSERT ... SELECT * keeps working #}
            {% set bkp_state = mig_headers_state(bkp_table) %}
            {% if not bkp_state.has_ts_col and bkp_state.has_bigint %}
              {% do log("  aligning backup schema: adding headers.processedAt_ts", info=True) %}
              {% do adapter.execute("ALTER TABLE " ~ bkp_table ~ " ADD COLUMN headers.processedAt_ts timestamp") %}
            {% endif %}
          {% else %}
            {% do log("  creating backup: " ~ bkp_table, info=True) %}
            {% do adapter.execute("CREATE TABLE " ~ bkp_table ~ " AS SELECT * FROM " ~ src_table) %}
          {% endif %}

          {# ----- 5. Verify backup row count (source may keep growing: backup <= source is OK,
                       the convert phase backs up the delta; backup > source is a hard error) ----- #}
          {% set counts = mig_run_query(
              "SELECT s.c AS src_cnt, b.c AS bkp_cnt FROM " ~
              " (SELECT COUNT(*) AS c FROM " ~ src_table ~ ") s" ~
              " CROSS JOIN " ~
              " (SELECT COUNT(*) AS c FROM " ~ bkp_table ~ ") b").rows[0] %}
          {% do log("  count check -> source: " ~ counts[0] ~ " | backup: " ~ counts[1], info=True) %}
          {% if counts[1] | int > counts[0] | int %}
            {% do mig_control_set(cfg, t, {
                'status': "'failed'",
                'last_error': mig_sql_str("prepare: backup has MORE rows than source (" ~ counts[1] ~ " > " ~ counts[0] ~ ")")}) %}
            {% do exceptions.raise_compiler_error(
                "ABORT " ~ src_table ~ ": backup count (" ~ counts[1] ~
                ") exceeds source count (" ~ counts[0] ~ "). Inspect backup manually.") %}
          {% elif counts[1] | int < counts[0] | int %}
            {% do log("  note: source grew by " ~ (counts[0] | int - counts[1] | int) ~
                " row(s) since backup - the convert phase will back up the delta", info=True) %}
          {% endif %}

          {# ----- 6. Register as prepared ----- #}
          {% do mig_control_set(cfg, t, {
              'status': "'prepared'",
              'source_count': counts[0] | string,
              'backup_count': counts[1] | string,
              'prepare_time': 'current_timestamp()',
              'last_error': 'NULL'}) %}
          {% do prepared.append(t) %}
          {% do log("  prepared", info=True) %}

        {% endif %}
      {% endif %}
    {% endfor %}

    {% do log("", info=True) %}
    {% do log("migration_prepare complete: " ~ prepared | length ~ " prepared, " ~
        skipped | length ~ " skipped", info=True) %}

  {% endif %}
{% endmacro %}
