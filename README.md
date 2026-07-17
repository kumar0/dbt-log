# dbt-log

Jupyter-friendly observability scripts for a dbt project running on the Glue
adapter (interactive sessions), containerised on Fargate and triggered by Step
Functions. Copy either file into a notebook cell, or `import` it.

All scripts use `boto3.Session(profile_name="ro-prd1", region_name="eu-west-1")`
— edit the constants at the top of each file to change.

Dependencies: `boto3`, `pandas`, `plotly`.

**Setup**: see `OBSERVABILITY_SETUP.txt` for the dbt profile / Glue session
settings that make these scripts work (JSON dbt logs, `--enable-metrics`,
`--enable-observability-metrics`, continuous logging, Spark event logs), plus
Windows instructions for the Spark UI (`spark-ui/Dockerfile`).

## dbt_log_analyzer.py

Parses the dbt JSON logs from a CloudWatch log stream (`ecs/spinv/<uuid>`).

1. Edit `LOG_GROUP` at the top (the log group containing the streams).
2. Run:

```python
result = analyze("ecs/spinv/<uuid>")
result["nodes"]    # per-node table: start, end, duration, status, message
result["errors"]   # errors & warnings
result["summary"]  # run summary
```

Also renders a plotly Gantt timeline of node execution and a top-N
slowest-nodes chart.

## glue_metrics_analyzer.py

Pulls the session's custom metrics from the CloudWatch `Glue` namespace and,
when enabled on the session (`--enable-observability-metrics`, Glue 4.0+),
the `Glue Observability` namespace (worker utilization, stage/job skewness,
error categories). Works identically on Glue 4 and Glue 5.

`JobRunId` = the interactive session name from the dbt profile; `JobName`
(the auto-generated session UUID) is discovered automatically. Counters whose
named series is empty fall back to the `JobRunId=ALL` roll-up automatically.

```python
result = analyze_session("<session-name-from-dbt-profile>")   # last 24h
# or pass start=/end= datetimes (e.g. the run window from the log analyzer)
result["summary"]     # workers, autoscaling usage, utilization, skewness,
                      # heap, CPU, shuffle, S3 I/O, tasks, errors, disk spill
result["metrics"]     # every discovered metric: min/avg/max/last/total
result["timeseries"]  # raw datapoints (long format)
```

The summary includes an autoscaling usage view: time-weighted average
executors allocated, executor-hours consumed, and approximate worker-hours
including the driver (billing proxy) — not just the peak allocation.

Renders plotly time-series panels: executors (allocated vs needed), JVM heap,
CPU load, data moved per interval — plus worker utilization and skewness
panels when observability metrics exist.

Notes: CloudWatch `list_metrics` only sees metrics active in the last
~2 weeks, and 60s-resolution data is retained 15 days (pass `period=300`
for older runs).

## glue_session_log_analyzer.py

Reads the Spark driver/executor logs the session streams to CloudWatch when
continuous logging is enabled (`--enable-continuous-cloudwatch-log=true`;
add `--enable-continuous-log-filter=false` for full verbosity). Scans every
`/aws-glue*` log group for streams containing the session UUID (the `JobName`
printed by `glue_metrics_analyzer`), then extracts Spark internals:

```python
result = analyze_session_logs("<session-uuid>")   # last 24h, or pass start=/end=
result["errors"]    # WARN/ERROR lines deduplicated with counts
result["jobs"]      # Spark jobs by duration (DAGScheduler)
result["stages"]    # stage completions by duration
result["signals"]   # OOM / disk spill / GC pressure / lost executors / retries
```

Also renders a top-N slowest-stages chart.

## Spark UI (spark-ui/Dockerfile)

Glue's console Spark UI tab only exists for ETL **job runs**, not interactive
sessions — so event logs (`--enable-spark-ui=true` +
`--spark-event-logs-path=s3://.../spark-events/`) are viewed with a local
Spark History Server:

```bash
docker build -t glue-sparkui spark-ui/
# Option A: sync logs locally first (most reliable)
aws s3 sync s3://<bucket>/spark-events ./spark-events --profile ro-prd1
docker run --rm -p 18080:18080 -v "$PWD/spark-events:/logs:ro" -e LOG_DIR=file:/logs glue-sparkui
# then open http://localhost:18080
```

Windows/PowerShell commands and a direct-from-S3 option are in
`OBSERVABILITY_SETUP.txt` (STEP 4).

## Offline self-tests

Each script has a `__main__` block that checks the parsing/summary logic
without touching AWS:

```bash
python dbt_log_analyzer.py
python glue_metrics_analyzer.py
python glue_session_log_analyzer.py
```
