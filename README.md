# dbt-log

Jupyter-friendly observability scripts for a dbt project running on the Glue
adapter (interactive sessions), containerised on Fargate and triggered by Step
Functions. Copy either file into a notebook cell, or `import` it.

Both scripts use `boto3.Session(profile_name="roprd1", region_name="eu-west-1")`
— edit the constants at the top of each file to change.

Dependencies: `boto3`, `pandas`, `plotly`.

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

Pulls the session's custom metrics from the CloudWatch `Glue` namespace.
`JobRunId` = the interactive session name from the dbt profile; `JobName`
(the auto-generated session UUID) is discovered automatically.

```python
result = analyze_session("<session-name-from-dbt-profile>")   # last 24h
# or pass start=/end= datetimes (e.g. the run window from the log analyzer)
result["summary"]     # workers, heap, CPU, shuffle, S3 I/O, tasks, disk spill
result["metrics"]     # every discovered metric: min/avg/max/last/total
result["timeseries"]  # raw datapoints (long format)
```

Also renders 2x2 plotly time-series panels: executors, JVM heap, CPU load,
data moved per interval.

Note: CloudWatch `list_metrics` only sees metrics active in the last ~2 weeks.

## Offline self-tests

Each script has a `__main__` block that checks the parsing/summary logic
without touching AWS:

```bash
python dbt_log_analyzer.py
python glue_metrics_analyzer.py
```
