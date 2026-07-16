"""
Glue interactive-session metrics analyzer (Jupyter-friendly).

Pulls the custom metrics a Glue interactive session publishes to CloudWatch
and summarises the useful ones: workers used, JVM heap, CPU load, shuffle,
S3 I/O, task counts, disk spill — plus, when enabled, the Glue Observability
metrics (worker utilization, stage/job skewness, error categories).

Namespaces read (Glue 4.0 and 5.0 publish the same names):
  - "Glue"              profiling metrics  (--enable-metrics)
  - "Glue Observability" observability     (--enable-observability-metrics)

Dimension model (as seen in the CloudWatch console):
  - JobRunId = the interactive session NAME you set in the dbt profile
  - JobName  = the auto-generated session UUID (discovered automatically here,
               so you never need to know it)
  - each JobName also publishes a JobRunId="ALL" roll-up series; some counters
    only carry data on the ALL series, so the summary falls back to it when
    the named series is empty or all zeros.

Usage in a Jupyter cell:

    result = analyze_session("dpiibc_avqdf_position_identifier_prd1_17062026")

    # or with an explicit window (e.g. the run window from dbt_log_analyzer):
    from datetime import datetime, timezone
    result = analyze_session(
        "dpiibc_avqdf_position_identifier_prd1_17062026",
        start=datetime(2026, 7, 15, 18, 0, tzinfo=timezone.utc),
        end=datetime(2026, 7, 15, 22, 0, tzinfo=timezone.utc),
    )

    result["summary"]     # key numbers with plain-English labels
    result["metrics"]     # every discovered metric: min/avg/max/last/total
    result["timeseries"]  # long-format DataFrame of all datapoints

Note: CloudWatch list_metrics only returns metrics that received data in the
last ~2 weeks; older sessions need explicit metric names. 60s-resolution data
is retained 15 days — pass period=300 for older runs.

Requires: boto3, pandas, plotly
"""

from datetime import datetime, timedelta, timezone

import boto3
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# ---------------------------------------------------------------- config ----
AWS_PROFILE = "roprd1"
AWS_REGION = "eu-west-1"
NAMESPACES = ["Glue", "Glue Observability"]

# Validated categorical palette (light surface)
C_BLUE, C_AQUA, C_YELLOW, C_VIOLET, C_RED, C_ORANGE = (
    "#2a78d6", "#1baf7a", "#eda100", "#4a3aa7", "#e34948", "#eb6834",
)

_CHART_LAYOUT = dict(
    template="plotly_white",
    paper_bgcolor="#fcfcfb",
    plot_bgcolor="#fcfcfb",
    font=dict(family='system-ui, -apple-system, "Segoe UI", sans-serif', color="#0b0b0b"),
)


def _session():
    return boto3.Session(profile_name=AWS_PROFILE, region_name=AWS_REGION)


def fmt_bytes(n):
    if n is None or pd.isna(n):
        return None
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(n) < 1024:
            return f"{n:,.2f} {unit}"
        n /= 1024
    return f"{n:,.2f} PB"


# ------------------------------------------------------------- discovery ----
def discover_metrics(session_name):
    """Find every metric for this session across both namespaces (incl. the
    ALL roll-up). Returns dicts: {"namespace", "name", "dimensions",
    "job_name", "job_run_id", "type"}.
    """
    cw = _session().client("cloudwatch")
    paginator = cw.get_paginator("list_metrics")

    def _list(namespace, dimension_filters):
        found = []
        for page in paginator.paginate(Namespace=namespace, Dimensions=dimension_filters):
            for m in page["Metrics"]:
                dims = {d["Name"]: d["Value"] for d in m["Dimensions"]}
                found.append(
                    {
                        "namespace": namespace,
                        "name": m["MetricName"],
                        "dimensions": m["Dimensions"],
                        "job_name": dims.get("JobName"),
                        "job_run_id": dims.get("JobRunId"),
                        "type": dims.get("Type"),  # "count" or "gauge" (may be absent)
                    }
                )
        return found

    metrics = []
    for ns in NAMESPACES:
        metrics += _list(ns, [{"Name": "JobRunId", "Value": session_name}])
    if not metrics:
        raise RuntimeError(
            f"No metrics found with JobRunId='{session_name}' in namespaces {NAMESPACES}. "
            "Check the session name; note list_metrics only covers the last ~2 weeks."
        )

    job_names = sorted({m["job_name"] for m in metrics if m["job_name"]})
    print(f"Discovered JobName(s) (auto-generated session UUID): {job_names}")

    # add the ALL roll-up series for the same JobName(s)
    seen = {(m["namespace"], m["name"], m["job_name"], m["job_run_id"]) for m in metrics}
    for ns in NAMESPACES:
        for jn in job_names:
            for m in _list(ns, [{"Name": "JobName", "Value": jn}, {"Name": "JobRunId", "Value": "ALL"}]):
                key = (m["namespace"], m["name"], m["job_name"], m["job_run_id"])
                if key not in seen:
                    seen.add(key)
                    metrics.append(m)

    by_ns = pd.Series([m["namespace"] for m in metrics]).value_counts().to_dict()
    print(f"Discovered {len(metrics)} metric series: {by_ns}")
    if "Glue Observability" not in by_ns:
        print(
            "No observability metrics found - enable them on the session with "
            '"--enable-observability-metrics": "true" (Glue 4.0+) to get worker '
            "utilization, skewness and error-category metrics."
        )
    return metrics


# ----------------------------------------------------------------- fetch ----
def _metric_type(m):
    if m["type"] in ("count", "gauge"):
        return m["type"]
    # observability metrics sometimes omit the Type dimension
    n = m["name"]
    return "count" if (".error." in n or ".succeed." in n or ".aggregate." in n) else "gauge"


def fetch_metric_data(metrics, start, end, period=60):
    """Batched get_metric_data for all series. Returns a long-format DataFrame:
    columns = ts, namespace, metric, job_run_id, stat, value.
    Counters are fetched as Sum; gauges as Average + Maximum.
    """
    cw = _session().client("cloudwatch")
    queries, meta = [], {}
    for m in metrics:
        stats = ["Sum"] if _metric_type(m) == "count" else ["Average", "Maximum"]
        for stat in stats:
            qid = f"q{len(queries)}"
            meta[qid] = (m["namespace"], m["name"], m["job_run_id"], stat)
            queries.append(
                {
                    "Id": qid,
                    "MetricStat": {
                        "Metric": {
                            "Namespace": m["namespace"],
                            "MetricName": m["name"],
                            "Dimensions": m["dimensions"],
                        },
                        "Period": period,
                        "Stat": stat,
                    },
                    "ReturnData": True,
                }
            )

    rows = []
    for i in range(0, len(queries), 400):  # API limit is 500 queries per call
        chunk = queries[i : i + 400]
        token = None
        while True:
            kwargs = dict(MetricDataQueries=chunk, StartTime=start, EndTime=end)
            if token:
                kwargs["NextToken"] = token
            resp = cw.get_metric_data(**kwargs)
            for res in resp["MetricDataResults"]:
                ns, name, run_id, stat = meta[res["Id"]]
                for ts, val in zip(res["Timestamps"], res["Values"]):
                    rows.append(
                        {"ts": ts, "namespace": ns, "metric": name,
                         "job_run_id": run_id, "stat": stat, "value": val}
                    )
            token = resp.get("NextToken")
            if not token:
                break

    df = pd.DataFrame(rows)
    if not df.empty:
        df["ts"] = pd.to_datetime(df["ts"], utc=True)
        df = df.sort_values("ts").reset_index(drop=True)
    return df


# ------------------------------------------------------------- selection ----
def _series_vals(ts_df, name_contains, stat, session_name):
    """Datapoints for one metric+stat, preferring the named series; falls back
    to the ALL roll-up when the named series is missing or all zeros (some
    counters only carry data on ALL)."""
    sub = ts_df[ts_df["metric"].str.contains(name_contains, regex=False) & (ts_df["stat"] == stat)]
    if sub.empty:
        return sub
    named = sub[sub["job_run_id"] == session_name]
    if not named.empty and named["value"].abs().sum() > 0:
        return named
    alt = sub[sub["job_run_id"] != session_name]
    if not alt.empty and alt["value"].abs().sum() > 0:
        return alt
    return named if not named.empty else sub


def _pick(ts_df, name_contains, stat, session_name, agg):
    s = _series_vals(ts_df, name_contains, stat, session_name)
    return None if s.empty else getattr(s["value"], agg)()


# --------------------------------------------------------------- tables -----
def build_metrics_table(ts_df):
    """Per (namespace, metric, series): min / avg / max / last / total."""
    if ts_df.empty:
        return pd.DataFrame()
    rows = []
    for (ns, metric, run_id), grp in ts_df.groupby(["namespace", "metric", "job_run_id"]):
        avg = grp[grp["stat"].isin(["Average", "Sum"])]["value"]
        mx = grp[grp["stat"] == "Maximum"]["value"]
        total = grp[grp["stat"] == "Sum"]["value"].sum() if (grp["stat"] == "Sum").any() else None
        rows.append(
            {
                "namespace": ns,
                "metric": metric,
                "series": run_id,
                "datapoints": len(avg),
                "min": avg.min(),
                "avg": avg.mean(),
                "max": mx.max() if not mx.empty else avg.max(),
                "last": avg.iloc[-1] if not avg.empty else None,
                "total (sum)": total,
            }
        )
    return pd.DataFrame(rows).sort_values(["namespace", "metric", "series"]).reset_index(drop=True)


def build_session_summary(ts_df, session_name, start, end, period=60):
    """Key numbers, plain-English labels."""
    p = lambda *a: _pick(ts_df, *a)
    max_execs = p("numberAllExecutors", "Maximum", session_name, "max")
    needed = p("numberMaxNeededExecutors", "Maximum", session_name, "max")

    # --- autoscaling usage: how many executors were actually held over time
    alloc = _series_vals(ts_df, "numberAllExecutors", "Average", session_name)
    avg_execs = exec_hours = active_hours = None
    if not alloc.empty:
        per_ts = alloc.groupby("ts")["value"].mean()          # one value per interval
        avg_execs = per_ts.mean()                              # time-weighted average
        exec_hours = per_ts.sum() * period / 3600              # integral: executor-hours
        active_hours = len(per_ts) * period / 3600             # intervals with data

    rows = [
        ("Window analysed", f"{start:%Y-%m-%d %H:%M} - {end:%Y-%m-%d %H:%M} UTC"),
        ("Session active in window",
         None if active_hours is None else str(timedelta(hours=active_hours)).split(".")[0]),
        ("Max executors allocated", max_execs),
        ("Avg executors allocated (time-weighted)",
         None if avg_execs is None else f"{avg_execs:.1f}"),
        ("Executor-hours consumed (approx)",
         None if exec_hours is None else f"{exec_hours:,.1f}"),
        ("Worker-hours incl. driver (approx, billing proxy)",
         None if exec_hours is None else f"{exec_hours + active_hours:,.1f}"),
        ("Max executors needed", needed),
        ("Over/under-provisioned",
         None if None in (max_execs, needed)
         else f"{'over' if max_execs > needed else 'under' if max_execs < needed else 'right-sized'} "
              f"(allocated {max_execs:.0f} vs needed {needed:.0f} at peak)"),
        # --- observability (Glue 4.0+, --enable-observability-metrics) -------
        ("Worker utilization (avg)",
         None if (v := p("workerUtilization", "Average", session_name, "mean")) is None else f"{v * 100:.1f}%"),
        ("Worker utilization (max)",
         None if (v := p("workerUtilization", "Maximum", session_name, "max")) is None else f"{v * 100:.1f}%"),
        ("Stage skewness (max, >1 = skewed)",
         None if (v := p("skewness.stage", "Maximum", session_name, "max")) is None else f"{v:,.2f}"),
        ("Job skewness (max, >1 = skewed)",
         None if (v := p("skewness.job", "Maximum", session_name, "max")) is None else f"{v:,.2f}"),
        # --- memory / cpu -----------------------------------------------------
        ("Driver JVM heap used (peak)", fmt_bytes(p("driver.jvm.heap.used", "Maximum", session_name, "max"))),
        ("Driver JVM heap usage % (peak)",
         None if (v := p("driver.jvm.heap.usage", "Maximum", session_name, "max")) is None else f"{v * 100:.1f}%"),
        ("All-executor JVM heap used (peak)", fmt_bytes(p("ALL.jvm.heap.used", "Maximum", session_name, "max"))),
        ("Driver CPU load (avg)",
         None if (v := p("driver.system.cpuSystemLoad", "Average", session_name, "mean")) is None else f"{v * 100:.1f}%"),
        ("Driver CPU load (max)",
         None if (v := p("driver.system.cpuSystemLoad", "Maximum", session_name, "max")) is None else f"{v * 100:.1f}%"),
        # --- data movement ----------------------------------------------------
        ("Shuffle bytes written (total)", fmt_bytes(p("aggregate.shuffleBytesWritten", "Sum", session_name, "sum"))),
        ("Shuffle local bytes read (total)", fmt_bytes(p("aggregate.shuffleLocalBytesRead", "Sum", session_name, "sum"))),
        ("S3 bytes read (total)", fmt_bytes(p("s3.filesystem.read_bytes", "Sum", session_name, "sum"))),
        ("S3 bytes written (total)", fmt_bytes(p("s3.filesystem.write_bytes", "Sum", session_name, "sum"))),
        ("Records read (total)",
         None if (v := p("aggregate.recordsRead", "Sum", session_name, "sum")) is None else f"{v:,.0f}"),
        ("Bytes read by tasks (total)", fmt_bytes(p("aggregate.bytesRead", "Sum", session_name, "sum"))),
        # --- tasks ------------------------------------------------------------
        ("Tasks completed",
         None if (v := p("aggregate.numCompletedTasks", "Sum", session_name, "sum")) is None else f"{v:,.0f}"),
        ("Tasks failed",
         None if (v := p("aggregate.numFailedTasks", "Sum", session_name, "sum")) is None else f"{v:,.0f}"),
        ("Tasks killed",
         None if (v := p("aggregate.numKilledTasks", "Sum", session_name, "sum")) is None else f"{v:,.0f}"),
        ("Stages completed",
         None if (v := p("aggregate.numCompletedStages", "Sum", session_name, "sum")) is None else f"{v:,.0f}"),
        ("Executor task time (elapsedTime total)",
         None if (v := p("aggregate.elapsedTime", "Sum", session_name, "sum")) is None
         else str(timedelta(milliseconds=int(v)))),
        ("Disk spill - BlockManager disk used (peak)",
         None if (v := p("BlockManager.disk.diskSpaceUsed", "Maximum", session_name, "max")) is None
         else f"{v:,.1f} MB"),
    ]

    # error categories from observability (glue.error.<category>)
    err = ts_df[ts_df["metric"].str.contains("glue.error.", regex=False) & (ts_df["stat"] == "Sum")]
    if not err.empty:
        totals = err.groupby("metric")["value"].sum()
        nonzero = totals[totals > 0]
        rows.append(
            ("Errors by category",
             "none" if nonzero.empty
             else ", ".join(f"{m.split('glue.error.')[-1]}={v:,.0f}" for m, v in nonzero.items()))
        )

    return pd.DataFrame(
        [(label, val) for label, val in rows if val is not None],
        columns=["metric", "value"],
    )


# --------------------------------------------------------------- charts -----
def plot_timeseries(ts_df, session_name):
    """Panels: executors / heap / CPU / data moved (+ worker utilization and
    skewness when observability metrics are present)."""
    if ts_df.empty:
        return None
    has_obs = (ts_df["namespace"] == "Glue Observability").any()
    n_rows = 3 if has_obs else 2

    titles = [
        "Executors (autoscaling view)", "JVM heap used (GB)",
        "Driver CPU load", "Data moved per interval (MB)",
    ]
    if has_obs:
        titles += ["Worker utilization (%)", "Skewness (>1 = skewed)"]

    fig = make_subplots(rows=n_rows, cols=2, subplot_titles=titles, vertical_spacing=0.10)

    def series(name_contains, stat):
        s = _series_vals(ts_df, name_contains, stat, session_name)
        return s.groupby("ts", as_index=False)["value"].mean() if not s.empty else s

    def add(row, col, name_contains, stat, label, color, scale=1.0, dash=None):
        s = series(name_contains, stat)
        if s.empty:
            return
        fig.add_trace(
            go.Scatter(
                x=s["ts"], y=s["value"] * scale, name=label, legendgroup=label,
                mode="lines", line=dict(color=color, width=2, dash=dash),
            ),
            row=row, col=col,
        )

    GB, MB = 1 / 1024**3, 1 / 1024**2
    add(1, 1, "numberAllExecutors", "Average", "executors allocated", C_BLUE)
    add(1, 1, "numberMaxNeededExecutors", "Average", "executors needed", C_YELLOW, dash="dot")
    add(1, 2, "driver.jvm.heap.used", "Average", "driver heap", C_BLUE, scale=GB)
    add(1, 2, "ALL.jvm.heap.used", "Average", "all executors heap", C_AQUA, scale=GB)
    add(2, 1, "driver.system.cpuSystemLoad", "Average", "driver CPU", C_VIOLET)
    add(2, 2, "s3.filesystem.read_bytes", "Sum", "S3 read", C_BLUE, scale=MB)
    add(2, 2, "s3.filesystem.write_bytes", "Sum", "S3 write", C_AQUA, scale=MB)
    add(2, 2, "aggregate.shuffleBytesWritten", "Sum", "shuffle written", C_RED, scale=MB)
    if has_obs:
        add(3, 1, "workerUtilization", "Average", "worker utilization", C_ORANGE, scale=100)
        add(3, 2, "skewness.stage", "Maximum", "stage skewness", C_RED)
        add(3, 2, "skewness.job", "Maximum", "job skewness", C_YELLOW, dash="dot")

    fig.update_xaxes(gridcolor="#e1e0d9")
    fig.update_yaxes(gridcolor="#e1e0d9", rangemode="tozero")
    fig.update_layout(
        title=f"Glue session metrics - {session_name}",
        height=350 * n_rows,
        legend=dict(orientation="h", yanchor="bottom", y=-0.10),
        **_CHART_LAYOUT,
    )
    return fig


# ---------------------------------------------------------------- main ------
def analyze_session(session_name, start=None, end=None, period=60, show_plots=True):
    """One-call entry point: discover, fetch, summarise, chart.

    session_name : the Glue interactive session name from the dbt profile
                   (= the JobRunId dimension in CloudWatch).
    start / end  : datetime window; defaults to the last 24 hours (UTC).
    period       : datapoint resolution in seconds (60 default; use 300 for
                   runs older than 15 days - CloudWatch drops 60s data).
    """
    end = end or datetime.now(timezone.utc)
    start = start or end - timedelta(hours=24)

    metrics = discover_metrics(session_name)
    ts_df = fetch_metric_data(metrics, start, end, period)
    if ts_df.empty:
        print(
            "Metrics exist but no datapoints in this window "
            f"({start:%Y-%m-%d %H:%M} - {end:%Y-%m-%d %H:%M} UTC). "
            "Widen it with start=/end=, or raise period= for old runs."
        )
        return {"summary": pd.DataFrame(), "metrics": pd.DataFrame(), "timeseries": ts_df}

    summary = build_session_summary(ts_df, session_name, start, end, period)
    metrics_table = build_metrics_table(ts_df)

    try:
        from IPython.display import display
    except ImportError:
        display = print

    print(f"\n=== Session summary: {session_name} ===")
    display(summary)
    print("\n=== All discovered metrics ===")
    display(metrics_table)

    if show_plots:
        fig = plot_timeseries(ts_df, session_name)
        if fig:
            fig.show()

    return {"summary": summary, "metrics": metrics_table, "timeseries": ts_df}


# ------------------------------------------------- offline logic check ------
if __name__ == "__main__":
    _t0 = datetime(2026, 7, 15, 21, 0, tzinfo=timezone.utc)
    _t1 = _t0 + timedelta(minutes=1)
    G = "Glue"
    O = "Glue Observability"
    _ts = pd.DataFrame(
        [
            {"ts": _t0, "namespace": G, "metric": "glue.driver.ExecutorAllocationManager.executors.numberAllExecutors",
             "job_run_id": "mysession", "stat": "Maximum", "value": 9},
            {"ts": _t0, "namespace": G, "metric": "glue.driver.ExecutorAllocationManager.executors.numberAllExecutors",
             "job_run_id": "mysession", "stat": "Average", "value": 6},
            {"ts": _t1, "namespace": G, "metric": "glue.driver.ExecutorAllocationManager.executors.numberAllExecutors",
             "job_run_id": "mysession", "stat": "Average", "value": 3},
            {"ts": _t0, "namespace": G, "metric": "glue.driver.ExecutorAllocationManager.executors.numberMaxNeededExecutors",
             "job_run_id": "mysession", "stat": "Maximum", "value": 4},
            # named series all zeros, ALL roll-up carries the real total -> fallback
            {"ts": _t0, "namespace": G, "metric": "glue.driver.aggregate.shuffleBytesWritten",
             "job_run_id": "mysession", "stat": "Sum", "value": 0},
            {"ts": _t0, "namespace": G, "metric": "glue.driver.aggregate.shuffleBytesWritten",
             "job_run_id": "ALL", "stat": "Sum", "value": 2.5 * 1024**3},
            {"ts": _t0, "namespace": G, "metric": "glue.driver.jvm.heap.used",
             "job_run_id": "ALL", "stat": "Maximum", "value": 3 * 1024**3},
            {"ts": _t0, "namespace": O, "metric": "glue.driver.workerUtilization",
             "job_run_id": "mysession", "stat": "Average", "value": 0.35},
            {"ts": _t0, "namespace": O, "metric": "glue.driver.skewness.stage",
             "job_run_id": "mysession", "stat": "Maximum", "value": 8.2},
            {"ts": _t0, "namespace": O, "metric": "glue.error.OUT_OF_MEMORY",
             "job_run_id": "mysession", "stat": "Sum", "value": 2},
        ]
    )
    df = build_session_summary(_ts, "mysession", _t0 - timedelta(hours=1), _t1, period=60)
    s = df.set_index("metric")["value"]
    assert s["Shuffle bytes written (total)"] == "2.50 GB", "ALL-series fallback failed"
    assert s["Worker utilization (avg)"] == "35.0%"
    assert s["Stage skewness (max, >1 = skewed)"] == "8.20"
    assert s["Avg executors allocated (time-weighted)"] == "4.5"
    assert "OUT_OF_MEMORY=2" in s["Errors by category"]
    assert "over" in s["Over/under-provisioned"]
    assert not build_metrics_table(_ts).empty
    assert plot_timeseries(_ts, "mysession") is not None
    print("Offline logic self-test passed.")
    print(df.to_string(index=False))
