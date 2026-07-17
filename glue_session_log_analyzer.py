"""
Glue session CloudWatch log analyzer (Jupyter-friendly).

Reads the Spark driver/executor logs a Glue interactive session streams to
CloudWatch when continuous logging is enabled on the session
(--enable-continuous-cloudwatch-log=true; add
--enable-continuous-log-filter=false to keep every Spark message) and digs
out what Spark was doing internally:

  1. errors & warnings    - every WARN/ERROR line, deduplicated with counts
  2. spark jobs           - DAGScheduler "Job N finished ... took Xs" lines
  3. spark stages         - every stage completion with its duration
  4. trouble signals      - OOM, spill, GC overhead, lost executors, retries
  5. slowest-stages chart - plotly bar of stage durations

Glue writes session logs to log groups under /aws-glue/ (exact names vary by
setup: /aws-glue/sessions/..., /aws-glue/interactive-sessions/..., logs-v2
groups for continuous logging). find_streams() scans every /aws-glue* group
for stream names containing your session UUID, so you don't need to know
which one.

The session UUID is the JobName that glue_metrics_analyzer.discover_metrics()
prints ("Discovered JobName(s): [...]"), or the session id shown in the Glue
console.

Usage in a Jupyter cell:

    result = analyze_session_logs("<session-uuid>")

    # or with an explicit window (e.g. the run window from dbt_log_analyzer):
    from datetime import datetime, timezone
    result = analyze_session_logs(
        "<session-uuid>",
        start=datetime(2026, 7, 15, 18, 0, tzinfo=timezone.utc),
        end=datetime(2026, 7, 15, 22, 0, tzinfo=timezone.utc),
    )

    result["errors"]    # deduplicated WARN/ERROR table with counts
    result["jobs"]      # spark jobs with durations
    result["stages"]    # stage completions with durations
    result["signals"]   # OOM / spill / GC / lost-executor indicator lines
    result["events"]    # every fetched raw line

Requires: boto3, pandas, plotly
"""

import re
from datetime import datetime, timedelta, timezone

import boto3
import pandas as pd
import plotly.graph_objects as go

# ---------------------------------------------------------------- config ----
AWS_PROFILE = "ro-prd1"
AWS_REGION = "eu-west-1"
LOG_GROUP_PREFIX = "/aws-glue"  # scanned for streams matching the session UUID

_CHART_LAYOUT = dict(
    template="plotly_white",
    paper_bgcolor="#fcfcfb",
    plot_bgcolor="#fcfcfb",
    font=dict(family='system-ui, -apple-system, "Segoe UI", sans-serif', color="#0b0b0b"),
)


def _session():
    return boto3.Session(profile_name=AWS_PROFILE, region_name=AWS_REGION)


# ------------------------------------------------------------- discovery ----
def list_glue_log_groups():
    """Every log group under /aws-glue (continuous + legacy output/error)."""
    client = _session().client("logs")
    groups, token = [], None
    while True:
        kwargs = dict(logGroupNamePrefix=LOG_GROUP_PREFIX)
        if token:
            kwargs["nextToken"] = token
        resp = client.describe_log_groups(**kwargs)
        groups += [g["logGroupName"] for g in resp["logGroups"]]
        token = resp.get("nextToken")
        if not token:
            break
    return groups


def find_streams(session_uuid, groups=None):
    """Scan /aws-glue* groups for streams whose name contains the session UUID.
    Returns [(group, stream_name), ...] - typically one driver stream plus one
    per executor when continuous logging is on."""
    client = _session().client("logs")
    found = []
    for group in groups or list_glue_log_groups():
        token = None
        while True:
            kwargs = dict(logGroupName=group)
            if token:
                kwargs["nextToken"] = token
            try:
                resp = client.describe_log_streams(**kwargs)
            except client.exceptions.ResourceNotFoundException:
                break
            for s in resp["logStreams"]:
                if session_uuid in s["logStreamName"]:
                    found.append((group, s["logStreamName"]))
            token = resp.get("nextToken")
            if not token:
                break
    for group, stream in found:
        print(f"Found stream: {group} / {stream}")
    if not found:
        print(
            f"No streams containing '{session_uuid}' under {LOG_GROUP_PREFIX}*. "
            "Is continuous logging enabled on the session "
            '("--enable-continuous-cloudwatch-log": "true")?'
        )
    return found


# ----------------------------------------------------------------- fetch ----
def fetch_stream_events(group, stream, start=None, end=None):
    """All events of one stream (filter_log_events pagination), optional window."""
    client = _session().client("logs")
    kwargs = dict(logGroupName=group, logStreamNames=[stream])
    if start:
        kwargs["startTime"] = int(start.timestamp() * 1000)
    if end:
        kwargs["endTime"] = int(end.timestamp() * 1000)
    events, token = [], None
    while True:
        if token:
            kwargs["nextToken"] = token
        resp = client.filter_log_events(**kwargs)
        for ev in resp["events"]:
            events.append(
                {
                    "ts": pd.to_datetime(ev["timestamp"], unit="ms", utc=True),
                    "group": group,
                    "stream": stream,
                    "message": (ev["message"] or "").rstrip("\n"),
                }
            )
        token = resp.get("nextToken")
        if not token:
            break
    print(f"Fetched {len(events)} events from {group} / {stream}")
    return events


# --------------------------------------------------------------- parse ------
_LEVEL_RE = re.compile(r"\b(ERROR|WARN|INFO)\b")
# 26/07/15 18:27:43 INFO DAGScheduler: Job 12 finished: save at ..., took 812.345678 s
_JOB_RE = re.compile(r"Job (\d+) (finished|failed): (.*?),? took ([\d.]+) s")
# ... DAGScheduler: ShuffleMapStage 34 (sql at ...) finished in 456.789 s
_STAGE_RE = re.compile(r"(ShuffleMapStage|ResultStage|Stage) (\d+) \((.*?)\) (finished|failed) in ([\d.]+) s")

_SIGNAL_PATTERNS = [
    ("out_of_memory", re.compile(r"OutOfMemoryError|OOM|Container killed .* memory", re.I)),
    ("disk_spill", re.compile(r"[Ss]pilling|spill(ed)? .* to disk", re.I)),
    ("gc_pressure", re.compile(r"GC overhead|Full GC|heartbeat(er)? .* timed? ?out", re.I)),
    ("lost_executor", re.compile(r"Lost executor|ExecutorLostFailure|Removing executor", re.I)),
    ("task_retry", re.compile(r"Lost task|Resubmitt(ed|ing)|FetchFailed", re.I)),
    ("throttling", re.compile(r"SlowDown|Throttl|503 Slow Down|Rate exceeded", re.I)),
]

_NUM_RE = re.compile(r"\d+")


def _level(msg):
    m = _LEVEL_RE.search(msg)
    return m.group(1) if m else None


def build_error_table(events):
    """WARN/ERROR lines deduplicated by their number-stripped signature."""
    buckets = {}
    for ev in events:
        lvl = _level(ev["message"])
        if lvl not in ("WARN", "ERROR"):
            continue
        sig = _NUM_RE.sub("#", ev["message"])[:300]
        b = buckets.setdefault(sig, {"level": lvl, "count": 0, "first_ts": ev["ts"],
                                     "last_ts": ev["ts"], "example": ev["message"][:500]})
        b["count"] += 1
        b["first_ts"] = min(b["first_ts"], ev["ts"])
        b["last_ts"] = max(b["last_ts"], ev["ts"])
    df = pd.DataFrame(buckets.values())
    if not df.empty:
        df = df.sort_values(["level", "count"], ascending=[True, False]).reset_index(drop=True)
    return df


def build_job_table(events):
    rows = []
    for ev in events:
        m = _JOB_RE.search(ev["message"])
        if m:
            rows.append(
                {"ts": ev["ts"], "job": int(m.group(1)), "outcome": m.group(2),
                 "action": m.group(3)[:120], "duration_s": float(m.group(4))}
            )
    df = pd.DataFrame(rows)
    if not df.empty:
        df = df.sort_values("duration_s", ascending=False).reset_index(drop=True)
    return df


def build_stage_table(events):
    rows = []
    for ev in events:
        m = _STAGE_RE.search(ev["message"])
        if m:
            rows.append(
                {"ts": ev["ts"], "stage": int(m.group(2)), "kind": m.group(1),
                 "callsite": m.group(3)[:120], "outcome": m.group(4),
                 "duration_s": float(m.group(5))}
            )
    df = pd.DataFrame(rows)
    if not df.empty:
        df = df.sort_values("duration_s", ascending=False).reset_index(drop=True)
    return df


def build_signal_table(events):
    """Known trouble indicators (memory, spill, GC, lost executors, retries)."""
    rows = []
    for ev in events:
        for signal, pattern in _SIGNAL_PATTERNS:
            if pattern.search(ev["message"]):
                rows.append({"ts": ev["ts"], "signal": signal, "message": ev["message"][:500]})
                break
    df = pd.DataFrame(rows)
    if not df.empty:
        df = df.sort_values("ts").reset_index(drop=True)
    return df


# --------------------------------------------------------------- charts -----
def plot_slowest_stages(stages_df, top_n=15):
    df = stages_df.nlargest(top_n, "duration_s").sort_values("duration_s")
    if df.empty:
        return None
    df["label"] = df["kind"] + " " + df["stage"].astype(str) + " (" + df["callsite"] + ")"
    fig = go.Figure(
        go.Bar(
            x=df["duration_s"],
            y=df["label"],
            orientation="h",
            marker=dict(color="#2a78d6", cornerradius=4),
            text=[f"{v:,.0f}s" for v in df["duration_s"]],
            textposition="outside",
            textfont=dict(color="#52514e"),
        )
    )
    fig.update_xaxes(title="stage duration (s)", gridcolor="#e1e0d9")
    fig.update_layout(
        title=f"Top {len(df)} slowest Spark stages",
        height=max(350, 30 * len(df) + 120),
        bargap=0.45,
        **_CHART_LAYOUT,
    )
    return fig


# ---------------------------------------------------------------- main ------
def analyze_session_logs(session_uuid, start=None, end=None, top_n=15, show_plots=True):
    """One-call entry point: find streams, fetch, extract Spark internals.

    session_uuid : the auto-generated session UUID - the JobName printed by
                   glue_metrics_analyzer.discover_metrics(), or the session id
                   in the Glue console.
    start / end  : datetime window; defaults to the last 24 hours (UTC).
    """
    end = end or datetime.now(timezone.utc)
    start = start or end - timedelta(hours=24)

    events = []
    for group, stream in find_streams(session_uuid):
        events += fetch_stream_events(group, stream, start, end)
    if not events:
        print("No log events found - check the UUID, window, and continuous logging.")
        return {}
    events.sort(key=lambda e: e["ts"])

    errors = build_error_table(events)
    jobs = build_job_table(events)
    stages = build_stage_table(events)
    signals = build_signal_table(events)
    events_df = pd.DataFrame(events)

    try:
        from IPython.display import display
    except ImportError:
        display = print

    if errors.empty:
        print("\nNo WARN/ERROR lines in this window.")
    else:
        print(f"\n=== Errors & warnings (deduplicated, {len(errors)} kinds) ===")
        with pd.option_context("display.max_colwidth", 300):
            display(errors)
    if not jobs.empty:
        print(f"\n=== Spark jobs by duration ({len(jobs)}) ===")
        display(jobs.head(top_n))
    if not stages.empty:
        print(f"\n=== Spark stages by duration ({len(stages)}) ===")
        display(stages.head(top_n))
    if signals.empty:
        print("\nNo memory/spill/GC/lost-executor signals found.")
    else:
        print(f"\n=== Trouble signals ({len(signals)}) ===")
        print(signals["signal"].value_counts().to_string())
        with pd.option_context("display.max_colwidth", 300):
            display(signals.head(50))

    if show_plots and not stages.empty:
        fig = plot_slowest_stages(stages, top_n)
        if fig:
            fig.show()

    return {"errors": errors, "jobs": jobs, "stages": stages,
            "signals": signals, "events": events_df}


# ------------------------------------------------- offline parsing check ----
if __name__ == "__main__":
    _t = pd.Timestamp("2026-07-15T20:00:00Z")
    _lines = [
        "26/07/15 20:00:01 INFO DAGScheduler: Job 12 finished: save at NativeMethodAccessorImpl.java:0, took 812.345678 s",
        "26/07/15 20:00:02 INFO DAGScheduler: ShuffleMapStage 34 (sql at model.avqdf_valuation) finished in 456.789 s",
        "26/07/15 20:00:03 INFO DAGScheduler: ResultStage 35 (sql at model.avqdf_valuation) finished in 12.5 s",
        "26/07/15 20:00:04 WARN TaskSetManager: Lost task 3.0 in stage 34.0 (TID 812): FetchFailed",
        "26/07/15 20:00:04 WARN TaskSetManager: Lost task 4.0 in stage 34.0 (TID 813): FetchFailed",
        "26/07/15 20:00:05 ERROR YarnScheduler: Lost executor 7: Container killed by YARN for exceeding memory limits",
        "26/07/15 20:00:06 INFO UnsafeExternalSorter: Thread 42 spilling sort data of 512.0 MB to disk",
        "just a plain info line with no level",
    ]
    _events = [{"ts": _t + pd.Timedelta(seconds=i), "group": "/aws-glue/sessions",
                "stream": "abc123_driver", "message": m} for i, m in enumerate(_lines)]

    jobs = build_job_table(_events)
    assert len(jobs) == 1 and abs(jobs.loc[0, "duration_s"] - 812.345678) < 1e-6
    stages = build_stage_table(_events)
    assert len(stages) == 2 and stages.loc[0, "stage"] == 34, "slowest stage should sort first"
    errors = build_error_table(_events)
    assert errors["count"].max() == 2, "duplicate WARN lines should collapse into one bucket"
    assert set(errors["level"]) == {"WARN", "ERROR"}
    signals = build_signal_table(_events)
    # the lost-executor line is memory-caused, so it classifies as out_of_memory
    assert {"task_retry", "out_of_memory", "disk_spill"} <= set(signals["signal"])
    assert plot_slowest_stages(stages) is not None
    print("Offline parsing self-test passed.")
    print(stages.to_string(index=False))
