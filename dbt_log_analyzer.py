"""
dbt CloudWatch log analyzer (Jupyter-friendly).

Reads a CloudWatch log stream produced by a containerised dbt run (JSON log
format, e.g. dbt on Fargate with the Glue adapter), parses the structured dbt
events and produces:

  1. run summary          - invocation id, wall time, node counts by status
  2. node table           - per node: start, end, duration, status, message
  3. errors & warnings    - every error/warning event with full message
  4. plotly timeline      - Gantt view of node execution (spot slow models)
  5. slowest-nodes chart  - top-N nodes by execution time

Usage in a Jupyter cell:

    result = analyze("ecs/spinv/0b2bb10d-8220-429c-ba9b-bb79bcd95dae")

    result["nodes"]      # pandas DataFrame - one row per node
    result["errors"]     # pandas DataFrame - errors/warnings
    result["summary"]    # pandas DataFrame - run summary
    result["events"]     # pandas DataFrame - every parsed raw event

Requires: boto3, pandas, plotly
"""

import json
import re

import boto3
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

# ---------------------------------------------------------------- config ----
AWS_PROFILE = "ro-prd1"
AWS_REGION = "eu-west-1"
LOG_GROUP = "CHANGE_ME"  # <-- EDIT: CloudWatch log group containing ecs/spinv/* streams

# Validated status palette (light surface)
STATUS_COLORS = {
    "success": "#0ca30c",
    "pass": "#0ca30c",
    "ok": "#0ca30c",
    "warn": "#fab219",
    "warning": "#fab219",
    "error": "#d03b3b",
    "fail": "#d03b3b",
    "skipped": "#898781",
    "started": "#2a78d6",
    "running": "#2a78d6",
    "unknown": "#898781",
}

_ANSI_RE = re.compile(r"\x1b\[[0-9;]*m|\[\d+m")

_CHART_LAYOUT = dict(
    template="plotly_white",
    paper_bgcolor="#fcfcfb",
    plot_bgcolor="#fcfcfb",
    font=dict(family='system-ui, -apple-system, "Segoe UI", sans-serif', color="#0b0b0b"),
)


def _session():
    return boto3.Session(profile_name=AWS_PROFILE, region_name=AWS_REGION)


def _clean(msg):
    """Strip ANSI colour codes dbt embeds in messages."""
    return _ANSI_RE.sub("", msg or "").strip()


# --------------------------------------------------------------- fetch ------
def fetch_log_events(log_stream, log_group=None):
    """Fetch every event of a log stream (paginated with nextForwardToken)."""
    client = _session().client("logs")
    group = log_group or LOG_GROUP
    if group == "CHANGE_ME":
        raise ValueError("Set LOG_GROUP at the top of this script (or pass log_group=...)")

    events, token = [], None
    while True:
        kwargs = dict(logGroupName=group, logStreamName=log_stream, startFromHead=True)
        if token:
            kwargs["nextToken"] = token
        resp = client.get_log_events(**kwargs)
        events.extend(resp["events"])
        # CloudWatch signals the end by returning the same token again
        if resp["nextForwardToken"] == token:
            break
        token = resp["nextForwardToken"]
    print(f"Fetched {len(events)} log events from {group} / {log_stream}")
    return events


# --------------------------------------------------------------- parse ------
def parse_dbt_events(events):
    """Parse raw CloudWatch events into a flat list of dbt structured records."""
    records, skipped = [], 0
    for ev in events:
        try:
            rec = json.loads(ev["message"])
        except (json.JSONDecodeError, TypeError):
            skipped += 1
            continue
        if not isinstance(rec, dict) or "info" not in rec:
            skipped += 1
            continue
        info = rec.get("info", {}) or {}
        data = rec.get("data", {}) or {}
        records.append(
            {
                "ts": info.get("ts"),
                "level": info.get("level"),
                "code": info.get("code"),
                "event_name": info.get("name"),
                "invocation_id": info.get("invocation_id"),
                "thread": info.get("thread"),
                "msg": _clean(info.get("msg")),
                "data": data,
            }
        )
    if skipped:
        print(f"Skipped {skipped} non-JSON / non-dbt log lines")
    return records


_TERMINAL = {"success", "error", "fail", "skipped", "pass", "warn"}


def build_node_table(records):
    """One row per dbt node (model/test/seed/snapshot/hook) with timings."""
    nodes = {}
    for rec in records:
        ni = rec["data"].get("node_info")
        if not ni or not isinstance(ni, dict):
            continue
        uid = ni.get("unique_id") or ni.get("node_name")
        if not uid:
            continue
        node = nodes.setdefault(
            uid,
            {
                "node_name": None, "resource_type": None, "materialized": None,
                "started_at": None, "finished_at": None, "status": None,
                "execution_time_s": None, "index": None, "total": None,
                "message": None, "node_path": None, "relation": None,
            },
        )
        node["node_name"] = ni.get("node_name") or node["node_name"]
        node["resource_type"] = ni.get("resource_type") or node["resource_type"]
        node["materialized"] = ni.get("materialized") or node["materialized"]
        node["node_path"] = ni.get("node_path") or node["node_path"]
        rel = ni.get("node_relation") or {}
        node["relation"] = rel.get("relation_name") or node["relation"]

        started = ni.get("node_started_at")
        if started:
            node["started_at"] = min(filter(None, [node["started_at"], started]))
        finished = ni.get("node_finished_at")
        if finished:
            node["finished_at"] = max(filter(None, [node["finished_at"], finished]))

        status = (ni.get("node_status") or "").lower()
        if status and status != "none":
            # a terminal status always wins over "started"/"running"
            if status in _TERMINAL or (node["status"] or "") not in _TERMINAL:
                node["status"] = status

        # result events carry execution_time + the human-readable outcome line
        if "execution_time" in rec["data"]:
            node["execution_time_s"] = rec["data"]["execution_time"]
            node["message"] = rec["msg"]
            node["index"] = rec["data"].get("index")
            node["total"] = rec["data"].get("total")

    if not nodes:
        return pd.DataFrame()

    df = pd.DataFrame(nodes.values(), index=nodes.keys()).rename_axis("unique_id").reset_index()
    df["started_at"] = pd.to_datetime(df["started_at"], errors="coerce")
    df["finished_at"] = pd.to_datetime(df["finished_at"], errors="coerce")
    df["duration_s"] = df["execution_time_s"].fillna(
        (df["finished_at"] - df["started_at"]).dt.total_seconds()
    )
    df["status"] = df["status"].fillna("unknown")
    df = df.sort_values("started_at").reset_index(drop=True)
    cols = [
        "node_name", "resource_type", "materialized", "status",
        "started_at", "finished_at", "duration_s", "index", "total",
        "message", "relation", "node_path", "unique_id",
    ]
    return df[cols]


def build_error_table(records):
    """Every error/warning-level event, plus node failures."""
    rows = []
    for rec in records:
        ni = rec["data"].get("node_info") or {}
        node_status = (ni.get("node_status") or "").lower()
        if rec["level"] in ("error", "warning", "warn") or node_status in ("error", "fail"):
            rows.append(
                {
                    "ts": rec["ts"],
                    "level": rec["level"],
                    "code": rec["code"],
                    "event": rec["event_name"],
                    "node": ni.get("node_name"),
                    "message": rec["msg"],
                }
            )
    df = pd.DataFrame(rows)
    if not df.empty:
        df["ts"] = pd.to_datetime(df["ts"], errors="coerce")
        df = df.sort_values("ts").reset_index(drop=True)
    return df


def build_run_summary(records, nodes_df):
    ts = pd.to_datetime(pd.Series([r["ts"] for r in records if r["ts"]]), errors="coerce")
    version = next(
        (r["data"].get("version") or r["data"].get("v")
         for r in records if r["event_name"] == "MainReportVersion"),
        None,
    )
    cmd = next((r["msg"] for r in records if r["event_name"] == "MainReportArgs"), None)
    info = {
        "invocation_id": next((r["invocation_id"] for r in records if r["invocation_id"]), None),
        "dbt_version": version,
        "command": cmd,
        "first_event": ts.min(),
        "last_event": ts.max(),
        "wall_time": ts.max() - ts.min(),
        "total_events": len(records),
        "total_nodes": len(nodes_df),
    }
    if not nodes_df.empty:
        for status, count in nodes_df["status"].value_counts().items():
            info[f"nodes_{status}"] = count
    return pd.DataFrame(info.items(), columns=["item", "value"])


# --------------------------------------------------------------- charts -----
def plot_timeline(nodes_df):
    """Gantt-style timeline of node execution, coloured by status."""
    df = nodes_df.dropna(subset=["started_at", "finished_at"]).copy()
    if df.empty:
        print("No nodes with both start and finish timestamps - skipping timeline")
        return None
    df["label"] = df["node_name"].fillna(df["unique_id"])
    fig = px.timeline(
        df,
        x_start="started_at",
        x_end="finished_at",
        y="label",
        color="status",
        color_discrete_map=STATUS_COLORS,
        hover_data={"duration_s": ":.1f", "materialized": True, "message": True},
    )
    fig.update_yaxes(autorange="reversed", title=None, tickfont=dict(color="#52514e"))
    fig.update_xaxes(title=None, gridcolor="#e1e0d9")
    fig.update_traces(marker_line_width=0)
    fig.update_layout(
        title="dbt node execution timeline",
        height=max(400, 28 * len(df) + 120),
        legend_title_text="status",
        **_CHART_LAYOUT,
    )
    return fig


def plot_slowest(nodes_df, top_n=15):
    df = nodes_df.dropna(subset=["duration_s"]).nlargest(top_n, "duration_s").copy()
    if df.empty:
        return None
    df["label"] = df["node_name"].fillna(df["unique_id"])
    df = df.sort_values("duration_s")
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
    fig.update_xaxes(title="execution time (s)", gridcolor="#e1e0d9")
    fig.update_layout(
        title=f"Top {len(df)} slowest nodes",
        height=max(350, 30 * len(df) + 120),
        bargap=0.45,
        **_CHART_LAYOUT,
    )
    return fig


# ---------------------------------------------------------------- main ------
def analyze(log_stream, log_group=None, top_n=15, show_plots=True):
    """One-call entry point: fetch, parse, display tables and charts."""
    events = fetch_log_events(log_stream, log_group)
    records = parse_dbt_events(events)
    if not records:
        print("No dbt JSON records found in this stream - is it the right stream?")
        return {}

    nodes = build_node_table(records)
    errors = build_error_table(records)
    summary = build_run_summary(records, nodes)
    events_df = pd.DataFrame(records).drop(columns=["data"])
    events_df["ts"] = pd.to_datetime(events_df["ts"], errors="coerce")

    try:
        from IPython.display import display
    except ImportError:
        display = print

    print("\n=== Run summary ===")
    display(summary)
    print("\n=== Nodes ===")
    display(nodes)
    if errors.empty:
        print("\nNo errors or warnings in this run.")
    else:
        print(f"\n=== Errors & warnings ({len(errors)}) ===")
        with pd.option_context("display.max_colwidth", 300):
            display(errors)

    if show_plots and not nodes.empty:
        fig = plot_timeline(nodes)
        if fig:
            fig.show()
        fig = plot_slowest(nodes, top_n)
        if fig:
            fig.show()

    return {"summary": summary, "nodes": nodes, "errors": errors, "events": events_df}


# ------------------------------------------------- offline parsing check ----
if __name__ == "__main__":
    _sample = {
        "data": {
            "description": "sql table model pbwm_x.avqdf_valuation",
            "execution_time": 10882.464, "index": 5, "total": 5, "status": "OK",
            "node_info": {
                "materialized": "table", "node_name": "avqdf_valuation",
                "node_path": "avqdf/valuation_transform/avqdf_valuation.sql",
                "node_relation": {"relation_name": "pbwm_x.avqdf_valuation"},
                "node_started_at": "2026-07-15T18:27:43.411023",
                "node_finished_at": "2026-07-15T21:29:05.878028",
                "node_status": "success", "resource_type": "model",
                "unique_id": "model.dpiibc_prepared.avqdf_valuation",
            },
        },
        "info": {
            "category": "", "code": "Q012", "invocation_id": "d40cd347",
            "level": "info",
            "msg": "5 of 5 OK created sql table model pbwm_x.avqdf_valuation "
                   "[32mOK[0m in 10882.46s",
            "name": "LogModelResult", "pid": 1, "thread": "Thread-8 (worker)",
            "ts": "2026-07-15T21:29:05.879095Z",
        },
    }
    recs = parse_dbt_events([{"message": json.dumps(_sample)}, {"message": "not json"}])
    assert len(recs) == 1, "should parse exactly one record"
    assert "" not in recs[0]["msg"], "ANSI codes should be stripped"
    df = build_node_table(recs)
    assert len(df) == 1 and df.loc[0, "status"] == "success"
    assert abs(df.loc[0, "duration_s"] - 10882.464) < 0.001
    print("Offline parsing self-test passed.")
    print(df.T)
