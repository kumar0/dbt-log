"""
Demo of what --enable-observability-metrics adds, using SYNTHETIC data.

No AWS access needed. Generates a realistic day of metrics shaped like a real
dbt-on-Glue run (busy parallel work, then a ~3h skewed single-task stage, then
a short task burst at the end) and feeds it through glue_metrics_analyzer's
own build_session_summary() / plot_timeseries(), so the output is exactly
what you'll see once observability metrics are enabled on a session:

  - Worker utilization (%)  - collapses during the skewed stage
  - Stage/job skewness      - spikes >1 exactly when one task hogs the run
  - Errors by category      - e.g. OUT_OF_MEMORY counts
  - plus the usual executors / heap / CPU / data-moved panels

Outputs (written to sample/):
  sample_observability_metrics.html   interactive plotly chart
  sample_observability_metrics.png    static image (needs kaleido)
  sample_session_summary.csv          the summary table
  sample_timeseries.csv               the raw synthetic datapoints

Run:  python sample_observability_demo.py
"""

from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd

from glue_metrics_analyzer import build_metrics_table, build_session_summary, plot_timeseries

SESSION = "sample-session (SYNTHETIC DATA)"
G, O = "Glue", "Glue Observability"
OUT_DIR = Path(__file__).parent / "sample"


def generate_timeseries():
    rng = np.random.default_rng(42)
    start = datetime(2026, 7, 15, 0, 0, tzinfo=timezone.utc)
    n = 22 * 60  # one datapoint per minute, 00:00-22:00
    ts = [start + timedelta(minutes=i) for i in range(n)]
    h = np.arange(n) / 60.0  # hours since start

    busy = h < 18.5                      # parallel dbt models running
    skewed = (h >= 18.5) & (h < 21.4)    # one long skewed task, cluster idle
    burst = h >= 21.4                    # final wide stage + write

    def phase(b, s, u, noise=0.0):
        v = np.where(busy, b, np.where(skewed, s, u)).astype(float)
        return v + rng.normal(0, noise, n) if noise else v

    # --- observability metrics (the new ones) -------------------------------
    utilization = np.clip(
        phase(0.78, 0.12, 0.92, 0.08) + np.where(busy, 0.12 * np.sin(h * 3), 0), 0.02, 1.0
    )
    skew_stage = np.clip(
        phase(1.2, 0.0, 1.5, 0.3) + np.where(skewed, np.minimum((h - 18.5) * 4, 8.0), 0), 0.2, None
    )
    skew_job = np.clip(phase(1.0, 0.0, 1.2, 0.1) + np.where(skewed, 3.0, 0), 0.5, None)
    oom = np.zeros(n)
    oom[int(17.75 * 60)] = 2  # two executor OOM-kills mid-afternoon

    # --- core Glue namespace metrics ----------------------------------------
    allocated = np.minimum(np.arange(n) / 8, 7.0)  # ramp to 7 executors, then flat
    needed = np.clip(phase(5, 0.5, 40, 3), 0, None)
    needed[int(21.4 * 60):int(21.45 * 60)] = 223  # the end-of-run pending-task spike

    driver_heap = np.clip(3 + np.cumsum(rng.normal(0, 0.08, n)), 1.5, 8.5) * 1024**3
    exec_heap = np.clip(
        phase(65, 40, 75) + np.where(busy | burst, 25 * np.sin(h * 20) + rng.normal(0, 8, n), 0),
        10, 105,
    ) * 1024**3
    driver_cpu = np.clip(phase(0.03, 0.004, 0.05) * rng.exponential(1, n), 0, 0.09)

    s3_read = np.where(busy, rng.lognormal(4.5, 1.0, n), np.where(burst, 30, 0.5)) * 1024**2
    s3_write = np.where(busy, rng.lognormal(3.5, 1.2, n), np.where(burst, 400, 0.2)) * 1024**2
    shuffle = np.where(busy, (rng.random(n) < 0.15) * rng.lognormal(5, 1, n),
                       np.where(burst, 250, 0)) * 1024**2

    rows = []

    def emit(namespace, metric, stat, values, run_id=SESSION):
        rows.extend(
            {"ts": t, "namespace": namespace, "metric": metric,
             "job_run_id": run_id, "stat": stat, "value": float(v)}
            for t, v in zip(ts, values)
        )

    emit(O, "glue.driver.workerUtilization", "Average", utilization)
    emit(O, "glue.driver.workerUtilization", "Maximum", np.clip(utilization * 1.05, 0, 1))
    emit(O, "glue.driver.skewness.stage", "Maximum", skew_stage)
    emit(O, "glue.driver.skewness.job", "Maximum", skew_job)
    emit(O, "glue.error.OUT_OF_MEMORY", "Sum", oom)

    emit(G, "glue.driver.ExecutorAllocationManager.executors.numberAllExecutors", "Average", allocated)
    emit(G, "glue.driver.ExecutorAllocationManager.executors.numberAllExecutors", "Maximum", allocated)
    emit(G, "glue.driver.ExecutorAllocationManager.executors.numberMaxNeededExecutors", "Average", needed)
    emit(G, "glue.driver.ExecutorAllocationManager.executors.numberMaxNeededExecutors", "Maximum", needed)
    emit(G, "glue.driver.jvm.heap.used", "Average", driver_heap)
    emit(G, "glue.driver.jvm.heap.used", "Maximum", driver_heap * 1.05)
    emit(G, "glue.driver.jvm.heap.usage", "Maximum", driver_heap / (12 * 1024**3))
    emit(G, "glue.ALL.jvm.heap.used", "Average", exec_heap)
    emit(G, "glue.ALL.jvm.heap.used", "Maximum", exec_heap * 1.05)
    emit(G, "glue.driver.system.cpuSystemLoad", "Average", driver_cpu)
    emit(G, "glue.driver.system.cpuSystemLoad", "Maximum", driver_cpu * 1.5)
    # counters carried on the ALL roll-up (as on real sessions)
    emit(G, "glue.driver.s3.filesystem.read_bytes", "Sum", s3_read, run_id="ALL")
    emit(G, "glue.driver.s3.filesystem.write_bytes", "Sum", s3_write, run_id="ALL")
    emit(G, "glue.driver.aggregate.shuffleBytesWritten", "Sum", shuffle, run_id="ALL")
    emit(G, "glue.driver.aggregate.numCompletedTasks", "Sum",
         np.where(busy, rng.poisson(120, n), np.where(skewed, 0, 800)), run_id="ALL")
    emit(G, "glue.driver.aggregate.recordsRead", "Sum",
         np.where(busy, rng.poisson(2_000_000, n), np.where(burst, 5_000_000, 0)), run_id="ALL")

    df = pd.DataFrame(rows)
    df["ts"] = pd.to_datetime(df["ts"], utc=True)
    return df.sort_values("ts").reset_index(drop=True), start, start + timedelta(hours=22)


def main():
    OUT_DIR.mkdir(exist_ok=True)
    ts_df, start, end = generate_timeseries()

    summary = build_session_summary(ts_df, SESSION, start, end, period=60)
    metrics_table = build_metrics_table(ts_df)
    fig = plot_timeseries(ts_df, SESSION)

    summary.to_csv(OUT_DIR / "sample_session_summary.csv", index=False)
    ts_df.to_csv(OUT_DIR / "sample_timeseries.csv", index=False)
    fig.write_html(OUT_DIR / "sample_observability_metrics.html", include_plotlyjs="cdn")
    try:
        fig.write_image(OUT_DIR / "sample_observability_metrics.png", width=1400, height=1050, scale=2)
    except Exception as e:  # kaleido not installed
        print(f"PNG export skipped ({e}); HTML written.")

    print(f"\nSaved outputs to {OUT_DIR}/")
    print("\n=== Sample session summary (synthetic) ===")
    print(summary.to_string(index=False))
    print(f"\n{len(metrics_table)} metric series in the raw table")
    return {"summary": summary, "metrics": metrics_table, "timeseries": ts_df, "fig": fig}


if __name__ == "__main__":
    main()
