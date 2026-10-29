import json
import math
import os
import tempfile
from pathlib import Path

import pytest

from obkit.prof import (
    attribute,
    load_denet,
    load_events,
    load_snakemake_benchmark,
    summarise,
)

DATA = Path(__file__).parent / "data"
T0 = 1_700_000_000_000  # ms since epoch, 2023-11-14T22:13:20Z


def iso(ms):
    from datetime import datetime, timezone

    dt = datetime.fromtimestamp(ms / 1000, timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{int(ms) % 1000:03d}Z"


def write_lines(d, name, records, tail=""):
    p = os.path.join(d, name)
    with open(p, "w") as f:
        f.writelines(json.dumps(r) + "\n" for r in records)
        f.write(tail)
    return p


def denet_trace(d, cpu=200.0, seconds=10, gpu=None, layout="system_metrics", tail=""):
    """A tree trace from T0 to T0 + seconds, one sample per second at constant cpu (%)."""
    recs = [
        {"kind": "env", "ts_ms": T0, "host": "h"},
        {"kind": "metadata", "pid": 7, "cmd": ["python3", "x.py"], "t0_ms": T0},
    ]
    for i in range(1, seconds + 1):
        a = {
            "ts_ms": T0 + 1000 * i,
            "cpu_usage": cpu,
            "mem_rss_kb": 1024 * i,
            "thread_count": i,
        }
        rec = {
            "kind": "tree",
            "ts_ms": T0 + 1000 * i,
            "parent": {},
            "children": [],
            "aggregated": a,
        }
        if gpu is not None and layout == "system_metrics":
            rec["parent"]["gpu"] = {
                "system_metrics": [{"memory_used": gpu * 2**20, "utilization_gpu": 50}]
            }
        if gpu is not None and layout == "devices":
            a["gpu"] = {
                "devices": [{"process_memory_usage": gpu * 2**20}],
                "max_gpu_utilization": 80,
            }
        recs.append(rec)
    recs.append(
        {"kind": "exit", "ts_ms": T0 + 1000 * seconds, "last_sample_to_exit_ms": 0}
    )
    return write_lines(d, "denet.jsonl", recs, tail)


def events(d, spans, tail=""):
    recs = []
    for name, a, b in spans:
        recs.append(
            {
                "ts": iso(T0 + a * 1000),
                "event": name,
                "phase": "start",
                "pid": 7,
                "host": "h",
            }
        )
        recs.append(
            {
                "ts": iso(T0 + b * 1000),
                "event": name,
                "phase": "end",
                "pid": 7,
                "host": "h",
                "attrs": {"n": 1},
            }
        )
    return write_lines(d, "obkit-events.jsonl", recs, tail)


# ---- load_events --------------------------------------------------------------


def test_load_events_pairs_start_end_and_converts_to_ms():
    with tempfile.TemporaryDirectory() as d:
        ph = load_events(events(d, [("load", 0, 2), ("compute", 2, 9.5)]))
    assert [p["event"] for p in ph] == ["load", "compute"]
    assert ph[1]["start_ms"] == T0 + 2000 and ph[1]["end_ms"] == T0 + 9500
    assert ph[0]["pid"] == 7 and ph[0]["attrs"] == {"n": 1}


def test_load_events_tolerates_partial_last_line_and_warns_on_unmatched():
    with tempfile.TemporaryDirectory() as d:
        recs = [
            {"ts": iso(T0), "event": "a", "phase": "end", "pid": 1},
            {"ts": iso(T0), "event": "b", "phase": "start", "pid": 1},
        ]
        p = write_lines(d, "e.jsonl", recs, tail='{"ts": "2023-11-14T22:13')
        with pytest.warns(UserWarning) as rec:
            assert load_events(p) == []
    msgs = " ".join(str(w.message) for w in rec)
    assert "unmatched end" in msgs and "unmatched start" in msgs


def test_load_events_keeps_pids_apart():
    with tempfile.TemporaryDirectory() as d:
        recs = [
            {"ts": iso(T0), "event": "x", "phase": "start", "pid": 1},
            {"ts": iso(T0 + 100), "event": "x", "phase": "start", "pid": 2},
            {"ts": iso(T0 + 300), "event": "x", "phase": "end", "pid": 1},
            {"ts": iso(T0 + 500), "event": "x", "phase": "end", "pid": 2},
        ]
        ph = load_events(write_lines(d, "e.jsonl", recs))
    assert [(p["pid"], p["end_ms"] - p["start_ms"]) for p in ph] == [(1, 300), (2, 400)]


# ---- load_denet ---------------------------------------------------------------


def test_load_denet_reads_tree_samples_and_skips_env_and_partial_lines():
    with tempfile.TemporaryDirectory() as d:
        t = load_denet(denet_trace(d, seconds=3, tail='{"kind":"tree","ts_ms":'))
    assert t["t0_ms"] == T0 and t["end_ms"] == T0 + 3000 and t["pid"] == 7
    assert [s["ts_ms"] for s in t["samples"]] == [T0 + 1000, T0 + 2000, T0 + 3000]
    assert t["samples"][0]["cpu_usage"] == 200.0 and t["samples"][0]["gpu_util"] is None


@pytest.mark.parametrize(
    "layout,util,mem", [("system_metrics", 50, 512), ("devices", 80, 512)]
)
def test_load_denet_reads_both_gpu_layouts(layout, util, mem):
    with tempfile.TemporaryDirectory() as d:
        t = load_denet(denet_trace(d, seconds=2, gpu=mem, layout=layout))
    s = t["samples"][0]
    assert s["gpu_util"] == util and s["gpu_mem_bytes"] == mem * 2**20


def test_load_denet_single_process_samples_and_missing_header():
    with tempfile.TemporaryDirectory() as d:
        recs = [
            {
                "kind": "sample",
                "ts_ms": T0 + 1000,
                "cpu_usage": 50.0,
                "mem_rss_kb": 10,
                "thread_count": 1,
            },
            {
                "kind": "sample",
                "ts_ms": T0 + 2000,
                "cpu_usage": 150.0,
                "mem_rss_kb": 20,
                "thread_count": 2,
            },
        ]
        t = load_denet(write_lines(d, "s.jsonl", recs))
    assert (
        t["t0_ms"] == T0 + 1000 and t["end_ms"] == T0 + 2000 and len(t["samples"]) == 2
    )


# ---- attribute ----------------------------------------------------------------


def test_attribute_splits_cpu_time_by_phase_and_reports_unphased():
    with tempfile.TemporaryDirectory() as d:
        trace = load_denet(denet_trace(d))
        rows = attribute(
            load_events(events(d, [("load", 2, 5), ("compute", 5, 9)])), trace
        )
    by = {r["event"]: r for r in rows}
    load, compute, rest = by["load"], by["compute"], by["(unphased)"]
    assert load["duration_s"] == pytest.approx(3) and load["cpu_s"] == pytest.approx(6)
    assert compute["cpu_s"] == pytest.approx(8) and compute[
        "mean_cores"
    ] == pytest.approx(2)
    # outside the phases: 0-2 s and 9-10 s
    assert rest["duration_s"] == pytest.approx(3) and rest["cpu_s"] == pytest.approx(6)
    assert sum(r["cpu_s"] for r in rows) == pytest.approx(summarise(trace)["cpu_s"])
    # samples at 6..9 s; rss = i MB
    assert compute["peak_rss_mb"] == pytest.approx(9)
    assert math.isnan(by["compute"]["gpu_mem_peak_mb"])


def test_attribute_gives_short_phases_their_share():
    with tempfile.TemporaryDirectory() as d:
        rows = attribute(
            load_events(events(d, [("blip", 2.2, 2.4)])), load_denet(denet_trace(d))
        )
    blip = rows[0]
    assert (
        blip["samples"] == 1
        and blip["cpu_s"] == pytest.approx(0.4)
        and blip["peak_cores"] == 2
    )


def test_attribute_phase_outside_trace_and_pid_filter():
    with tempfile.TemporaryDirectory() as d:
        ph = load_events(events(d, [("late", 20, 30)]))
        trace = load_denet(denet_trace(d))
    late = attribute(ph, trace)[0]
    assert (
        late["samples"] == 0 and late["cpu_s"] == 0 and math.isnan(late["peak_cores"])
    )
    assert [r["event"] for r in attribute(ph, trace, pid=99)] == ["(unphased)"]


def test_attribute_gpu_and_overlapping_phases():
    with tempfile.TemporaryDirectory() as d:
        rows = attribute(
            load_events(events(d, [("outer", 1, 8), ("inner", 3, 4)])),
            load_denet(denet_trace(d, gpu=256)),
        )
    by = {r["event"]: r for r in rows}
    inner = by["inner"]
    assert inner["gpu_util_mean"] == pytest.approx(50)
    assert inner["gpu_mem_peak_mb"] == pytest.approx(256)
    # 0-1 and 8-10 s: nested phases are not counted twice
    assert by["(unphased)"]["duration_s"] == pytest.approx(3)


# ---- real trace: omni-scrna/scanpy pca.py (b98b70b) under denet 0.10.3 -----------


def test_real_scanpy_pca_trace():
    ph = load_events(DATA / "obkit-events-scanpy-pca.jsonl")
    trace = load_denet(DATA / "denet-scanpy-pca.jsonl")
    assert [p["event"] for p in ph] == ["load", "pca", "write"]
    rows = attribute(ph, trace)
    total = summarise(trace)
    assert sum(r["cpu_s"] for r in rows) == pytest.approx(total["cpu_s"], rel=1e-9)
    assert sum(r["duration_s"] for r in rows) == pytest.approx(
        total["duration_s"], rel=1e-9
    )
    unphased = rows[-1]
    # imports dominate a short PCA job
    assert unphased["event"] == "(unphased)"
    assert unphased["duration_s"] > 0.5 * total["duration_s"]


# ---- Snakemake benchmark ------------------------------------------------------


def test_load_snakemake_benchmark_na_and_empty():
    with tempfile.TemporaryDirectory() as d:
        p = os.path.join(d, "b.txt")
        with open(p, "w") as f:
            f.write("s\th:m:s\tmax_rss\tcpu_time\n3.2\t0:00:03\tNA\t1.5\n")
        rec = load_snakemake_benchmark(p)
        open(p, "w").close()
        assert load_snakemake_benchmark(p) is None
    assert rec["s"] == 3.2 and rec["max_rss"] is None and rec["h:m:s"] == "0:00:03"
