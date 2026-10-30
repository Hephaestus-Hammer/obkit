# prof — parse and align profiling outputs (denet, Snakemake bench files)
# with logger events for per-phase resource attribution.
"""Attribute profiler samples to the phases an obkit logger recorded.

Three calls, pure stdlib:

    from obkit.prof import load_events, load_denet, attribute

    phases = load_events("out/obkit-events.jsonl")   # [{event, pid, start_ms, end_ms, attrs}]
    trace = load_denet("out/denet.jsonl")            # {t0_ms, end_ms, pid, samples: [...]}
    rows = attribute(phases, trace)                  # one dict per phase, plus "(unphased)"

Both streams carry wall-clock milliseconds since the Unix epoch, so they join on
time. A denet sample's ``cpu_usage`` is the average over the interval since the
previous sample, so each sample's CPU is spread over that interval and a phase
gets the part that overlaps it: phases shorter than one sample still get their
share, and ``samples`` says how many samples overlap a phase (how well resolved
it is). Peaks (cores, memory, GPU) are the maxima over the overlapping samples.
"""

import json
import math
import warnings
from datetime import datetime, timezone

__all__ = [
    "attribute",
    "load_denet",
    "load_events",
    "load_snakemake_benchmark",
    "summarise",
]


def _json_lines(path):
    """Parsed JSON objects of a JSONL file; malformed lines (e.g. a trailing partial
    line from a killed writer) are skipped, as the logger spec requires of readers."""
    out = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(rec, dict):
                out.append(rec)
    return out


def _iso_to_ms(ts):
    """ISO-8601 UTC timestamp (spec: millisecond precision, 'Z') -> ms since epoch."""
    if ts.endswith("Z"):
        ts = ts[:-1] + "+00:00"
    dt = datetime.fromisoformat(ts)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.timestamp() * 1000.0


def load_events(path):
    """Phase ranges from an obkit logger event file.

    Pairs ``start``/``end`` events on ``(pid, event)`` in file order, like the R
    ``read_phase_ranges()``. Returns one dict per completed pair, in order of the
    end events: ``event``, ``pid``, ``start_ms``, ``end_ms`` (floats, ms since epoch)
    and ``attrs`` (the end event's attrs, else the start's, else ``{}``). Unmatched
    starts and ends are dropped with a warning.
    """
    open_ = {}
    out = []
    for r in _json_lines(path):
        event, phase, ts = r.get("event"), r.get("phase"), r.get("ts")
        if not event or phase not in ("start", "end") or not ts:
            continue
        try:
            t = _iso_to_ms(ts)
        except ValueError:
            continue
        key = (r.get("pid"), event)
        if phase == "start":
            open_[key] = (t, r.get("attrs") or {})
        else:
            if key not in open_:
                warnings.warn(
                    f"load_events(): unmatched end for event={event} pid={key[0]}"
                )
                continue
            t0, start_attrs = open_.pop(key)
            out.append(
                {
                    "event": event,
                    "pid": key[0],
                    "start_ms": t0,
                    "end_ms": t,
                    "attrs": r.get("attrs") or start_attrs,
                }
            )
    if open_:
        warnings.warn(f"load_events(): {len(open_)} unmatched start event(s)")
    return out


def _gpu(record, aggregated):
    """(utilisation %, memory bytes) from one sample's GPU block, or (None, None).

    denet 0.10.3 writes ``gpu`` under ``parent`` and ``aggregated``, with
    ``process_data`` (per monitored PID: ``gpu_utilization``, ``memory_usage``) when
    NVML can see the process, and ``system_metrics`` (per device:
    ``system_utilization_gpu``, ``memory_used``) always. The process's own numbers
    are preferred; else the devices' (which include anything else on the GPU).
    An older/doc layout (``devices`` with ``process_memory_usage``,
    ``max_gpu_utilization``) is read too.
    """

    def vals(rows, *keys):
        out = []
        for d in rows:
            for k in keys:
                if isinstance(d, dict) and d.get(k) is not None:
                    out.append(d[k])
                    break
        return out

    for g in ((aggregated or {}).get("gpu"), (record.get("parent") or {}).get("gpu")):
        if not isinstance(g, dict) or not g:
            continue
        procs = g.get("process_data") or []
        devs = g.get("system_metrics") or g.get("devices") or []
        p_util = vals(procs, "gpu_utilization")
        p_mem = vals(procs, "memory_usage") or vals(devs, "process_memory_usage")
        d_util = vals(
            devs, "system_utilization_gpu", "utilization_gpu", "gpu_utilization"
        )
        d_mem = vals(devs, "memory_used")
        util = g.get("max_gpu_utilization")
        if util is None:
            util = sum(p_util) if p_util else (max(d_util) if d_util else None)
        mem = sum(p_mem) if p_mem else g.get("total_memory_used")
        if mem is None and d_mem:
            mem = sum(d_mem)
        return util, mem
    return None, None


def load_denet(path):
    """Samples from a denet JSONL trace (``denet --json --out FILE run ...``).

    Returns ``{"t0_ms", "end_ms", "pid", "cmd", "samples"}``. ``t0_ms`` comes from the
    ``metadata`` line (else the first sample), ``end_ms`` from the ``exit`` line (else
    the last sample). Each sample is a dict with ``ts_ms``, ``cpu_usage`` (% of one
    core), ``mem_rss_kb``, ``thread_count``, ``gpu_util`` (%) and ``gpu_mem_bytes``
    (None without GPU monitoring). Process-tree traces (``tree`` lines) use the
    ``aggregated`` metrics of the process and its children; single-process traces
    (``sample`` lines) their own. ``env``, ``child`` and unknown lines are ignored,
    as are malformed lines.
    """
    t0 = end = pid = cmd = None
    samples = []
    for r in _json_lines(path):
        kind = r.get("kind")
        if kind == "metadata":
            t0, pid, cmd = r.get("t0_ms"), r.get("pid"), r.get("cmd")
        elif kind == "exit":
            end = r.get("ts_ms")
        elif kind in ("tree", "sample") or (kind is None and "aggregated" in r):
            a = r.get("aggregated") if kind != "sample" else r
            if not isinstance(a, dict):
                continue
            ts = r.get("ts_ms", a.get("ts_ms"))
            if ts is None:
                continue
            util, mem = _gpu(r, a)
            samples.append(
                {
                    "ts_ms": float(ts),
                    "cpu_usage": float(a.get("cpu_usage") or 0.0),
                    "mem_rss_kb": a.get("mem_rss_kb") or 0,
                    "thread_count": a.get("thread_count") or 0,
                    "gpu_util": util,
                    "gpu_mem_bytes": mem,
                }
            )
    samples.sort(key=lambda s: s["ts_ms"])
    if samples:
        t0 = float(t0) if t0 is not None else samples[0]["ts_ms"]
        end = float(end) if end is not None else samples[-1]["ts_ms"]
    return {"t0_ms": t0, "end_ms": end, "pid": pid, "cmd": cmd, "samples": samples}


def load_snakemake_benchmark(path):
    """The one record of a Snakemake ``benchmark:`` file, as a dict of floats (``s``,
    ``max_rss`` in MB, ``cpu_time``, ...; NA becomes None), or None if empty.

    Snakemake samples every 0.5 s for 15 s and then every 30 s, and loses what
    happens after the last sample: for longer jobs ``cpu_time`` and ``max_rss`` are
    lower bounds. There is no phase attribution for it (one record per job).
    """
    with open(path, encoding="utf-8") as f:
        lines = [ln.rstrip("\n") for ln in f if ln.strip()]
    if len(lines) < 2:
        return None
    head, row = lines[0].split("\t"), lines[1].split("\t")

    def num(v):
        try:
            return float(v)
        except ValueError:
            return None

    return {k: (v if k == "h:m:s" else num(v)) for k, v in zip(head, row)}


def _intervals(trace):
    """(start_ms, end_ms, sample) per sample: the interval its averages cover."""
    out, prev = [], trace["t0_ms"]
    for s in trace["samples"]:
        start = prev if prev is not None and prev < s["ts_ms"] else s["ts_ms"]
        out.append((start, s["ts_ms"], s))
        prev = s["ts_ms"]
    return out


def _stats(intervals, a, b):
    """Resources of the samples whose intervals overlap [a, b] (ms)."""
    cpu_ms, n = 0.0, 0
    peak_cores = peak_rss = peak_threads = 0
    gpu_util, gpu_w, gpu_mem = 0.0, 0.0, None
    for s0, s1, s in intervals:
        overlap = min(s1, b) - max(s0, a)
        point = s0 == s1 and a <= s1 <= b
        if overlap <= 0 and not point:
            continue
        n += 1
        overlap = max(overlap, 0.0)
        cpu_ms += s["cpu_usage"] / 100.0 * overlap
        peak_cores = max(peak_cores, s["cpu_usage"] / 100.0)
        peak_rss = max(peak_rss, s["mem_rss_kb"])
        peak_threads = max(peak_threads, s["thread_count"])
        if s["gpu_util"] is not None and overlap > 0:
            gpu_util += s["gpu_util"] * overlap
            gpu_w += overlap
        if s["gpu_mem_bytes"] is not None:
            gpu_mem = max(gpu_mem or 0, s["gpu_mem_bytes"])
    dur_s = max(b - a, 0.0) / 1000.0
    cpu_s = cpu_ms / 1000.0
    return {
        "duration_s": dur_s,
        "samples": n,
        "cpu_s": cpu_s,
        "mean_cores": cpu_s / dur_s if dur_s > 0 else math.nan,
        "peak_cores": peak_cores if n else math.nan,
        "peak_rss_mb": peak_rss / 1024.0 if n else math.nan,
        "peak_threads": peak_threads if n else None,
        "gpu_util_mean": gpu_util / gpu_w if gpu_w > 0 else math.nan,
        "_gpu_ms": gpu_w,  # weight for combining windows; dropped from public rows
        "gpu_mem_peak_mb": gpu_mem / 2**20 if gpu_mem is not None else math.nan,
    }


def _uncovered(spans, a, b):
    """Parts of [a, b] covered by none of the spans."""
    gaps, cur = [], a
    for s0, s1 in sorted(spans):
        if s1 <= cur:
            continue
        if s0 > cur:
            gaps.append((cur, min(s0, b)))
        cur = max(cur, s1)
        if cur >= b:
            break
    if cur < b:
        gaps.append((cur, b))
    return [(x, y) for x, y in gaps if y > x]


def _public(stats):
    return {k: v for k, v in stats.items() if not k.startswith("_")}


def attribute(events, trace, pid=None):
    """Per-phase resources: one dict per phase in ``events`` (from ``load_events``)
    over ``trace`` (from ``load_denet``), in event order, then one ``"(unphased)"`` row
    for the trace time outside every phase (start-up, imports, teardown).

    Each row: ``event``, ``pid``, ``start_ms``, ``end_ms``, ``duration_s``, ``samples``
    (samples overlapping the phase), ``cpu_s``, ``mean_cores`` (cpu_s / duration_s),
    ``peak_cores``, ``peak_rss_mb``, ``peak_threads``, ``gpu_util_mean`` (time-weighted)
    and ``gpu_mem_peak_mb`` (NaN without GPU data), and ``attrs``. Phases outside the
    trace get zero samples and NaN peaks. ``pid`` restricts ``events`` to one process
    (default: all; denet's pid is the traced command's).
    """
    ivs = _intervals(trace)
    rows = []
    evs = [e for e in events if pid is None or e["pid"] == pid]
    for e in evs:
        rows.append(
            {
                "event": e["event"],
                "pid": e["pid"],
                "start_ms": e["start_ms"],
                "end_ms": e["end_ms"],
                **_public(_stats(ivs, e["start_ms"], e["end_ms"])),
                "attrs": e.get("attrs") or {},
            }
        )
    if trace["t0_ms"] is not None:
        gaps = _uncovered(
            [(e["start_ms"], e["end_ms"]) for e in evs], trace["t0_ms"], trace["end_ms"]
        )
        parts = [_stats(ivs, x, y) for x, y in gaps]
        dur = sum(p["duration_s"] for p in parts)
        cpu = sum(p["cpu_s"] for p in parts)
        gpu_ms = sum(p["_gpu_ms"] for p in parts)
        gpu_mean = (
            sum(p["gpu_util_mean"] * p["_gpu_ms"] for p in parts if p["_gpu_ms"] > 0)
            / gpu_ms
            if gpu_ms > 0
            else math.nan
        )

        def peak(key):
            vals = [
                p[key]
                for p in parts
                if p[key] is not None
                and not (isinstance(p[key], float) and math.isnan(p[key]))
            ]
            return max(vals) if vals else math.nan

        rows.append(
            {
                "event": "(unphased)",
                "pid": None,
                "start_ms": trace["t0_ms"],
                "end_ms": trace["end_ms"],
                "duration_s": dur,
                "samples": sum(p["samples"] for p in parts),
                "cpu_s": cpu,
                "mean_cores": cpu / dur if dur > 0 else math.nan,
                "peak_cores": peak("peak_cores"),
                "peak_rss_mb": peak("peak_rss_mb"),
                "peak_threads": peak("peak_threads"),
                "gpu_util_mean": gpu_mean,
                "gpu_mem_peak_mb": peak("gpu_mem_peak_mb"),
                "attrs": {},
            }
        )
    return rows


def summarise(trace):
    """Whole-trace resources, the same fields as an ``attribute`` row (event "(total)")."""
    if trace["t0_ms"] is None:
        return None
    return {
        "event": "(total)",
        "pid": trace["pid"],
        "start_ms": trace["t0_ms"],
        "end_ms": trace["end_ms"],
        **_public(_stats(_intervals(trace), trace["t0_ms"], trace["end_ms"])),
        "attrs": {},
    }
