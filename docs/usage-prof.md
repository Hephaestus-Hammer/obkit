# prof — usage

Parse and align profiler output (denet, Snakemake bench files) with
logger events for per-phase resource attribution.

> **Status:** the Python submodule (`obkit.prof`) is implemented; the
> R prefix group (`prof_*`) is in progress.

---

## Concept

A sampling profiler such as [denet](https://github.com/btraven00/denet)
records CPU / RSS / IO at a fixed cadence. The logger records sparse
phase anchors (`align start`, `align end`, …). `prof` joins the two on
timestamp, attributing profiler samples to the phase active at each
sample time.

```
denet samples:       ~~.~..~~..~~..~~..~~..~~..
logger events:  |start "align"                    |end "align"
                └──────────── attributed ──────────┘
```

The result is a per-phase resource summary without any instrumentation
inside the profiler or changes to the workflow rule.

---

## Python (`obkit.prof`)

Zero dependencies (pure stdlib), Python ≥ 3.9. Every function returns
plain dicts and lists; `pandas.DataFrame(rows)` turns them into a table.

```python
from obkit.prof import load_events, load_denet, attribute, summarise

events = load_events("out/obkit-events.jsonl")
trace = load_denet("out/denet.jsonl")
for row in attribute(events, trace):
    print(row["event"], row["duration_s"], row["cpu_s"], row["mean_cores"])
print(summarise(trace))
```

### `load_events(path)`

Reads a logger file (see [spec](spec-logger.md)) and pairs each `start`
with the next `end` of the same event from the same PID. Returns one dict
per phase: `event`, `pid`, `start_ms`, `end_ms` (ms since the epoch,
UTC) and `attrs` (from the end record, else the start record), in the
order the phases ended. An unmatched `start` or `end` raises a warning and is
dropped; a malformed line (e.g. a job killed mid-write) is skipped.

### `load_denet(path)`

Reads a denet `--json` trace (tested with denet 0.10.3: `metadata`,
`tree` and `exit` records; flat `sample` records also work). Returns
`{t0_ms, end_ms, pid, cmd, samples}` or `None` if the file has no
samples. Each sample has `ts_ms`, `cpu_usage` (%; 100 = one busy core;
the process and its children), `mem_rss_kb`, `thread_count`, and, when
denet ran with `--gpu`, `gpu_util` (%) and `gpu_mem_bytes`. GPU memory
is the process's own where NVML reports it, otherwise the device total
(which counts other processes on that GPU).

### `attribute(events, trace, pid=None)`

One row per phase plus a final `(unphased)` row for the time no phase
covers (interpreter start-up, imports, anything not wrapped in a phase).
`pid` keeps only phases logged by that PID.

| field | meaning |
|-------|---------|
| `duration_s` | phase wall time |
| `samples` | samples falling inside the phase |
| `cpu_s` | CPU time: `cpu_usage` integrated over the phase |
| `mean_cores` | `cpu_s / duration_s`: average busy cores |
| `peak_cores` | highest sampled `cpu_usage / 100` |
| `peak_rss_mb`, `peak_threads` | highest sampled value |
| `gpu_util_mean` | time-weighted mean GPU utilisation (%) |
| `gpu_mem_peak_mb` | highest sampled GPU memory |

Each sample's `cpu_usage` covers the interval since the previous sample,
so CPU time is split by overlap: a phase shorter than one sampling
interval still gets its share. For non-overlapping phases the `cpu_s` of
all rows (including `(unphased)`) adds up to the trace's total. Nested
phases each get their own row; `(unphased)` counts only time outside
every phase.

### `summarise(trace)`

The same fields for the whole trace, as one `(total)` row.

### `load_snakemake_benchmark(path)`

Reads a Snakemake `benchmark:` file (`s`, `max_rss`, `cpu_time`, …) into
a dict, with non-numeric values such as `NA` as `None`. Snakemake samples every 0.5 s for the
first 15 s and every 30 s after that, so for longer jobs `max_rss` and
`cpu_time` are lower bounds. No phases: use it as the whole-job fallback
when no denet trace exists.

---

## R (`prof_*`, in progress)

```r
library(obkit)

prof_load_events(log_file)
prof_load_denet(denet_file)
prof_attribute(events, samples)
```
