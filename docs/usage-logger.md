# logger — usage

Structured JSONL lifecycle event logger. Wire format: [spec-logger.md](spec-logger.md).

Two calls cover the full API: initialise once, emit at phase boundaries.

---

## Python

`logger` is a submodule of `obkit`. Zero dependencies (pure stdlib).

### Install

```bash
pip install obkit
```

### API

```python
from obkit.logger import init_logger, emit, phase

init_logger("/path/to/logs")
```

`init_logger(path)` creates the directory if needed and sets the output
file to `<path>/obkit-events.jsonl`. Must be called before any `emit`.

```python
emit("align", "start")
# … do work …
emit("align", "end")
```

`emit(event, phase, attrs=None)` appends one JSONL record with a UTC
timestamp, PID, and hostname.

| argument | type | notes |
|----------|------|-------|
| `event`  | `str` | caller-supplied phase name, e.g. `"align"` |
| `phase`  | `str` | `"start"` or `"end"` |
| `attrs`  | `dict` | optional extra scalar fields |

Returns `True` on success, `False` on I/O failure (never raises on I/O).
Raises `ValueError` on bad arguments.

### `phase(name, sync=None)`

A context manager that emits `start` on entry and `end` on exit, also
when the block raises. It yields a dict; whatever the block puts in it
is written as the `end` record's `attrs`.

```python
from obkit.logger import phase

with phase("pca", sync=cupy.cuda.Device().synchronize) as attrs:
    run_pca()
    attrs["n_comps"] = 50
```

`sync` is called just before the `end` event. Use it for asynchronous
work such as GPU kernels, so the phase ends when the work does rather
than when it was queued. If `sync` fails, the error is printed to
stderr and the `end` event is still written.

### Example

```python
from obkit.logger import init_logger, emit

init_logger("/data/logs/run42")

emit("download", "start")
fetch_data()
emit("download", "end")

emit("align", "start", attrs={"ref": "hg38", "threads": 8})
run_aligner()
emit("align", "end")
```

---

## R

`obkit` is a single package. Logger functions use the `logger_` prefix.
One dependency: `jsonlite`.

### Install

```r
# from source
devtools::install("r/obkit")
```

### API

```r
library(obkit)

logger_init(path)
```

`logger_init(path)` creates the directory if needed and configures the
output file to `<path>/obkit-events.jsonl`. Returns the resolved
path invisibly.

```r
logger_emit(event, phase, attrs = NULL)
```

`logger_emit()` appends one JSONL record. Returns `TRUE` invisibly on
success, `FALSE` on I/O failure (never stops on I/O). Stops on bad
arguments.

| argument | type | notes |
|----------|------|-------|
| `event`  | character(1) | caller-supplied phase name |
| `phase`  | character(1) | `"start"` or `"end"` |
| `attrs`  | named list   | optional extra scalar fields |

### Example

```r
library(obkit)

logger_init("/data/logs/run42")

logger_emit("download", "start")
fetch_data()
logger_emit("download", "end")

logger_emit("align", "start", attrs = list(ref = "hg38", threads = 8L))
run_aligner()
logger_emit("align", "end")
```
