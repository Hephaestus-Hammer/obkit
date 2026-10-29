# obkit

[![obkit status badge](https://omnibenchmark.r-universe.dev/obkit/badges/version)](https://omnibenchmark.r-universe.dev/obkit)

Utility toolkit for [omnibenchmark](https://omnibenchmark.org) modules and
snakemake workflows. Available for Python and R. Zero or near-zero
dependencies.

## Modules

| module   | purpose |
|----------|---------|
| `logger` | Structured JSONL lifecycle event logging — drop phase-boundary anchors into a log file so downstream tooling can attribute profiler samples to named phases of a rule. [Wire format spec.](docs/spec-logger.md) |
| `prof`   | Parse and align profiler output (denet, Snakemake bench files) with logger events for per-phase resource attribution. Python done; R in progress. |

## Language implementations

### Python

`logger` and `prof` are submodules of the `obkit` package:

```python
from obkit.logger import init_logger, emit
from obkit.prof import load_events, load_denet, attribute, summarise
```

Zero dependencies — pure stdlib.

```bash
pip install obkit
```

### R

`obkit` is a single package. Modules map to function-prefix groups:
`logger_init()` / `logger_emit()` for logger, `prof_*()` for prof.
This is idiomatic R — there are no submodule namespaces, but the
prefix makes the grouping explicit.

```r
library(obkit)
logger_init("/path/to/logs")
logger_emit("align", "start")
```

One dependency: `jsonlite`.

```r
devtools::install("r/obkit")
```

## Documentation

- [Logger usage](docs/usage-logger.md)
- [Prof usage](docs/usage-prof.md)
- [Logger wire format spec](docs/spec-logger.md)

## Repository layout

```
python/obkit/
  logger/      # submodule: init_logger, emit, phase
  prof/        # submodule: load_events, load_denet, attribute, summarise
r/obkit/
  R/
    logger.R   # logger_init, logger_emit
    prof.R     # prof_* (in progress)
docs/
  spec-logger.md
  usage-logger.md
  usage-prof.md
```

## Status

v0.1, pre-release. Wire format is stable; API may still shift.
