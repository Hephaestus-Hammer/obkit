import json
import os
import socket
import sys
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path

_state = {
    "log_file": None,
    "warned_uninit": False,
}


def init_logger(path):
    if not isinstance(path, (str, Path)) or not str(path):
        raise ValueError("init_logger(): 'path' must be a non-empty string or Path")
    os.makedirs(path, exist_ok=True)
    log_file = os.path.join(os.path.realpath(path), "obkit-events.jsonl")
    _state["log_file"] = log_file
    _state["warned_uninit"] = False
    return log_file


def _iso_ts():
    now = datetime.now(timezone.utc)
    return now.strftime("%Y-%m-%dT%H:%M:%S.") + f"{now.microsecond // 1000:03d}Z"


def emit(event, phase, attrs=None):
    if not isinstance(event, str) or not event:
        raise ValueError("emit(): 'event' must be a non-empty string")
    if phase not in ("start", "end"):
        raise ValueError('emit(): \'phase\' must be "start" or "end"')

    log_file = _state["log_file"]
    if log_file is None:
        if not _state["warned_uninit"]:
            print(
                "obkit: emit() called before init_logger(); events discarded.",
                file=sys.stderr,
            )
            _state["warned_uninit"] = True
        return False

    rec = {
        "ts": _iso_ts(),
        "event": event,
        "phase": phase,
        "pid": os.getpid(),
        "host": socket.gethostname(),
    }
    if attrs is not None:
        if not isinstance(attrs, dict):
            raise ValueError("emit(): 'attrs' must be a dict")
        rec["attrs"] = attrs

    try:
        line = json.dumps(rec, separators=(",", ":"))
        with open(log_file, "a", encoding="utf-8") as f:
            f.write(line + "\n")
        return True
    except Exception as e:  # noqa: BLE001 - logging must never crash the module
        print(f"obkit: emit() write failed: {e}", file=sys.stderr)
        return False


@contextmanager
def phase(name, sync=None):
    """Emit ``start``/``end`` events around a block.

    Yields a dict; whatever the block puts in it is logged with the ``end`` event::

        with phase("load") as attrs:
            adata = load(path)
            attrs["n_cells"] = adata.n_obs

    ``sync``: a callable run before the ``end`` event, e.g.
    ``cupy.cuda.Device().synchronize``. GPU work is asynchronous: without it a phase
    can end while its kernels still run, and their time shows up in the next phase.
    If ``sync`` raises, a warning is printed and the ``end`` event is still written.
    The ``end`` event is written even if the block raises.
    """
    attrs = {}
    emit(name, "start")
    try:
        yield attrs
    finally:
        if sync is not None:
            try:
                sync()
            except Exception as e:  # noqa: BLE001 - a failed sync must not lose the end event
                print(f"obkit: phase({name!r}) sync failed: {e}", file=sys.stderr)
        emit(name, "end", attrs=attrs or None)
