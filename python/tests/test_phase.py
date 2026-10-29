import json
import tempfile

import pytest

from obkit.logger import init_logger, phase


def read(p):
    with open(p) as f:
        return [json.loads(line) for line in f]


def test_phase_emits_start_end_with_attrs():
    with tempfile.TemporaryDirectory() as d:
        log = init_logger(d)
        with phase("load") as attrs:
            attrs["n_cells"] = 3
        ev = read(log)
    assert [(e["event"], e["phase"]) for e in ev] == [
        ("load", "start"),
        ("load", "end"),
    ]
    assert ev[1]["attrs"] == {"n_cells": 3} and "attrs" not in ev[0]


def test_phase_runs_sync_before_end_and_ends_on_error():
    with tempfile.TemporaryDirectory() as d:
        log = init_logger(d)
        calls = []

        def sync():
            calls.append(len(read(log)))

        with pytest.raises(RuntimeError), phase("gpu", sync=sync):
            raise RuntimeError("boom")
        ev = read(log)
    assert calls == [1]  # sync ran after start, before end
    assert [e["phase"] for e in ev] == ["start", "end"]


def test_phase_sync_failure_still_writes_end(capsys):
    with tempfile.TemporaryDirectory() as d:
        log = init_logger(d)

        def bad():
            raise ValueError("no device")

        with phase("gpu", sync=bad):
            pass
        ev = read(log)
    assert [e["phase"] for e in ev] == ["start", "end"]
    assert "sync failed" in capsys.readouterr().err
