"""Phase 2 bridge and paper-engine integration tests (C++ bindings)."""
from __future__ import annotations

import gzip

import pytest

from python_analytics.bridge import PaperSession
from python_analytics.native import hft_native

DEPTH1 = (
    '{"e":"depthUpdate","E":1000,"U":1,"u":2,"pu":0,'
    '"b":[["30000.00","10"],["29999.50","4"]],'
    '"a":[["30000.50","2"],["30001.00","3"]]}'
)


def test_ingest_and_market_slippage() -> None:
    session = PaperSession.create("0.5", "30000", taker_bps=5, initial_cash=100_000.0)
    stats = session.feed([DEPTH1])
    assert stats["depth"] == 1
    assert stats["malformed"] == 0
    assert stats["gaps"] == 0

    result = session.market(hft_native.Side.BUY, "4")
    assert result["accepted"] is True
    assert result["filled"] == pytest.approx(4.0)
    assert result["average_price"] == pytest.approx(30000.75)
    assert result["slippage"] == pytest.approx(1.0)

    metrics = session.metrics()
    assert metrics["fills"] == 2
    assert metrics["net_quantity"] == pytest.approx(4.0)
    assert metrics["average_entry_price"] == pytest.approx(30000.75)
    assert metrics["fees_paid"] == pytest.approx(60.0015)
    assert metrics["slippage_total"] == pytest.approx(1.0)
    assert len(session.fills) == 2
    assert session.fills[0]["is_maker"] is False
    assert session.fills[0]["price"] == pytest.approx(30000.5)


def test_gap_detection_via_pu() -> None:
    session = PaperSession.create("0.5", "30000")
    contiguous = (
        '{"e":"depthUpdate","E":1100,"U":3,"u":4,"pu":2,'
        '"b":[["30000.00","1"]],"a":[["30001.00","1"]]}'
    )
    session.feed([DEPTH1, contiguous])
    assert session.metrics()["gap_events"] == 0

    gapped = (
        '{"e":"depthUpdate","E":1200,"U":9,"u":10,"pu":7,'
        '"b":[["30000.00","1"]],"a":[["30001.00","1"]]}'
    )
    session.feed([gapped])
    assert session.metrics()["gap_events"] == 1


def test_malformed_events_counted() -> None:
    session = PaperSession.create("0.5", "30000")
    stats = session.feed([DEPTH1, "not json"])
    assert stats["depth"] == 1
    assert stats["malformed"] == 1
    assert session.metrics()["depth_events"] == 1


def test_per_frame_latency_telemetry() -> None:
    session = PaperSession.create("0.5", "30000")
    session.feed([DEPTH1] * 8)
    metrics = session.metrics()
    assert metrics["depth_events"] == 8
    assert metrics["last_frame_latency_ns"] >= 0
    assert metrics["p50_ns"] >= 0


def test_feed_archive_gzip(tmp_path) -> None:
    session = PaperSession.create("0.5", "30000")
    archive = tmp_path / "capture.ndjson.gz"
    with gzip.open(archive, "wt", encoding="utf-8") as handle:
        handle.write(DEPTH1 + "\n")
        handle.write(DEPTH1 + "\n")
    totals = session.feed_file(archive, batch_size=1)
    assert totals["depth"] == 2
    assert totals["malformed"] == 0
    assert session.metrics()["depth_events"] == 2


def test_resting_limit_maker_fill() -> None:
    session = PaperSession.create("0.5", "30000", maker_bps=-1, taker_bps=5,
                                  initial_cash=100_000.0)
    session.feed([DEPTH1])
    request = hft_native.OrderRequest()
    request.side = hft_native.Side.BUY
    request.type = hft_native.OrderType.LIMIT
    request.price = hft_native.Price.from_string("29999.5")
    request.quantity = hft_native.Quantity.from_string("1")
    ack = session.engine.submit(request)
    assert ack.accepted()
    assert session.metrics()["open_orders"] == 1
    assert session.metrics()["fills"] == 0

    crossed = (
        '{"e":"depthUpdate","E":2000,"U":5,"u":6,"pu":2,'
        '"b":[["30000.00","1"]],"a":[["29999.00","1"]]}'
    )
    session.feed([crossed])
    session._drain_fills()
    metrics = session.metrics()
    assert metrics["fills"] == 1
    assert metrics["open_orders"] == 0
    assert metrics["maker_fills"] == 1
    assert metrics["fees_paid"] == pytest.approx(-2.99995)
    assert session.fills[0]["is_maker"] is True
    assert session.fills[0]["price"] == pytest.approx(29999.5)
