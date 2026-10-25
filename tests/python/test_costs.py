"""Fee assumptions shared by the studies and the readiness gates.

The point of these tests is that a *taker* round trip is charged the taker rate
on both fills: an aggressive entry and exit never rest, so a maker rebate cannot
offset either side.
"""
from __future__ import annotations

import pytest

from python_analytics.costs import (
    DEFAULT_MAKER_BPS,
    DEFAULT_TAKER_BPS,
    round_trip_cost_bps,
)


def test_round_trip_defaults_to_a_passive_maker_on_both_sides():
    assert round_trip_cost_bps() == pytest.approx(2.0 * DEFAULT_MAKER_BPS)


def test_round_trip_taker_side_charges_the_taker_rate_twice():
    assert round_trip_cost_bps(both_sides_maker=False) == pytest.approx(
        2.0 * DEFAULT_TAKER_BPS)


def test_round_trip_taker_side_ignores_the_maker_rate():
    assert round_trip_cost_bps(-1.0, both_sides_maker=False) == pytest.approx(
        2.0 * DEFAULT_TAKER_BPS)
    assert round_trip_cost_bps(
        2.0, both_sides_maker=False, taker_bps=4.0) == pytest.approx(8.0)


def test_round_trip_maker_rebate_is_a_negative_cost():
    assert round_trip_cost_bps(-1.0) == pytest.approx(-2.0)
