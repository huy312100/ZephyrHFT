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
    FeeWall,
    gross_edge_bps,
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


# --------------------------------------------------------------------------- #
# The fee wall: the maker fee a measured edge can pay before it stops paying
# --------------------------------------------------------------------------- #
def test_gross_edge_is_pnl_per_unit_of_traded_notional():
    # 2.5 USDT earned while trading 50,000 USDT of notional is 0.5 bps.
    assert gross_edge_bps(2.5, 50_000.0) == pytest.approx(0.5)
    assert gross_edge_bps(-2.5, 50_000.0) == pytest.approx(-0.5)


def test_gross_edge_refuses_to_invent_an_edge_out_of_nothing():
    """Nothing traded means no unit to charge a fee against, not an edge of 0."""
    assert gross_edge_bps(-1.0, 0.0) is None
    assert FeeWall.from_study(pre_fee_pnl=-1.0, traded_notional=0.0,
                              maker_bps=2.0) is None


def test_fee_wall_compares_edge_and_fee_in_the_same_unit():
    wall = FeeWall.from_study(pre_fee_pnl=0.5, traded_notional=25_000.0,
                              maker_bps=2.0, fills=12)
    assert wall.gross_bps == pytest.approx(0.2)
    assert wall.net_bps == pytest.approx(-1.8)
    assert wall.viable is False
    assert wall.fee_multiple == pytest.approx(10.0)
    assert (wall.fills, wall.traded_notional) == (12, 25_000.0)


def test_fee_wall_at_exact_break_even_is_not_viable():
    """An edge equal to the fee survives nothing, so the comparison is strict."""
    wall = FeeWall.from_study(pre_fee_pnl=1.0, traded_notional=10_000.0,
                              maker_bps=1.0)
    assert wall.gross_bps == pytest.approx(1.0)
    assert wall.viable is False


def test_fee_wall_reports_no_multiple_when_the_edge_is_zero():
    wall = FeeWall.from_study(pre_fee_pnl=0.0, traded_notional=10_000.0,
                              maker_bps=2.0)
    assert wall.fee_multiple is None
    assert "break-even maker fee +0.000 bps" in wall.describe()
    assert "2 bps maker fee" in wall.describe()


def test_fee_wall_clears_a_rebate():
    wall = FeeWall.from_study(pre_fee_pnl=0.3, traded_notional=10_000.0,
                              maker_bps=-1.0)
    assert wall.net_bps == pytest.approx(1.3)
    assert wall.viable is True
