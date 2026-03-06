from types import SimpleNamespace
from typing import cast
from unittest.mock import AsyncMock

import pytest
from ib_async.contract import Contract, Option

from thetagang.config import Config
from thetagang.strategies.pmcc_engine import PMCCEngine


def _make_engine(mocker):
    config = SimpleNamespace(
        strategies=SimpleNamespace(
            pmcc=SimpleNamespace(
                enabled=False,
                symbols=["TQQQ"],
                leaps_delta=0.80,
                leaps_target_dte=365,
                leaps_min_dte=180,
                leaps_roll_dte=90,
                short_call_delta=0.30,
                short_call_dte=30,
                max_positions=1,
                roll_short_call_pnl=0.50,
                roll_short_call_dte=5,
            ),
        ),
        portfolio=SimpleNamespace(
            symbols={
                "TQQQ": SimpleNamespace(primary_exchange="NASDAQ"),
            },
        ),
        runtime=SimpleNamespace(
            orders=SimpleNamespace(
                minimum_credit=0.0,
                exchange="SMART",
                algo=SimpleNamespace(strategy="Adaptive", params=[]),
            ),
        ),
        trading_is_allowed=lambda symbol: True,
    )

    ibkr = mocker.Mock()
    ibkr.get_ticker_for_contract = AsyncMock()
    ibkr.get_ticker_for_stock = AsyncMock()

    option_scanner = mocker.Mock()
    option_scanner.find_eligible_contracts = AsyncMock()

    order_ops = mocker.Mock()
    order_ops.get_order_exchange = mocker.Mock(return_value="SMART")
    order_ops.create_limit_order = mocker.Mock(return_value="ORDER")
    order_ops.enqueue_order = mocker.Mock()

    return (
        PMCCEngine(
            config=cast(Config, config),
            ibkr=ibkr,
            option_scanner=option_scanner,
            order_ops=order_ops,
        ),
        ibkr,
        option_scanner,
        order_ops,
    )


def _make_long_call() -> SimpleNamespace:
    long_call = SimpleNamespace(
        contract=Option("TQQQ", "20270115", 50.0, "C", "SMART", currency="USD"),
        position=1.0,
        averageCost=5000.0,
        marketValue=5500.0,
        marketPrice=55.0,
        unrealizedPNL=500.0,
        account="DUX",
    )
    long_call.contract.conId = 12345
    long_call.contract.exchange = "SMART"
    long_call.contract.multiplier = "100"
    return long_call


def _make_short_call() -> SimpleNamespace:
    short_call = SimpleNamespace(
        contract=Option("TQQQ", "20260417", 65.0, "C", "SMART", currency="USD"),
        position=-1.0,
        averageCost=120.0,
        marketValue=-60.0,
        marketPrice=0.6,
        unrealizedPNL=60.0,
        account="DUX",
    )
    short_call.contract.conId = 54321
    short_call.contract.exchange = "SMART"
    short_call.contract.multiplier = "100"
    return short_call


def _make_candidate_ticker(con_id: int, expiry: str, strike: float, delta: float):
    ticker = SimpleNamespace(
        contract=Option("TQQQ", expiry, strike, "C", "SMART", currency="USD"),
        modelGreeks=SimpleNamespace(delta=delta),
    )
    ticker.contract.conId = con_id
    ticker.contract.multiplier = "100"
    return ticker


@pytest.mark.asyncio
async def test_manage_leaps_disabled_noops(mocker):
    engine, _ibkr, _scanner, order_ops = _make_engine(mocker)

    await engine.manage_leaps({}, {})

    order_ops.create_limit_order.assert_not_called()
    order_ops.enqueue_order.assert_not_called()


@pytest.mark.asyncio
async def test_manage_leaps_buys_new_when_no_positions(mocker):
    engine, _ibkr, scanner, order_ops = _make_engine(mocker)
    engine.config.strategies.pmcc.enabled = True
    scanner.find_eligible_contracts = AsyncMock(
        return_value=_make_candidate_ticker(99999, "20280119", 50.0, 0.85)
    )
    mocker.patch("thetagang.strategies.pmcc_engine.option_dte", return_value=400)
    mocker.patch(
        "thetagang.strategies.pmcc_engine.midpoint_or_market_price", return_value=55.0
    )

    await engine.manage_leaps({}, {})

    scanner.find_eligible_contracts.assert_awaited_once()
    assert scanner.find_eligible_contracts.call_args.kwargs["target_dte"] == 365
    order_ops.create_limit_order.assert_called_once()
    assert order_ops.create_limit_order.call_args.kwargs["action"] == "BUY"
    assert order_ops.create_limit_order.call_args.kwargs["quantity"] == 1
    order_ops.enqueue_order.assert_called_once()


@pytest.mark.asyncio
async def test_manage_leaps_skips_when_max_positions_reached(mocker):
    engine, _ibkr, scanner, order_ops = _make_engine(mocker)
    engine.config.strategies.pmcc.enabled = True
    portfolio_positions = {"TQQQ": [_make_long_call()]}
    mocker.patch("thetagang.strategies.pmcc_engine.option_dte", return_value=120)

    await engine.manage_leaps({}, portfolio_positions)

    scanner.find_eligible_contracts.assert_not_called()
    order_ops.create_limit_order.assert_not_called()
    order_ops.enqueue_order.assert_not_called()


@pytest.mark.asyncio
async def test_manage_leaps_rolls_when_dte_low(mocker):
    engine, ibkr, scanner, order_ops = _make_engine(mocker)
    engine.config.strategies.pmcc.enabled = True
    portfolio_positions = {"TQQQ": [_make_long_call()]}
    ibkr.get_ticker_for_contract = AsyncMock(
        return_value=SimpleNamespace(contract=Contract())
    )
    scanner.find_eligible_contracts = AsyncMock(
        return_value=_make_candidate_ticker(99999, "20280119", 55.0, 0.90)
    )
    mocker.patch("thetagang.strategies.pmcc_engine.option_dte", return_value=80)
    mocker.patch(
        "thetagang.strategies.pmcc_engine.midpoint_or_market_price",
        side_effect=[56.0, 50.0],
    )

    await engine.manage_leaps({}, portfolio_positions)

    order_ops.enqueue_order.assert_called_once()
    combo_contract = order_ops.enqueue_order.call_args.args[0]
    assert combo_contract.secType == "BAG"
    order_ops.create_limit_order.assert_called_once()
    assert order_ops.create_limit_order.call_args.kwargs["quantity"] == 1


@pytest.mark.asyncio
async def test_write_short_calls_disabled_noops(mocker):
    engine, _ibkr, _scanner, order_ops = _make_engine(mocker)

    await engine.write_short_calls({}, {})

    order_ops.create_limit_order.assert_not_called()
    order_ops.enqueue_order.assert_not_called()


@pytest.mark.asyncio
async def test_write_short_calls_sells_when_leaps_uncovered(mocker):
    engine, ibkr, scanner, order_ops = _make_engine(mocker)
    engine.config.strategies.pmcc.enabled = True
    portfolio_positions = {"TQQQ": [_make_long_call()]}
    ibkr.get_ticker_for_stock = AsyncMock(
        return_value=SimpleNamespace(marketPrice=lambda: 54.2)
    )
    scanner.find_eligible_contracts = AsyncMock(
        return_value=_make_candidate_ticker(77777, "20260515", 60.0, 0.30)
    )
    mocker.patch(
        "thetagang.strategies.pmcc_engine.midpoint_or_market_price", return_value=1.23
    )

    await engine.write_short_calls({}, portfolio_positions)

    order_ops.create_limit_order.assert_called_once()
    assert order_ops.create_limit_order.call_args.kwargs["action"] == "SELL"
    assert order_ops.create_limit_order.call_args.kwargs["quantity"] == 1
    order_ops.enqueue_order.assert_called_once()


@pytest.mark.asyncio
async def test_write_short_calls_skips_when_all_covered(mocker):
    engine, _ibkr, scanner, order_ops = _make_engine(mocker)
    engine.config.strategies.pmcc.enabled = True
    portfolio_positions = {"TQQQ": [_make_long_call(), _make_short_call()]}
    mocker.patch("thetagang.strategies.pmcc_engine.position_pnl", return_value=0.10)
    mocker.patch("thetagang.strategies.pmcc_engine.option_dte", return_value=20)

    await engine.write_short_calls({}, portfolio_positions)

    scanner.find_eligible_contracts.assert_not_called()
    order_ops.create_limit_order.assert_not_called()
    order_ops.enqueue_order.assert_not_called()


@pytest.mark.asyncio
async def test_write_short_calls_rolls_at_pnl_target(mocker):
    engine, ibkr, scanner, order_ops = _make_engine(mocker)
    engine.config.strategies.pmcc.enabled = True
    portfolio_positions = {"TQQQ": [_make_long_call(), _make_short_call()]}
    ibkr.get_ticker_for_contract = AsyncMock(
        return_value=SimpleNamespace(contract=Contract())
    )
    ibkr.get_ticker_for_stock = AsyncMock(
        return_value=SimpleNamespace(marketPrice=lambda: 64.0)
    )
    scanner.find_eligible_contracts = AsyncMock(
        return_value=_make_candidate_ticker(88888, "20260515", 70.0, 0.30)
    )
    mocker.patch("thetagang.strategies.pmcc_engine.position_pnl", return_value=0.60)
    mocker.patch("thetagang.strategies.pmcc_engine.option_dte", return_value=20)
    mocker.patch(
        "thetagang.strategies.pmcc_engine.midpoint_or_market_price",
        side_effect=[1.50, 1.00],
    )

    await engine.write_short_calls({}, portfolio_positions)

    order_ops.create_limit_order.assert_called_once()
    assert order_ops.create_limit_order.call_args.kwargs["action"] == "BUY"
    assert order_ops.create_limit_order.call_args.kwargs["quantity"] == 1
    order_ops.enqueue_order.assert_called_once()
    assert order_ops.enqueue_order.call_args.args[0].secType == "BAG"


@pytest.mark.asyncio
async def test_write_short_calls_rolls_at_low_dte(mocker):
    engine, ibkr, scanner, order_ops = _make_engine(mocker)
    engine.config.strategies.pmcc.enabled = True
    portfolio_positions = {"TQQQ": [_make_long_call(), _make_short_call()]}
    ibkr.get_ticker_for_contract = AsyncMock(
        return_value=SimpleNamespace(contract=Contract())
    )
    ibkr.get_ticker_for_stock = AsyncMock(
        return_value=SimpleNamespace(marketPrice=lambda: 64.0)
    )
    scanner.find_eligible_contracts = AsyncMock(
        return_value=_make_candidate_ticker(88889, "20260515", 70.0, 0.30)
    )
    mocker.patch("thetagang.strategies.pmcc_engine.position_pnl", return_value=0.10)
    mocker.patch("thetagang.strategies.pmcc_engine.option_dte", return_value=3)
    mocker.patch(
        "thetagang.strategies.pmcc_engine.midpoint_or_market_price",
        side_effect=[1.40, 1.00],
    )

    await engine.write_short_calls({}, portfolio_positions)

    order_ops.create_limit_order.assert_called_once()
    assert order_ops.create_limit_order.call_args.kwargs["action"] == "BUY"
    assert order_ops.create_limit_order.call_args.kwargs["quantity"] == 1
    order_ops.enqueue_order.assert_called_once()
    assert order_ops.enqueue_order.call_args.args[0].secType == "BAG"
