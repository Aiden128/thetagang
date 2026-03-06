from __future__ import annotations

from copy import deepcopy
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import AsyncMock

import pytest
from ib_async.contract import Option, Stock

from thetagang.config import Config


def _merge_dict(base: dict[str, Any], overrides: dict[str, Any]) -> dict[str, Any]:
    for key, value in overrides.items():
        if isinstance(value, dict) and isinstance(base.get(key), dict):
            _merge_dict(base[key], value)
        else:
            base[key] = value
    return base


def _base_config(**overrides: Any) -> dict[str, Any]:
    base = {
        "meta": {"schema_version": 2},
        "run": {"strategies": ["wheel"]},
        "runtime": {
            "account": {"number": "DUX", "margin_usage": 0.5},
            "option_chains": {"expirations": 4, "strikes": 10},
        },
        "portfolio": {"symbols": {"TQQQ": {"weight": 1.0}}},
        "strategies": {
            "wheel": {
                "defaults": {
                    "target": {"dte": 30, "minimum_open_interest": 5},
                    "roll_when": {"dte": 7},
                }
            }
        },
    }
    merged = deepcopy(base)
    _merge_dict(merged, overrides)
    return merged


def _make_options_engine(mocker, spread_width=None):
    config = SimpleNamespace(
        runtime=SimpleNamespace(
            orders=SimpleNamespace(
                minimum_credit=0.05,
                exchange="SMART",
                algo=SimpleNamespace(strategy="Adaptive", params=[]),
            ),
            option_chains=SimpleNamespace(expirations=4, strikes=10),
        ),
        strategies=SimpleNamespace(
            wheel=SimpleNamespace(
                defaults=SimpleNamespace(
                    roll_when=SimpleNamespace(
                        dte=7,
                        max_dte=None,
                        puts=SimpleNamespace(credit_only=False),
                        calls=SimpleNamespace(credit_only=False),
                    ),
                    target=SimpleNamespace(
                        maximum_new_contracts_percent=0.1,
                    ),
                    constants=SimpleNamespace(daily_stddev_window=30),
                ),
            ),
        ),
        portfolio=SimpleNamespace(
            symbols={"TQQQ": SimpleNamespace(primary_exchange="NASDAQ")},
        ),
        get_spread_width=lambda symbol, right: spread_width,
        close_if_unable_to_roll=lambda symbol: False,
        get_target_dte=lambda symbol: 30,
        get_target_delta=lambda symbol, right: 0.3,
        get_max_dte_for=lambda symbol: None,
        get_strike_limit=lambda symbol, right: None,
        maintain_high_water_mark=lambda symbol: False,
    )
    ibkr = mocker.Mock()
    ibkr.get_ticker_for_contract = AsyncMock()
    ibkr.portfolio = mocker.Mock(return_value=[])
    option_scanner = mocker.Mock()
    option_scanner.find_eligible_contracts = AsyncMock()
    order_ops = mocker.Mock()
    order_ops.create_limit_order = mocker.Mock(return_value="ORDER")
    order_ops.enqueue_order = mocker.Mock()
    order_ops.get_order_exchange = mocker.Mock(return_value="SMART")
    services = SimpleNamespace(
        get_symbols=lambda: ["TQQQ"],
        get_primary_exchange=lambda s: "NASDAQ",
        get_buying_power=lambda a: 10000,
        get_maximum_new_contracts_for=AsyncMock(return_value=10),
        get_write_threshold=AsyncMock(return_value=(1.0, 0.5)),
        get_close_price=lambda t: 100.0,
    )
    from thetagang.strategies.options_engine import OptionsStrategyEngine

    engine = OptionsStrategyEngine(
        config=cast(Config, config),
        ibkr=ibkr,
        option_scanner=option_scanner,
        order_ops=order_ops,
        services=services,
        target_quantities={},
        has_excess_puts=set(),
        has_excess_calls=set(),
        qualified_contracts={},
    )
    return engine, ibkr, order_ops, option_scanner


def test_get_spread_width_returns_none_by_default() -> None:
    config = Config(**_base_config())
    assert config.get_spread_width("TQQQ", "P") is None
    assert config.get_spread_width("TQQQ", "C") is None


def test_get_spread_width_returns_target_default() -> None:
    config = Config(
        **_base_config(
            strategies={
                "wheel": {
                    "defaults": {
                        "target": {
                            "dte": 30,
                            "minimum_open_interest": 5,
                            "spread_width": 5.0,
                        }
                    }
                }
            }
        )
    )
    assert config.get_spread_width("TQQQ", "P") == pytest.approx(5.0)
    assert config.get_spread_width("TQQQ", "C") == pytest.approx(5.0)


def test_get_spread_width_puts_override_takes_precedence() -> None:
    config = Config(
        **_base_config(
            strategies={
                "wheel": {
                    "defaults": {
                        "target": {
                            "dte": 30,
                            "minimum_open_interest": 5,
                            "spread_width": 5.0,
                            "puts": {"spread_width": 3.0},
                        }
                    }
                }
            }
        )
    )
    assert config.get_spread_width("TQQQ", "P") == pytest.approx(3.0)


def test_get_spread_width_calls_override_takes_precedence() -> None:
    config = Config(
        **_base_config(
            strategies={
                "wheel": {
                    "defaults": {
                        "target": {
                            "dte": 30,
                            "minimum_open_interest": 5,
                            "spread_width": 5.0,
                            "calls": {"spread_width": 2.5},
                        }
                    }
                }
            }
        )
    )
    assert config.get_spread_width("TQQQ", "C") == pytest.approx(2.5)


def test_get_spread_width_symbol_level_overrides_target() -> None:
    config = Config(
        **_base_config(
            portfolio={
                "symbols": {
                    "TQQQ": {
                        "weight": 1.0,
                        "puts": {"spread_width": 2.0},
                    }
                }
            },
            strategies={
                "wheel": {
                    "defaults": {
                        "target": {
                            "dte": 30,
                            "minimum_open_interest": 5,
                            "spread_width": 5.0,
                        }
                    }
                }
            },
        )
    )
    assert config.get_spread_width("TQQQ", "P") == pytest.approx(2.0)


@pytest.mark.asyncio
async def test_write_puts_no_spread_when_width_is_none(mocker) -> None:
    engine, _ibkr, order_ops, option_scanner = _make_options_engine(
        mocker, spread_width=None
    )
    sell_contract = Option("TQQQ", "20270321", 100.0, "P", "SMART", currency="USD")
    sell_contract.conId = 11111
    sell_ticker = SimpleNamespace(contract=sell_contract)
    option_scanner.find_eligible_contracts = AsyncMock(return_value=sell_ticker)
    mocker.patch(
        "thetagang.strategies.options_engine.get_higher_price", return_value=2.0
    )

    await engine.write_puts([("TQQQ", "NASDAQ", 1, None)])

    order_ops.enqueue_order.assert_called_once_with(sell_contract, "ORDER")
    order_ops.create_limit_order.assert_called_once_with(
        action="SELL", quantity=1, limit_price=2.0
    )


@pytest.mark.asyncio
async def test_write_puts_buys_protective_put_when_spread_configured(mocker) -> None:
    engine, ibkr, order_ops, option_scanner = _make_options_engine(
        mocker, spread_width=5.0
    )
    order_ops.create_limit_order = mocker.Mock(side_effect=["SELL_ORDER", "BUY_ORDER"])

    sell_contract = Option("TQQQ", "20270321", 100.0, "P", "SMART", currency="USD")
    sell_contract.conId = 11111
    sell_ticker = SimpleNamespace(contract=sell_contract)
    option_scanner.find_eligible_contracts = AsyncMock(return_value=sell_ticker)

    protective_contract = Option("TQQQ", "20270321", 95.0, "P", "SMART", currency="USD")
    protective_contract.conId = 22222
    protective_ticker = SimpleNamespace(contract=protective_contract)
    ibkr.get_ticker_for_contract = AsyncMock(return_value=protective_ticker)

    mocker.patch(
        "thetagang.strategies.options_engine.get_higher_price", return_value=2.0
    )
    mocker.patch(
        "thetagang.strategies.options_engine.get_lower_price", return_value=0.50
    )

    await engine.write_puts([("TQQQ", "NASDAQ", 1, None)])

    assert order_ops.enqueue_order.call_count == 2
    order_ops.enqueue_order.assert_any_call(sell_contract, "SELL_ORDER")
    order_ops.enqueue_order.assert_any_call(protective_contract, "BUY_ORDER")
    order_ops.create_limit_order.assert_any_call(
        action="SELL", quantity=1, limit_price=2.0
    )
    order_ops.create_limit_order.assert_any_call(
        action="BUY", quantity=1, limit_price=0.5
    )


@pytest.mark.asyncio
async def test_write_calls_buys_protective_call_when_spread_configured(mocker) -> None:
    engine, ibkr, order_ops, option_scanner = _make_options_engine(
        mocker, spread_width=5.0
    )
    order_ops.create_limit_order = mocker.Mock(side_effect=["SELL_ORDER", "BUY_ORDER"])

    sell_contract = Option("TQQQ", "20270321", 110.0, "C", "SMART", currency="USD")
    sell_contract.conId = 31111
    sell_ticker = SimpleNamespace(contract=sell_contract)
    option_scanner.find_eligible_contracts = AsyncMock(return_value=sell_ticker)

    protective_contract = Option(
        "TQQQ", "20270321", 115.0, "C", "SMART", currency="USD"
    )
    protective_contract.conId = 32222
    protective_ticker = SimpleNamespace(contract=protective_contract)
    ibkr.get_ticker_for_contract = AsyncMock(return_value=protective_ticker)

    mocker.patch(
        "thetagang.strategies.options_engine.get_higher_price", return_value=2.0
    )
    mocker.patch(
        "thetagang.strategies.options_engine.get_lower_price", return_value=0.50
    )

    await engine.write_calls([("TQQQ", "NASDAQ", 1, 0)])

    assert order_ops.enqueue_order.call_count == 2
    order_ops.enqueue_order.assert_any_call(sell_contract, "SELL_ORDER")
    order_ops.enqueue_order.assert_any_call(protective_contract, "BUY_ORDER")
    order_ops.create_limit_order.assert_any_call(
        action="SELL", quantity=1, limit_price=2.0
    )
    order_ops.create_limit_order.assert_any_call(
        action="BUY", quantity=1, limit_price=0.5
    )


@pytest.mark.asyncio
async def test_write_calls_no_spread_when_width_is_none(mocker) -> None:
    engine, _ibkr, order_ops, option_scanner = _make_options_engine(
        mocker, spread_width=None
    )
    sell_contract = Option("TQQQ", "20270321", 110.0, "C", "SMART", currency="USD")
    sell_contract.conId = 33333
    sell_ticker = SimpleNamespace(contract=sell_contract)
    option_scanner.find_eligible_contracts = AsyncMock(return_value=sell_ticker)
    mocker.patch(
        "thetagang.strategies.options_engine.get_higher_price", return_value=2.0
    )

    await engine.write_calls([("TQQQ", "NASDAQ", 1, 0)])

    order_ops.enqueue_order.assert_called_once_with(sell_contract, "ORDER")
    order_ops.create_limit_order.assert_called_once_with(
        action="SELL", quantity=1, limit_price=2.0
    )


@pytest.mark.asyncio
async def test_roll_positions_rolls_protective_legs_for_spread_position(mocker) -> None:
    engine, ibkr, order_ops, option_scanner = _make_options_engine(
        mocker, spread_width=5.0
    )
    order_ops.create_limit_order = mocker.Mock(
        side_effect=["ROLL_ORDER", "CLOSE_PROTECTIVE", "OPEN_PROTECTIVE"]
    )

    short_contract = Option("TQQQ", "20270321", 110.0, "C", "SMART", currency="USD")
    short_contract.conId = 40001
    long_contract = Option("TQQQ", "20270321", 115.0, "C", "SMART", currency="USD")
    long_contract.conId = 40002
    rolled_short_contract = Option(
        "TQQQ", "20270418", 115.0, "C", "SMART", currency="USD"
    )
    rolled_short_contract.conId = 40003
    rolled_long_contract = Option(
        "TQQQ", "20270418", 120.0, "C", "SMART", currency="USD"
    )
    rolled_long_contract.conId = 40004

    short_position = SimpleNamespace(
        contract=short_contract,
        position=-1,
        averageCost=100.0,
        account="DU123",
    )
    long_position = SimpleNamespace(contract=long_contract, position=1, averageCost=1.0)
    stock_position = SimpleNamespace(
        contract=Stock("TQQQ", "SMART", "USD"),
        position=100,
        averageCost=100.0,
    )

    buy_ticker = SimpleNamespace(contract=short_contract)
    sell_ticker = SimpleNamespace(contract=rolled_short_contract)
    protective_sell_ticker = SimpleNamespace(contract=rolled_long_contract)
    protective_buy_ticker = SimpleNamespace(contract=long_contract)

    ibkr.get_ticker_for_contract = AsyncMock(
        side_effect=[buy_ticker, protective_sell_ticker, protective_buy_ticker]
    )
    option_scanner.find_eligible_contracts = AsyncMock(return_value=sell_ticker)

    mocker.patch(
        "thetagang.strategies.options_engine.midpoint_or_market_price",
        side_effect=[3.0, 1.0],
    )
    mocker.patch(
        "thetagang.strategies.options_engine.get_higher_price", return_value=0.75
    )
    mocker.patch(
        "thetagang.strategies.options_engine.get_lower_price", return_value=0.20
    )

    await engine.roll_positions(
        [short_position],
        "C",
        {},
        {
            "TQQQ": [short_position, long_position, stock_position],
        },
    )

    assert order_ops.enqueue_order.call_count == 3
    combo_contract = order_ops.enqueue_order.call_args_list[0].args[0]
    assert combo_contract.secType == "BAG"
    order_ops.enqueue_order.assert_any_call(long_contract, "CLOSE_PROTECTIVE")
    order_ops.enqueue_order.assert_any_call(rolled_long_contract, "OPEN_PROTECTIVE")

    order_ops.create_limit_order.assert_any_call(
        action="BUY",
        quantity=1,
        limit_price=2.0,
        use_default_algo=False,
        transmit=True,
    )
    order_ops.create_limit_order.assert_any_call(
        action="SELL",
        quantity=1,
        limit_price=0.75,
        transmit=True,
    )
    order_ops.create_limit_order.assert_any_call(
        action="BUY",
        quantity=1,
        limit_price=0.2,
        transmit=True,
    )


@pytest.mark.asyncio
async def test_roll_positions_no_matching_existing_protective_only_rolls_combo(
    mocker,
) -> None:
    engine, ibkr, order_ops, option_scanner = _make_options_engine(
        mocker, spread_width=5.0
    )

    short_contract = Option("TQQQ", "20270321", 110.0, "C", "SMART", currency="USD")
    short_contract.conId = 50001
    rolled_short_contract = Option(
        "TQQQ", "20270418", 115.0, "C", "SMART", currency="USD"
    )
    rolled_short_contract.conId = 50002
    rolled_long_contract = Option(
        "TQQQ", "20270418", 120.0, "C", "SMART", currency="USD"
    )
    rolled_long_contract.conId = 50003

    short_position = SimpleNamespace(
        contract=short_contract,
        position=-1,
        averageCost=100.0,
        account="DU123",
    )
    stock_position = SimpleNamespace(
        contract=Stock("TQQQ", "SMART", "USD"),
        position=100,
        averageCost=100.0,
    )

    buy_ticker = SimpleNamespace(contract=short_contract)
    sell_ticker = SimpleNamespace(contract=rolled_short_contract)
    protective_sell_ticker = SimpleNamespace(contract=rolled_long_contract)

    ibkr.get_ticker_for_contract = AsyncMock(
        side_effect=[buy_ticker, protective_sell_ticker]
    )
    option_scanner.find_eligible_contracts = AsyncMock(return_value=sell_ticker)

    mocker.patch(
        "thetagang.strategies.options_engine.midpoint_or_market_price",
        side_effect=[2.5, 1.0],
    )

    await engine.roll_positions(
        [short_position],
        "C",
        {},
        {
            "TQQQ": [short_position, stock_position],
        },
    )

    order_ops.enqueue_order.assert_called_once()
    combo_contract = order_ops.enqueue_order.call_args.args[0]
    assert combo_contract.secType == "BAG"
    assert ibkr.get_ticker_for_contract.await_count == 2
