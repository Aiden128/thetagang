from __future__ import annotations

import math
from typing import Dict, List

from ib_async import AccountValue, PortfolioItem
from ib_async.contract import ComboLeg, Contract, Option, Stock

from thetagang import log
from thetagang.config import Config
from thetagang.ibkr import IBKR, TickerField
from thetagang.options import option_dte
from thetagang.trading_operations import (
    NoValidContractsError,
    OptionChainScanner,
    OrderOperations,
)
from thetagang.util import midpoint_or_market_price, position_pnl


class PMCCEngine:
    def __init__(
        self,
        *,
        config: Config,
        ibkr: IBKR,
        option_scanner: OptionChainScanner,
        order_ops: OrderOperations,
    ) -> None:
        self.config = config
        self.ibkr = ibkr
        self.option_scanner = option_scanner
        self.order_ops = order_ops

    def _pmcc_symbols(self) -> List[str]:
        return list(self.config.strategies.pmcc.symbols)

    def _long_calls_for_symbol(
        self, symbol: str, portfolio_positions: Dict[str, List[PortfolioItem]]
    ) -> List[PortfolioItem]:
        return [
            p
            for p in portfolio_positions.get(symbol, [])
            if isinstance(p.contract, Option)
            and p.contract.right.startswith("C")
            and p.position > 0
        ]

    def _short_calls_for_symbol(
        self, symbol: str, portfolio_positions: Dict[str, List[PortfolioItem]]
    ) -> List[PortfolioItem]:
        return [
            p
            for p in portfolio_positions.get(symbol, [])
            if isinstance(p.contract, Option)
            and p.contract.right.startswith("C")
            and p.position < 0
        ]

    async def _find_leaps_contract(self, symbol: str):
        return await self.option_scanner.find_eligible_contracts(
            Stock(
                symbol,
                self.order_ops.get_order_exchange(),
                currency="USD",
                primaryExchange=self.config.portfolio.symbols[symbol].primary_exchange,
            ),
            "C",
            strike_limit=0,
            minimum_price=lambda: 0.0,
            target_dte=self.config.strategies.pmcc.leaps_target_dte,
            target_delta=1.0,
        )

    async def _find_short_call_contract(self, symbol: str, strike_limit: float):
        return await self.option_scanner.find_eligible_contracts(
            Stock(
                symbol,
                self.order_ops.get_order_exchange(),
                currency="USD",
                primaryExchange=self.config.portfolio.symbols[symbol].primary_exchange,
            ),
            "C",
            strike_limit=strike_limit,
            minimum_price=lambda: self.config.runtime.orders.minimum_credit,
            target_dte=self.config.strategies.pmcc.short_call_dte,
            target_delta=self.config.strategies.pmcc.short_call_delta,
        )

    async def manage_leaps(
        self,
        account_summary: Dict[str, AccountValue],
        portfolio_positions: Dict[str, List[PortfolioItem]],
    ) -> None:
        if not self.config.strategies.pmcc.enabled:
            log.warning("🛑 PMCC not enabled, skipping LEAPS management...")
            return

        for symbol in self._pmcc_symbols():
            if symbol not in self.config.portfolio.symbols:
                log.warning(
                    f"PMCC: {symbol} not configured in portfolio.symbols, skipping"
                )
                continue
            if not self.config.trading_is_allowed(symbol):
                continue

            try:
                long_calls = self._long_calls_for_symbol(symbol, portfolio_positions)
                max_positions = self.config.strategies.pmcc.max_positions

                if len(long_calls) < max_positions:
                    missing = max_positions - len(long_calls)
                    buy_ticker = await self._find_leaps_contract(symbol)
                    if (
                        not buy_ticker.contract
                        or not buy_ticker.modelGreeks
                        or buy_ticker.modelGreeks.delta is None
                    ):
                        raise RuntimeError(
                            f"PMCC: Missing contract greeks for LEAPS candidate for {symbol}"
                        )

                    contract_delta = abs(float(buy_ticker.modelGreeks.delta))
                    contract_dte = option_dte(
                        buy_ticker.contract.lastTradeDateOrContractMonth
                    )
                    if contract_delta < self.config.strategies.pmcc.leaps_delta:
                        raise RuntimeError(
                            f"PMCC: LEAPS delta {contract_delta:.3f} below threshold {self.config.strategies.pmcc.leaps_delta:.3f} for {symbol}"
                        )
                    if contract_dte < self.config.strategies.pmcc.leaps_target_dte:
                        raise RuntimeError(
                            f"PMCC: LEAPS DTE {contract_dte} below target {self.config.strategies.pmcc.leaps_target_dte} for {symbol}"
                        )

                    price = round(midpoint_or_market_price(buy_ticker), 2)
                    order = self.order_ops.create_limit_order(
                        action="BUY",
                        quantity=missing,
                        limit_price=price,
                        transmit=True,
                    )
                    log.notice(f"PMCC: Buying LEAPS for {symbol}...")
                    self.order_ops.enqueue_order(buy_ticker.contract, order)

                for position in long_calls:
                    dte = option_dte(position.contract.lastTradeDateOrContractMonth)
                    if dte > self.config.strategies.pmcc.leaps_roll_dte:
                        continue

                    old_contract = position.contract
                    old_contract.exchange = self.order_ops.get_order_exchange()
                    old_ticker = await self.ibkr.get_ticker_for_contract(
                        old_contract,
                        required_fields=[],
                        optional_fields=[
                            TickerField.MIDPOINT,
                            TickerField.MARKET_PRICE,
                        ],
                    )

                    new_ticker = await self._find_leaps_contract(symbol)
                    if (
                        not new_ticker.contract
                        or not new_ticker.modelGreeks
                        or new_ticker.modelGreeks.delta is None
                    ):
                        raise RuntimeError(
                            f"PMCC: Missing contract greeks for roll candidate for {symbol}"
                        )

                    new_delta = abs(float(new_ticker.modelGreeks.delta))
                    if new_delta < self.config.strategies.pmcc.leaps_delta:
                        raise RuntimeError(
                            f"PMCC: Roll LEAPS delta {new_delta:.3f} below threshold {self.config.strategies.pmcc.leaps_delta:.3f} for {symbol}"
                        )

                    qty_to_roll = math.floor(abs(position.position))
                    combo_legs = [
                        ComboLeg(
                            conId=old_contract.conId,
                            ratio=1,
                            exchange=self.order_ops.get_order_exchange(),
                            action="SELL",
                        ),
                        ComboLeg(
                            conId=new_ticker.contract.conId,
                            ratio=1,
                            exchange=self.order_ops.get_order_exchange(),
                            action="BUY",
                        ),
                    ]
                    combo = Contract(
                        secType="BAG",
                        symbol=symbol,
                        currency="USD",
                        exchange=self.order_ops.get_order_exchange(),
                        comboLegs=combo_legs,
                    )

                    net_price = round(
                        midpoint_or_market_price(new_ticker)
                        - midpoint_or_market_price(old_ticker),
                        2,
                    )
                    action = "BUY" if net_price >= 0 else "SELL"
                    order = self.order_ops.create_limit_order(
                        action=action,
                        quantity=qty_to_roll,
                        limit_price=abs(net_price),
                        use_default_algo=False,
                        transmit=True,
                    )
                    log.notice(f"PMCC: Rolling LEAPS for {symbol}...")
                    self.order_ops.enqueue_order(combo, order)
            except (RuntimeError, NoValidContractsError):
                log.error(
                    f"PMCC: Error occurred while managing LEAPS for {symbol}. Continuing anyway..."
                )
                continue

    async def write_short_calls(
        self,
        account_summary: Dict[str, AccountValue],
        portfolio_positions: Dict[str, List[PortfolioItem]],
    ) -> None:
        if not self.config.strategies.pmcc.enabled:
            log.warning("🛑 PMCC not enabled, skipping short-call writing...")
            return

        for symbol in self._pmcc_symbols():
            if symbol not in self.config.portfolio.symbols:
                log.warning(
                    f"PMCC: {symbol} not configured in portfolio.symbols, skipping"
                )
                continue
            if not self.config.trading_is_allowed(symbol):
                continue

            try:
                long_calls = self._long_calls_for_symbol(symbol, portfolio_positions)
                if not long_calls:
                    continue

                long_count = math.floor(sum(abs(p.position) for p in long_calls))
                short_calls = self._short_calls_for_symbol(symbol, portfolio_positions)
                short_count = math.floor(sum(abs(p.position) for p in short_calls))

                calls_to_write = max(
                    0,
                    min(long_count, self.config.strategies.pmcc.max_positions)
                    - short_count,
                )
                if calls_to_write > 0:
                    stock_ticker = await self.ibkr.get_ticker_for_stock(
                        symbol,
                        self.config.portfolio.symbols[symbol].primary_exchange,
                    )
                    strike_limit = math.ceil(stock_ticker.marketPrice())
                    sell_ticker = await self._find_short_call_contract(
                        symbol, strike_limit
                    )
                    if not sell_ticker.contract:
                        raise RuntimeError(
                            f"PMCC: Missing short call contract candidate for {symbol}"
                        )

                    price = round(midpoint_or_market_price(sell_ticker), 2)
                    order = self.order_ops.create_limit_order(
                        action="SELL",
                        quantity=calls_to_write,
                        limit_price=price,
                        transmit=True,
                    )
                    log.notice(f"PMCC: Writing short call for {symbol}...")
                    self.order_ops.enqueue_order(sell_ticker.contract, order)

                for short_call in short_calls:
                    dte = option_dte(short_call.contract.lastTradeDateOrContractMonth)
                    pnl = position_pnl(short_call)
                    should_roll = (
                        pnl >= self.config.strategies.pmcc.roll_short_call_pnl
                        or dte <= self.config.strategies.pmcc.roll_short_call_dte
                    )
                    if not should_roll:
                        continue

                    old_contract = short_call.contract
                    old_contract.exchange = self.order_ops.get_order_exchange()
                    buy_ticker = await self.ibkr.get_ticker_for_contract(
                        old_contract,
                        required_fields=[],
                        optional_fields=[
                            TickerField.MIDPOINT,
                            TickerField.MARKET_PRICE,
                        ],
                    )

                    stock_ticker = await self.ibkr.get_ticker_for_stock(
                        symbol,
                        self.config.portfolio.symbols[symbol].primary_exchange,
                    )
                    strike_limit = max(
                        math.ceil(stock_ticker.marketPrice()),
                        float(old_contract.strike),
                    )
                    sell_ticker = await self._find_short_call_contract(
                        symbol, strike_limit
                    )
                    if not sell_ticker.contract:
                        raise RuntimeError(
                            f"PMCC: Missing roll short-call contract candidate for {symbol}"
                        )

                    qty_to_roll = math.floor(abs(short_call.position))
                    combo_legs = [
                        ComboLeg(
                            conId=old_contract.conId,
                            ratio=1,
                            exchange=self.order_ops.get_order_exchange(),
                            action="BUY",
                        ),
                        ComboLeg(
                            conId=sell_ticker.contract.conId,
                            ratio=1,
                            exchange=self.order_ops.get_order_exchange(),
                            action="SELL",
                        ),
                    ]
                    combo = Contract(
                        secType="BAG",
                        symbol=symbol,
                        currency="USD",
                        exchange=self.order_ops.get_order_exchange(),
                        comboLegs=combo_legs,
                    )
                    price = round(
                        midpoint_or_market_price(buy_ticker)
                        - midpoint_or_market_price(sell_ticker),
                        2,
                    )
                    order = self.order_ops.create_limit_order(
                        action="BUY",
                        quantity=qty_to_roll,
                        limit_price=price,
                        use_default_algo=False,
                        transmit=True,
                    )
                    log.notice(f"PMCC: Rolling short call for {symbol}...")
                    self.order_ops.enqueue_order(combo, order)
            except (RuntimeError, NoValidContractsError):
                log.error(
                    f"PMCC: Error occurred while writing/rolling short calls for {symbol}. Continuing anyway..."
                )
                continue
