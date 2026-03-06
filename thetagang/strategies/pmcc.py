from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Protocol

from ib_async import AccountValue, PortfolioItem

from thetagang import log

AccountSummary = Dict[str, AccountValue]
PortfolioBySymbol = Dict[str, List[PortfolioItem]]


@dataclass
class PMCCStrategyDeps:
    enabled_stages: set[str]
    service: "PMCCStageService"


class PMCCStageService(Protocol):
    async def manage_leaps(
        self,
        account_summary: AccountSummary,
        portfolio_positions: PortfolioBySymbol,
    ) -> None: ...

    async def write_short_calls(
        self,
        account_summary: AccountSummary,
        portfolio_positions: PortfolioBySymbol,
    ) -> None: ...


async def run_pmcc_stages(
    deps: PMCCStrategyDeps,
    account_summary: AccountSummary,
    portfolio_positions: PortfolioBySymbol,
) -> None:
    if "pmcc_manage_leaps" in deps.enabled_stages:
        log.notice("PMCC: Managing LEAPS positions...")
        await deps.service.manage_leaps(account_summary, portfolio_positions)

    if "pmcc_write_short_calls" in deps.enabled_stages:
        log.notice("PMCC: Writing short calls against LEAPS...")
        await deps.service.write_short_calls(account_summary, portfolio_positions)
