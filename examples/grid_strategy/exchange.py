import asyncio
import json
import logging
from typing import Any, List, Optional

import lighter
from lighter.exceptions import ApiException

from models import (
    AccountSnapshot, PositionSnapshot, RuntimeMonitor, TradeEvidence,
    format_position_snapshot, position_size_signed,
    RETRYABLE_HTTP_STATUS,
)

LOGGER = logging.getLogger("smart_grid")


def format_api_exception(exc: Exception) -> str:
    """Return a concise summary of an ApiException, parsing the JSON body when available."""
    status = getattr(exc, "status", None)
    body = getattr(exc, "body", None)
    if body:
        try:
            parsed = json.loads(body)
            code = parsed.get("code")
            message = parsed.get("message")
            if code is not None or message is not None:
                return f"http_status={status} code={code} message={message}"
        except Exception:
            pass
    reason = getattr(exc, "reason", None)
    if status is not None:
        return f"http_status={status} reason={reason}"
    return str(exc)


def is_rate_limited_exception(exc: Exception) -> bool:
    """Return True when the server explicitly responded with HTTP 429."""
    if isinstance(exc, ApiException):
        return getattr(exc, "status", None) == 429
    return False


def is_retryable_exception(exc: Exception) -> bool:
    if isinstance(exc, (TimeoutError, asyncio.TimeoutError)):
        return True
    if isinstance(exc, ApiException):
        status = getattr(exc, "status", None)
        if isinstance(status, int) and status in RETRYABLE_HTTP_STATUS:
            return True
    text = str(exc).lower()
    return (
        "gateway time-out" in text
        or "gateway timeout" in text
        or "timed out" in text
        or "timeout" in text
    )


async def fetch_market_detail(order_api: lighter.OrderApi, market_id: int) -> Any:
    max_attempts = 3
    for attempt in range(1, max_attempts + 1):
        try:
            resp = await order_api.order_book_details(market_id=market_id)
            if resp.order_book_details:
                return resp.order_book_details[0]
            if resp.spot_order_book_details:
                return resp.spot_order_book_details[0]
            raise RuntimeError(f"No market detail for market_id={market_id}")
        except RuntimeError:
            raise
        except Exception as exc:
            if not is_retryable_exception(exc) or attempt >= max_attempts:
                raise
            delay = 0.6 * attempt
            LOGGER.warning(
                "[market-detail:retry] market_id=%s attempt=%s/%s reason=%s sleep=%.1fs",
                market_id, attempt, max_attempts, format_api_exception(exc), delay,
            )
            await asyncio.sleep(delay)
    raise RuntimeError(f"fetch_market_detail exhausted retries for market_id={market_id}")


async def fetch_active_orders(
    order_api:     lighter.OrderApi,
    account_index: int,
    market_id:     int,
    auth_token:    str,
) -> List[Any]:
    max_attempts = 3
    for attempt in range(1, max_attempts + 1):
        try:
            resp = await order_api.account_active_orders(
                account_index=account_index,
                market_id=market_id,
                auth=auth_token,
            )
            return resp.orders or []
        except Exception as exc:
            if not is_retryable_exception(exc) or attempt >= max_attempts:
                raise
            delay = 0.6 * attempt
            LOGGER.warning(
                "[orders:retry] market_id=%s account=%s attempt=%s/%s reason=%s sleep=%.1fs",
                market_id, account_index, attempt, max_attempts, format_api_exception(exc), delay,
            )
            await asyncio.sleep(delay)
    return []


async def fetch_position_snapshot(
    account_api:   lighter.AccountApi,
    account_index: int,
    market_id:     int,
) -> Optional[PositionSnapshot]:
    resp = await account_api.account(by="index", value=str(account_index))
    accounts = resp.accounts or []
    if not accounts:
        return None

    for pos in accounts[0].positions or []:
        if int(pos.market_id) != market_id:
            continue
        return PositionSnapshot(
            market_id=int(pos.market_id),
            symbol=str(pos.symbol),
            sign=int(pos.sign),
            position=float(str(pos.position)),
            avg_entry_price=float(str(pos.avg_entry_price)),
            unrealized_pnl=float(str(pos.unrealized_pnl)),
            realized_pnl=float(str(pos.realized_pnl)),
            liquidation_price=float(str(getattr(pos, "liquidation_price", 0.0) or 0.0)),
            open_order_count=int(pos.open_order_count),
            pending_order_count=int(pos.pending_order_count),
        )

    return PositionSnapshot(
        market_id=market_id, symbol="", sign=0, position=0.0,
        avg_entry_price=0.0, unrealized_pnl=0.0, realized_pnl=0.0, liquidation_price=0.0,
        open_order_count=0, pending_order_count=0,
    )


async def fetch_account_snapshot(
    account_api: lighter.AccountApi,
    account_index: int,
) -> Optional[AccountSnapshot]:
    resp = await account_api.account(by="index", value=str(account_index))
    accounts = resp.accounts or []
    if not accounts:
        return None
    account = accounts[0]
    return AccountSnapshot(
        total_asset_value=float(str(getattr(account, "total_asset_value", 0.0) or 0.0)),
        available_balance=float(str(getattr(account, "available_balance", 0.0) or 0.0)),
    )


async def fetch_recent_trades(
    order_api:     lighter.OrderApi,
    account_index: int,
    market_id:     int,
    auth_token:    str,
    limit:         int = 20,
) -> List[Any]:
    max_attempts = 3
    for attempt in range(1, max_attempts + 1):
        try:
            resp = await order_api.trades(
                sort_by="timestamp",
                limit=limit,
                account_index=account_index,
                market_id=market_id,
                sort_dir="desc",
                auth=auth_token,
            )
            return resp.trades or []
        except Exception as exc:
            if not is_retryable_exception(exc) or attempt >= max_attempts:
                raise
            delay = 0.6 * attempt
            LOGGER.warning(
                "[trade:retry] market_id=%s account=%s attempt=%s/%s reason=%s sleep=%.1fs",
                market_id, account_index, attempt, max_attempts, format_api_exception(exc), delay,
            )
            await asyncio.sleep(delay)
    return []


def summarize_trade(trade: Any, account_index: int) -> str:
    ask_account_id = int(trade.ask_account_id)
    bid_account_id = int(trade.bid_account_id)
    if ask_account_id == account_index:
        side = "sell"
        client_order_index = int(trade.ask_client_id)
    elif bid_account_id == account_index:
        side = "buy"
        client_order_index = int(trade.bid_client_id)
    else:
        side = "unknown"
        client_order_index = 0
    return (
        f"trade_id={int(trade.trade_id)} side={side} client_order_index={client_order_index} "
        f"price={trade.price} size={trade.size} usd_amount={trade.usd_amount} tx_hash={trade.tx_hash}"
    )


async def initialize_runtime_monitor(
    monitor:       RuntimeMonitor,
    account_api:   lighter.AccountApi,
    order_api:     lighter.OrderApi,
    auth_mgr:      Any,
    account_index: int,
    market_id:     int,
) -> None:
    monitor.last_position = await fetch_position_snapshot(account_api, account_index, market_id)
    LOGGER.info("[position:init] %s", format_position_snapshot(monitor.last_position))

    auth_token = await auth_mgr.get()
    trades = await fetch_recent_trades(order_api, account_index, market_id, auth_token)
    monitor.seen_trade_ids = {int(t.trade_id) for t in trades}
    monitor.recent_trade_client_ids = {
        int(t.ask_client_id) for t in trades if int(t.ask_account_id) == account_index
    } | {
        int(t.bid_client_id) for t in trades if int(t.bid_account_id) == account_index
    }
    LOGGER.info("[trade:init] loaded recent trade baseline count=%s", len(monitor.seen_trade_ids))


async def collect_trade_evidence(
    monitor:       RuntimeMonitor,
    account_api:   lighter.AccountApi,
    order_api:     lighter.OrderApi,
    auth_mgr:      Any,
    account_index: int,
    market_id:     int,
) -> TradeEvidence:
    evidence = TradeEvidence(position_before=monitor.last_position)
    snapshot = await fetch_position_snapshot(account_api, account_index, market_id)
    evidence.position_after = snapshot
    def _position_key(s):
        if s is None:
            return None
        return (s.symbol, s.market_id, s.sign, s.position, s.avg_entry_price,
                s.open_order_count, s.pending_order_count)
    if _position_key(snapshot) != _position_key(monitor.last_position):
        LOGGER.info(
            "[position:change] before=(%s) after=(%s)",
            format_position_snapshot(monitor.last_position),
            format_position_snapshot(snapshot),
        )

    auth_token = await auth_mgr.get()
    trades = await fetch_recent_trades(order_api, account_index, market_id, auth_token)
    new_trades = [t for t in reversed(trades) if int(t.trade_id) not in monitor.seen_trade_ids]
    for trade in new_trades:
        trade_id = int(trade.trade_id)
        monitor.seen_trade_ids.add(trade_id)
        LOGGER.info("[trade:new] %s", summarize_trade(trade, account_index))
        if int(trade.ask_account_id) == account_index:
            evidence.new_trade_client_ids.add(int(trade.ask_client_id))
        if int(trade.bid_account_id) == account_index:
            evidence.new_trade_client_ids.add(int(trade.bid_client_id))

    if len(monitor.seen_trade_ids) > 500:
        monitor.seen_trade_ids = {int(t.trade_id) for t in trades[:200]}

    monitor.recent_trade_client_ids = {
        int(t.ask_client_id) for t in trades if int(t.ask_account_id) == account_index
    } | {
        int(t.bid_client_id) for t in trades if int(t.bid_account_id) == account_index
    }
    monitor.last_position = snapshot
    return evidence
