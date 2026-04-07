import logging
from typing import Any, Optional

import lighter

from examples.grid_strategy.auth import AuthTokenManager
from examples.grid_strategy.exchange import fetch_active_orders
from examples.grid_strategy.models import GridSlot, OrderLifecycle, RuntimeMonitor
from examples.grid_strategy.trace import now_iso_ms

LOGGER = logging.getLogger("smart_grid")


def record_order_lifecycle(
    monitor:            RuntimeMonitor,
    client_order_index: int,
    label:              str,
    event:              str,
    is_ask:             bool,
    reduce_only:        bool,
    slot:               Optional[GridSlot] = None,
    slot_kind:          str = "",
    tx_hash:            str = "",
    error:              str = "",
) -> None:
    prev = monitor.order_lifecycles.get(client_order_index)
    effective_slot_side = (
        "LONG" if slot is not None and slot.is_long else
        ("SHORT" if slot is not None else (prev.slot_side if prev is not None else ""))
    )
    effective_slot_kind  = slot_kind or (prev.slot_kind   if prev is not None else "")
    effective_entry_price = slot.place_price if slot is not None else (prev.entry_price if prev is not None else 0.0)
    effective_tp_price    = slot.tp_price    if slot is not None else (prev.tp_price    if prev is not None else 0.0)
    monitor.order_lifecycles[client_order_index] = OrderLifecycle(
        client_order_index=client_order_index,
        label=label,
        event=event,
        is_ask=is_ask,
        reduce_only=reduce_only,
        slot_side=effective_slot_side,
        slot_kind=effective_slot_kind,
        entry_price=effective_entry_price,
        tp_price=effective_tp_price,
        tx_hash=tx_hash,
        error=error,
    )
    LOGGER.info(
        "[coi] %s | %s | %s/%s | e=%.4f tp=%.4f | ask=%s ro=%s | tx=%s | err=%s | %s",
        client_order_index, event,
        effective_slot_side, effective_slot_kind,
        effective_entry_price, effective_tp_price,
        is_ask, reduce_only, tx_hash, error, label,
    )


async def do_place_order(
    monitor:        RuntimeMonitor,
    client:         lighter.SignerClient,
    market_id:      int,
    order_idx:      int,
    base_amount:    int,
    price_decimals: int,
    wire_price:     int,
    is_ask:         bool,
    reduce_only:    bool,
    dry_run:        bool,
    label:          str,
    slot:           Optional[GridSlot] = None,
    slot_kind:      str = "",
) -> bool:
    submit_time = now_iso_ms()
    LOGGER.info(
        "[order:req] label=%s coi=%s market=%s base_amount=%s price_wire=%s is_ask=%s reduce_only=%s",
        label, order_idx, market_id, base_amount, wire_price, is_ask, reduce_only,
    )
    record_order_lifecycle(monitor, order_idx, label, "request", is_ask, reduce_only, slot=slot, slot_kind=slot_kind)
    if dry_run:
        LOGGER.info("[order:dry-run] label=%s coi=%s", label, order_idx)
        record_order_lifecycle(monitor, order_idx, label, "dry-run", is_ask, reduce_only, slot=slot, slot_kind=slot_kind)
        return True
    _, tx_hash, err = await client.create_order(
        market_index=market_id,
        client_order_index=order_idx,
        base_amount=base_amount,
        price=wire_price,
        is_ask=is_ask,
        order_type=client.ORDER_TYPE_LIMIT,
        time_in_force=client.ORDER_TIME_IN_FORCE_GOOD_TILL_TIME,
        reduce_only=reduce_only,
        trigger_price=0,
    )
    if err is not None:
        LOGGER.error("[order:resp] label=%s coi=%s tx_hash=%s err=%s", label, order_idx, tx_hash, err)
        record_order_lifecycle(monitor, order_idx, label, "rejected", is_ask, reduce_only, slot=slot, slot_kind=slot_kind, tx_hash=str(tx_hash or ""), error=str(err))
        return False
    LOGGER.info("[order:resp] label=%s coi=%s tx_hash=%s err=None", label, order_idx, tx_hash)
    record_order_lifecycle(monitor, order_idx, label, "accepted", is_ask, reduce_only, slot=slot, slot_kind=slot_kind, tx_hash=str(tx_hash or ""))
    monitor.order_submit_times[order_idx] = submit_time
    return True


async def do_cancel_order(
    monitor:   RuntimeMonitor,
    client:    lighter.SignerClient,
    market_id: int,
    order_idx: int,
    dry_run:   bool,
    label:     str,
) -> None:
    if dry_run:
        LOGGER.info("[cancel:dry-run] label=%s coi=%s", label, order_idx)
        record_order_lifecycle(monitor, order_idx, label, "cancel-dry-run", False, False)
        monitor.order_submit_times.pop(order_idx, None)
        return
    LOGGER.info("[cancel:req] label=%s coi=%s market=%s", label, order_idx, market_id)
    existing = monitor.order_lifecycles.get(order_idx)
    record_order_lifecycle(
        monitor, order_idx, label, "cancel-request",
        existing.is_ask     if existing is not None else False,
        existing.reduce_only if existing is not None else False,
        slot_kind=existing.slot_kind if existing is not None else "",
    )
    _, tx_hash, err = await client.cancel_order(market_index=market_id, order_index=order_idx)
    if err is not None:
        LOGGER.warning("[cancel:resp] label=%s coi=%s tx_hash=%s err=%s", label, order_idx, tx_hash, err)
    else:
        LOGGER.info("[cancel:resp] label=%s coi=%s tx_hash=%s err=None", label, order_idx, tx_hash)
    record_order_lifecycle(
        monitor, order_idx, label,
        "cancel-confirmed" if err is None else "cancel-failed",
        existing.is_ask      if existing is not None else False,
        existing.reduce_only if existing is not None else False,
        slot_kind=existing.slot_kind if existing is not None else "",
        tx_hash=str(tx_hash or ""),
        error="" if err is None else str(err),
    )
    if err is None:
        monitor.order_submit_times.pop(order_idx, None)


async def cancel_all_active_orders_for_market(
    order_api:     lighter.OrderApi,
    client:        lighter.SignerClient,
    auth_mgr:      AuthTokenManager,
    account_index: int,
    market_id:     int,
    reason:        str,
    dry_run:       bool,
) -> int:
    auth_token = await auth_mgr.get()
    active_orders = await fetch_active_orders(order_api, account_index, market_id, auth_token)
    total = len(active_orders)
    LOGGER.info("[cleanup:%s] market_id=%s active_orders=%s", reason, market_id, total)
    if total == 0:
        return 0

    canceled = 0
    for order in active_orders:
        exchange_order_index = int(order.order_index)
        client_order_index   = int(order.client_order_index)
        if dry_run:
            LOGGER.info(
                "[cleanup:%s:dry-run] market_id=%s order_index=%s coi=%s",
                reason, market_id, exchange_order_index, client_order_index,
            )
            continue
        try:
            _, tx_hash, err = await client.cancel_order(
                market_index=market_id, order_index=exchange_order_index,
            )
            LOGGER.info(
                "[cleanup:%s] cancel market=%s order_index=%s coi=%s tx_hash=%s err=%s",
                reason, market_id, exchange_order_index, client_order_index, tx_hash, err,
            )
            if err is None:
                canceled += 1
        except Exception as exc:
            LOGGER.warning(
                "[cleanup:%s] cancel failed market=%s order_index=%s coi=%s reason=%s",
                reason, market_id, exchange_order_index, client_order_index, exc,
            )
    return canceled


async def do_market_add_position(
    monitor:        RuntimeMonitor,
    client:         lighter.SignerClient,
    market_id:      int,
    order_idx:      int,
    base_amount:    int,
    is_ask:         bool,
    dry_run:        bool,
    label:          str,
) -> bool:
    """
    Place a market order to add position.

    Args:
        monitor: RuntimeMonitor for tracking
        client: SignerClient
        market_id: Market ID
        order_idx: Client order index
        base_amount: Amount to add (in base asset)
        is_ask: True for short, False for long
        dry_run: If True, don't actually place the order
        label: Description label for logging

    Returns:
        True if order was successfully placed, False otherwise
    """
    submit_time = now_iso_ms()
    LOGGER.info(
        "[market-order:req] label=%s coi=%s market=%s base_amount=%s is_ask=%s",
        label, order_idx, market_id, base_amount, is_ask,
    )
    record_order_lifecycle(monitor, order_idx, label, "market-request", is_ask, False)

    if dry_run:
        LOGGER.info("[market-order:dry-run] label=%s coi=%s", label, order_idx)
        record_order_lifecycle(monitor, order_idx, label, "dry-run", is_ask, False)
        return True

    # Market orders still require a valid price threshold in this SDK/exchange path.
    # Use current best price as avg_execution_price to satisfy validation.
    best_price = await client.get_best_price(market_id, is_ask)
    avg_execution_price = max(1, int(best_price))
    _, tx_hash, err = await client.create_market_order(
        market_index=market_id,
        client_order_index=order_idx,
        base_amount=base_amount,
        avg_execution_price=avg_execution_price,
        is_ask=is_ask,
        reduce_only=False,
    )
    if err is not None:
        LOGGER.error("[market-order:resp] label=%s coi=%s tx_hash=%s err=%s", label, order_idx, tx_hash, err)
        record_order_lifecycle(monitor, order_idx, label, "rejected", is_ask, False, tx_hash=str(tx_hash or ""), error=str(err))
        return False

    LOGGER.info("[market-order:resp] label=%s coi=%s tx_hash=%s err=None", label, order_idx, tx_hash)
    record_order_lifecycle(monitor, order_idx, label, "accepted", is_ask, False, tx_hash=str(tx_hash or ""))
    monitor.order_submit_times[order_idx] = submit_time
    return True
