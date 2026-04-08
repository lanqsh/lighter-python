import asyncio
import datetime as _dt
import logging
from typing import Any, Dict, List, Optional

import lighter

from auth import AuthTokenManager
from config import normalize_side
from exchange import fetch_active_orders, collect_trade_evidence
from models import (
    GridSlot, GridState, GridConfig, RuntimeMonitor, TradeEvidence,
    SLOT_IDLE, SLOT_NEW, SLOT_FILLED, SIDE_LONG, SIDE_SHORT,
    position_size_signed,
)
from order_executor import do_place_order, do_cancel_order, record_order_lifecycle, do_market_add_position
from price_utils import (
    price_to_wire, size_to_wire,
    split_position_amounts, should_cancel_far_order,
)
from trace import now_iso_ms, append_filled_order_trace_record

LOGGER = logging.getLogger("smart_grid")


def _escape_bark_message(message: str) -> str:
    """Escape message for bark API."""
    _bark_escaping_map = {
        " ": "%20", '"': "%22", "#": "%23", "%": "%25", "&": "%26",
        "(": "%28", ")": "%29", "+": "%2B", ",": "%2C", "/": "%2F",
        ":": "%3A", ";": "%3B", "<": "%3C", "=": "%3D", ">": "%3E",
        "?": "%3F", "@": "%40", "\\": "%5C", "|": "%7C", "`": "\\`",
        "*": "\\*", "$": "\\$", "[": "%5B", "]": "%5D", "^": "%5E",
        "{": "%7B", "}": "%7D", "~": "%7E", "\n": "%0A",
    }
    return "".join(_bark_escaping_map.get(ch, ch) for ch in message)


def _send_bark_message_impl(bark_server: str, message: str) -> None:
    """Send message to bark server."""
    import urllib.request
    if not bark_server:
        return
    endpoint = bark_server.rstrip("/") + "/"
    ring = "?level=critical&volume=1"
    url = endpoint + _escape_bark_message(message) + ring
    try:
        with urllib.request.urlopen(url, timeout=10):
            pass
    except Exception as e:
        LOGGER.warning("[bark] send failed: %s", e)


def get_order_base_amount(order: Any) -> int:
    for field_name in ("initial_base_amount", "base_amount", "amount", "size"):
        value = getattr(order, field_name, None)
        if value is None:
            continue
        try:
            return int(value)
        except (TypeError, ValueError):
            continue
    return 0


def should_refill_tp_for_slot(
    slot: GridSlot,
    current_price: float,
    cfg: GridConfig,
    allow_any_distance: bool = False,
) -> bool:
    if slot.status != SLOT_FILLED or slot.tp_order_idx != 0:
        return False
    if allow_any_distance:
        return True
    min_distance = max(0, cfg.tp_refill_min_steps) * cfg.price_step
    far_cancel_distance = cfg.price_step * cfg.levels * 2
    auto_max_distance = max(0.0, far_cancel_distance - cfg.price_step)
    if cfg.tp_refill_max_steps > 0:
        max_distance = cfg.tp_refill_max_steps * cfg.price_step
    else:
        max_distance = auto_max_distance

    distance = abs(slot.tp_price - current_price)
    if distance < min_distance:
        return False
    if max_distance > 0 and distance > max_distance:
        return False
    return True


def count_side_tp_orders(state: GridState, side: str) -> int:
    slots = state.long_slots.values() if side == SIDE_LONG else state.short_slots.values()
    return sum(1 for slot in slots if slot.status == SLOT_FILLED and slot.tp_order_idx > 0)


async def ensure_tp_capacity_for_new_order(
    monitor: RuntimeMonitor,
    client: lighter.SignerClient,
    state: GridState,
    cfg: GridConfig,
    side: str,
    active_set: Dict[int, Any],
    current_price: float,
    desired_tp_price: float,
    reason: str,
) -> bool:
    current_tp_count = count_side_tp_orders(state, side)
    if current_tp_count < cfg.levels:
        return True

    slots = state.long_slots.values() if side == SIDE_LONG else state.short_slots.values()
    farthest_slot: Optional[GridSlot] = None
    farthest_distance = -1.0
    for slot in slots:
        if slot.status != SLOT_FILLED or slot.tp_order_idx <= 0:
            continue
        if slot.tp_order_idx not in active_set:
            continue
        distance = abs(slot.tp_price - current_price)
        if distance > farthest_distance:
            farthest_slot = slot
            farthest_distance = distance

    if farthest_slot is None:
        LOGGER.warning(
            "[tp:replace-cap-failed] side=%s reason=no-active-tp levels=%s desired_tp=%.4f why=%s",
            side,
            cfg.levels,
            desired_tp_price,
            reason,
        )
        return False

    desired_distance = abs(desired_tp_price - current_price)
    if farthest_distance <= desired_distance:
        LOGGER.info(
            "[tp:replace-cap-skip] side=%s reason=new-not-closer levels=%s current_tp=%s desired_tp=%.4f desired_dist=%.4f far_tp=%.4f far_dist=%.4f why=%s",
            side,
            cfg.levels,
            current_tp_count,
            desired_tp_price,
            desired_distance,
            farthest_slot.tp_price,
            farthest_distance,
            reason,
        )
        return False

    replaced_idx = farthest_slot.tp_order_idx
    await do_cancel_order(
        monitor,
        client,
        cfg.market_id,
        replaced_idx,
        cfg.dry_run,
        f"{'LONG' if farthest_slot.is_long else 'SHORT'} TP(replace-far) @{farthest_slot.tp_price:.4f}",
    )
    cancel_lifecycle = monitor.order_lifecycles.get(replaced_idx)
    cancel_ok = cfg.dry_run or (cancel_lifecycle is not None and cancel_lifecycle.event == "cancel-confirmed")
    if not cancel_ok:
        LOGGER.warning(
            "[tp:replace-cap-failed] side=%s reason=cancel-not-confirmed replace_coi=%s replace_tp=%.4f desired_tp=%.4f why=%s",
            side,
            replaced_idx,
            farthest_slot.tp_price,
            desired_tp_price,
            reason,
        )
        return False

    farthest_slot.tp_order_idx = 0
    farthest_slot.tp_base_amount = 0
    active_set.pop(replaced_idx, None)
    LOGGER.info(
        "[tp:replace-cap] side=%s levels=%s old_tp=%.4f old_coi=%s new_tp=%.4f why=%s",
        side,
        cfg.levels,
        farthest_slot.tp_price,
        replaced_idx,
        desired_tp_price,
        reason,
    )
    return True


def summarize_active_slots(
    state:      GridState,
    side:       str,
    active_set: Dict[int, Any],
    max_items:  int = 12,
) -> str:
    slots = state.long_slots.values() if side == SIDE_LONG else state.short_slots.values()
    rows: List = []
    for slot in slots:
        if slot.status == SLOT_NEW and slot.place_order_idx in active_set:
            rows.append((slot.place_price, f"ENTRY#{slot.place_order_idx}@{slot.place_price:.2f}->tp{slot.tp_price:.2f}"))
        elif slot.status == SLOT_FILLED and slot.tp_order_idx in active_set:
            rows.append((slot.tp_price, f"TP#{slot.tp_order_idx}@{slot.tp_price:.2f}(entry{slot.place_price:.2f})"))
    rows.sort(key=lambda x: x[0])
    if not rows:
        return "none"
    body = " | ".join(text for _, text in rows[:max_items])
    more = "" if len(rows) <= max_items else f" | ...(+{len(rows) - max_items})"
    return body + more


def evidence_confirms_entry_fill(slot: GridSlot, evidence: TradeEvidence) -> bool:
    if slot.place_order_idx in evidence.new_trade_client_ids:
        return True
    before = position_size_signed(evidence.position_before)
    after  = position_size_signed(evidence.position_after)
    delta  = after - before
    return delta > 0 if slot.is_long else delta < 0


def evidence_confirms_tp_fill(slot: GridSlot, evidence: TradeEvidence) -> bool:
    if slot.tp_order_idx in evidence.new_trade_client_ids:
        return True
    before = position_size_signed(evidence.position_before)
    after  = position_size_signed(evidence.position_after)
    delta  = after - before
    return delta < 0 if slot.is_long else delta > 0


async def seed_startup_position_take_profits(
    monitor:         RuntimeMonitor,
    client:          lighter.SignerClient,
    state:           GridState,
    cfg:             GridConfig,
    current_price:   float,
    price_decimals:  int,
    size_decimals:   int,
    base_amount:     int,
    min_base_amount: float,
) -> int:
    snapshot = monitor.last_position
    signed_position = position_size_signed(snapshot)
    if snapshot is None or signed_position == 0:
        LOGGER.info("[startup:position-seed] no existing position to seed")
        return 0

    if cfg.side == SIDE_LONG and signed_position <= 0:
        LOGGER.info("[startup:position-seed] existing position is not long, skip side=%s position=%s", cfg.side, signed_position)
        return 0
    if cfg.side == SIDE_SHORT and signed_position >= 0:
        LOGGER.info("[startup:position-seed] existing position is not short, skip side=%s position=%s", cfg.side, signed_position)
        return 0

    total_position_wire = size_to_wire(abs(signed_position), size_decimals)
    min_base_wire = max(1, size_to_wire(min_base_amount, size_decimals))
    tp_amounts = split_position_amounts(total_position_wire, base_amount, min_base_wire)
    if not tp_amounts:
        LOGGER.info(
            "[startup:position-seed] position exists but no valid tp chunks side=%s position=%s total_wire=%s",
            cfg.side, signed_position, total_position_wire,
        )
        return 0

    aligned = (int(current_price / cfg.price_step)) * cfg.price_step
    seeded_count = 0
    LOGGER.info(
        "[startup:position-seed] side=%s signed_position=%s total_wire=%s tp_chunks=%s aligned=%.4f avg_entry=%.4f",
        cfg.side, signed_position, total_position_wire, tp_amounts, aligned,
        snapshot.avg_entry_price if snapshot is not None else 0.0,
    )

    # Keep startup-seeded TP away from the nearest grid TP level to avoid
    # colliding with TP orders that will be created by fresh place fills.
    startup_tp_offset_steps = 2

    for idx, tp_amount in enumerate(tp_amounts, start=1):
        if seeded_count >= cfg.levels:
            LOGGER.info(
                "[startup:position-seed] reached tp cap levels=%s, stop seeding more tp orders",
                cfg.levels,
            )
            break
        is_long = cfg.side == SIDE_LONG
        tp_steps = idx + startup_tp_offset_steps - 1
        tp_price    = aligned + cfg.price_step * tp_steps if is_long else aligned - cfg.price_step * tp_steps
        place_price = tp_price - cfg.price_step       if is_long else tp_price + cfg.price_step
        synthetic_place_idx = state.alloc_idx()
        tp_idx = state.alloc_idx()
        slot = GridSlot(
            place_price=place_price,
            tp_price=tp_price,
            is_long=is_long,
            status=SLOT_FILLED,
            place_order_idx=synthetic_place_idx,
            place_base_amount=tp_amount,
        )
        record_order_lifecycle(
            monitor, synthetic_place_idx,
            f"{'LONG' if is_long else 'SHORT'} startup entry @{place_price:.4f}",
            "startup-position-seeded",
            not is_long, False, slot=slot, slot_kind="entry",
        )
        ok = await do_place_order(
            monitor=monitor, client=client, market_id=cfg.market_id,
            order_idx=tp_idx, base_amount=tp_amount, price_decimals=price_decimals,
            wire_price=price_to_wire(tp_price, price_decimals),
            is_ask=is_long, reduce_only=True, dry_run=cfg.dry_run,
            label=f"{'LONG' if is_long else 'SHORT'} startup TP @{tp_price:.4f}",
            slot=slot, slot_kind="tp",
        )
        if not ok:
            LOGGER.error(
                "[startup:position-seed] failed to place tp side=%s tp_price=%.4f amount=%s linked_place=%s",
                cfg.side, tp_price, tp_amount, synthetic_place_idx,
            )
            slot.tp_order_idx = 0
            slot.tp_base_amount = 0
            slot_map = state.long_slots if is_long else state.short_slots
            slot_map[GridState.price_key(place_price)] = slot
            continue
        slot.tp_order_idx = tp_idx
        slot.tp_base_amount = tp_amount
        slot_map = state.long_slots if is_long else state.short_slots
        slot_map[GridState.price_key(place_price)] = slot
        seeded_count += 1

    LOGGER.info("[startup:position-seed] seeded_tp_orders=%s side=%s", seeded_count, cfg.side)
    return seeded_count


async def check_and_add_position(
    monitor:     RuntimeMonitor,
    client:      lighter.SignerClient,
    state:       GridState,
    cfg:         GridConfig,
    evidence:    TradeEvidence,
    side:        str,
    base_amount: int,
    bark_server: str,
    size_decimals: int,
) -> bool:
    """
    Check if current position is below threshold. If so, add N-grid total amount via market order.

    Trigger threshold = (levels + 1) * base_amount
    Per top-up amount = levels * base_amount
    Transaction direction:
    - For LONG: use is_ask=False (BUY)
    - For SHORT: use is_ask=True (SELL)

    Returns:
        True if position was added, False otherwise
    """
    import time

    base_amount_float = base_amount / (10 ** size_decimals)
    grid_total_amount = cfg.levels * base_amount_float
    trigger_threshold_amount = grid_total_amount + base_amount_float
    current_position = position_size_signed(evidence.position_after)

    # For LONG side: we want current_position to be >= target position (positive)
    # For SHORT side: we want current_position to be <= -target position (negative)
    if side == SIDE_LONG:
        if current_position >= trigger_threshold_amount:
            LOGGER.info(
                "[add-position] skip LONG current=%.6f threshold=%.6f (levels=%s base_amount=%s)",
                current_position, trigger_threshold_amount, cfg.levels, base_amount,
            )
            return False
        add_amount_float = grid_total_amount
    else:  # SIDE_SHORT
        if current_position <= -trigger_threshold_amount:
            LOGGER.info(
                "[add-position] skip SHORT current=%.6f threshold=-%.6f (levels=%s base_amount=%s)",
                current_position, trigger_threshold_amount, cfg.levels, base_amount,
            )
            return False
        add_amount_float = grid_total_amount

    # Ensure add_amount is positive and at least one wire unit
    add_amount = size_to_wire(abs(add_amount_float), size_decimals)
    if add_amount <= 0:
        return False

    # Check 5-minute interval
    now = time.time()
    if now - monitor.last_add_position_time < 300:
        time_since_last = now - monitor.last_add_position_time
        LOGGER.info(
            "[add-position] throttled by interval check side=%s last_add=%.1fs ago (need >300s)",
            side, time_since_last,
        )
        return False

    order_idx = state.alloc_idx()
    is_ask = side == SIDE_SHORT  # For LONG, is_ask=False; for SHORT, is_ask=True
    label = f"{side.upper()} market-add-position threshold={trigger_threshold_amount:.6f} add={add_amount_float:.6f}"

    LOGGER.info(
        "[add-position:execute] side=%s current=%.6f threshold=%.6f add_amount=%.6f(%s wire) coi=%s",
        side, current_position, trigger_threshold_amount, abs(add_amount_float), add_amount, order_idx,
    )

    ok = await do_market_add_position(
        monitor=monitor, client=client, market_id=cfg.market_id,
        order_idx=order_idx, base_amount=add_amount,
        is_ask=is_ask, dry_run=cfg.dry_run, label=label,
    )

    if ok:
        monitor.last_add_position_time = now
        LOGGER.info(
            "[add-position:success] side=%s threshold=%.6f added=%.6f(%s wire) coi=%s",
            side, trigger_threshold_amount, abs(add_amount_float), add_amount, order_idx,
        )
        # Send bark message
        message = (
            f"[lighter] Market Add Position\n"
            f"side={side.upper()}\n"
            f"current_position={current_position:.4f}\n"
            f"threshold_position={trigger_threshold_amount:.4f}\n"
            f"added_amount={abs(add_amount_float):.4f}\n"
            f"symbol={cfg.market_symbol}\n"
            f"leverage={cfg.leverage}x"
        )
        if bark_server:
            await asyncio.to_thread(_send_bark_message_impl, bark_server, message)
        return True
    else:
        LOGGER.error(
            "[add-position:failed] side=%s threshold=%.6f failed_add=%s coi=%s",
            side, trigger_threshold_amount, add_amount, order_idx,
        )
        return False


async def run_one_cycle(
    monitor:        RuntimeMonitor,
    account_api:    lighter.AccountApi,
    client:         lighter.SignerClient,
    order_api:      lighter.OrderApi,
    state:          GridState,
    cfg:            GridConfig,
    current_price:  float,
    price_decimals: int,
    base_amount:    int,
    account_index:  int,
    auth_mgr:       AuthTokenManager,
    size_decimals:  int,
    bark_server:    str = "",
) -> None:
    side = normalize_side(cfg.side)

    auth_token    = await auth_mgr.get()
    active_orders = await fetch_active_orders(order_api, account_index, cfg.market_id, auth_token)
    active_set: Dict[int, Any] = {int(o.client_order_index): o for o in active_orders}
    LOGGER.info("[orders:active] count=%s market_id=%s side=%s", len(active_orders), cfg.market_id, side)
    LOGGER.info("[slots:active] %s", summarize_active_slots(state, side, active_set))

    evidence = await collect_trade_evidence(
        monitor=monitor, account_api=account_api, order_api=order_api,
        auth_mgr=auth_mgr, account_index=account_index, market_id=cfg.market_id,
    )
    for active_order in active_orders:
        active_coi = int(active_order.client_order_index)
        lifecycle  = monitor.order_lifecycles.get(active_coi)
        record_order_lifecycle(
            monitor, active_coi,
            lifecycle.label if lifecycle is not None else f"exchange-order-{active_coi}",
            f"active:{active_order.status}",
            bool(active_order.is_ask), bool(active_order.reduce_only),
            slot_kind=lifecycle.slot_kind if lifecycle is not None else "",
        )

    # ── Detect manual full-close: position=0 but FILLED slots remain ────────
    # If the exchange position is 0 but we still track FILLED slots, the user
    # manually closed all positions externally.  Cancel any lingering orders and
    # reset every slot to IDLE so the next cycle starts fresh.
    _current_position = position_size_signed(evidence.position_after)
    _active_slots_now = state.long_slots.values() if side == SIDE_LONG else state.short_slots.values()
    _has_filled_slots = any(s.status == SLOT_FILLED for s in _active_slots_now)
    if _current_position == 0.0 and _has_filled_slots:
        LOGGER.warning(
            "[manual-close] position=0 but FILLED slots exist — resetting all slots to IDLE "
            "and cancelling lingering orders (side=%s)",
            side,
        )
        for slot in list(_active_slots_now):
            # Cancel any remaining entry orders
            if slot.status == SLOT_NEW and slot.place_order_idx in active_set:
                await do_cancel_order(
                    monitor, client, cfg.market_id, slot.place_order_idx, cfg.dry_run,
                    f"{'LONG' if slot.is_long else 'SHORT'} entry(manual-close-reset) @{slot.place_price:.4f}",
                )
            # Cancel any remaining TP orders
            if slot.status == SLOT_FILLED and slot.tp_order_idx > 0 and slot.tp_order_idx in active_set:
                await do_cancel_order(
                    monitor, client, cfg.market_id, slot.tp_order_idx, cfg.dry_run,
                    f"{'LONG' if slot.is_long else 'SHORT'} TP(manual-close-reset) @{slot.tp_price:.4f}",
                )
            # Reset slot to IDLE
            slot.status = SLOT_IDLE
            slot.place_order_idx = 0
            slot.place_base_amount = 0
            slot.tp_order_idx = 0
            slot.tp_base_amount = 0
        LOGGER.info("[manual-close] all slots reset to IDLE, skipping this cycle")
        return

    # ── Check and add position if needed ──────────────────────────────────
    await check_and_add_position(
        monitor=monitor, client=client, state=state, cfg=cfg,
        evidence=evidence, side=side, base_amount=base_amount, bark_server=bark_server, size_decimals=size_decimals,
    )

    aligned       = (int(current_price / cfg.price_step)) * cfg.price_step
    far_threshold = cfg.price_step * cfg.levels * 2

    # ── Cancel orders that have drifted too far from current price ──────────
    active_slots = state.long_slots.values() if side == SIDE_LONG else state.short_slots.values()
    for slot in list(active_slots):
        should_cancel, order_kind, cancel_order_idx, cancel_price = should_cancel_far_order(
            slot, side, aligned, far_threshold, cfg.price_step, cfg.levels
        )
        if not should_cancel:
            continue
        if order_kind == "entry" and cancel_order_idx in active_set:
            active_order = active_set[cancel_order_idx]
            active_order_amount = get_order_base_amount(active_order)
            if slot.place_base_amount > 0 and active_order_amount != slot.place_base_amount:
                LOGGER.info(
                    "[cancel:skip-size-mismatch] kind=entry coi=%s expected=%s actual=%s",
                    cancel_order_idx,
                    slot.place_base_amount,
                    active_order_amount,
                )
                continue
            await do_cancel_order(
                monitor, client, cfg.market_id, cancel_order_idx, cfg.dry_run,
                f"{'LONG' if slot.is_long else 'SHORT'} entry(far) @{cancel_price:.4f}",
            )
            slot.status = SLOT_IDLE
            slot.place_order_idx = 0
            slot.place_base_amount = 0
            continue
        if order_kind == "tp" and cancel_order_idx in active_set:
            active_order = active_set[cancel_order_idx]
            active_order_amount = get_order_base_amount(active_order)
            if slot.tp_base_amount > 0 and active_order_amount != slot.tp_base_amount:
                LOGGER.info(
                    "[cancel:skip-size-mismatch] kind=tp coi=%s expected=%s actual=%s",
                    cancel_order_idx,
                    slot.tp_base_amount,
                    active_order_amount,
                )
                continue
            await do_cancel_order(
                monitor, client, cfg.market_id, cancel_order_idx, cfg.dry_run,
                f"{'LONG' if slot.is_long else 'SHORT'} TP(far) @{cancel_price:.4f}",
            )
            slot.tp_order_idx = 0
            slot.tp_base_amount = 0

    # ── Detect entry fills ───────────────────────────────────────────────────
    all_slots = list(active_slots)
    for slot in all_slots:
        if slot.status != SLOT_NEW:
            continue
        if slot.place_order_idx <= 0:
            continue
        if slot.place_order_idx in active_set:
            continue
        LOGGER.info(
            "[fill:candidate] entry_order_disappeared side=%s entry_price=%.4f coi=%s",
            "LONG" if slot.is_long else "SHORT", slot.place_price, slot.place_order_idx,
        )
        record_order_lifecycle(
            monitor, slot.place_order_idx,
            f"{'LONG' if slot.is_long else 'SHORT'} entry @{slot.place_price:.4f}",
            "disappeared-from-active", not slot.is_long, False, slot=slot, slot_kind="entry",
        )
        if not evidence_confirms_entry_fill(slot, evidence):
            LOGGER.error(
                "[fill:rejected] side=%s entry_price=%.4f coi=%s reason=no trade/position evidence",
                "LONG" if slot.is_long else "SHORT", slot.place_price, slot.place_order_idx,
            )
            record_order_lifecycle(
                monitor, slot.place_order_idx,
                f"{'LONG' if slot.is_long else 'SHORT'} entry @{slot.place_price:.4f}",
                "disappeared-without-fill-evidence", not slot.is_long, False,
                slot=slot, slot_kind="entry",
            )
            slot.status = SLOT_IDLE
            continue
        record_order_lifecycle(
            monitor, slot.place_order_idx,
            f"{'LONG' if slot.is_long else 'SHORT'} entry @{slot.place_price:.4f}",
            "fill-confirmed", not slot.is_long, False, slot=slot, slot_kind="entry",
        )
        append_filled_order_trace_record(
            market_id=cfg.market_id, order_kind="entry",
            label=f"{'LONG' if slot.is_long else 'SHORT'} entry @{slot.place_price:.4f}",
            client_order_index=slot.place_order_idx, linked_place_order_index=0,
            price_wire=price_to_wire(slot.place_price, price_decimals),
            price_decimals=price_decimals, base_amount=base_amount,
            is_ask=not slot.is_long, reduce_only=False,
            slot=slot, monitor=monitor,
            place_time=monitor.order_submit_times.get(slot.place_order_idx, ""),
            fill_time=now_iso_ms(),
        )
        monitor.order_submit_times.pop(slot.place_order_idx, None)

        tp_idx  = state.alloc_idx()
        tp_wire = price_to_wire(slot.tp_price, price_decimals)
        label   = f"{'LONG' if slot.is_long else 'SHORT'} TP @{slot.tp_price:.4f}"
        if not await ensure_tp_capacity_for_new_order(
            monitor=monitor,
            client=client,
            state=state,
            cfg=cfg,
            side=side,
            active_set=active_set,
            current_price=current_price,
            desired_tp_price=slot.tp_price,
            reason=f"entry-fill@{slot.place_price:.4f}",
        ):
            slot.tp_order_idx = 0
            slot.status = SLOT_FILLED
            continue
        ok = await do_place_order(
            monitor, client, cfg.market_id, tp_idx, base_amount, price_decimals, tp_wire,
            is_ask=slot.is_long, reduce_only=True, dry_run=cfg.dry_run,
            label=label, slot=slot, slot_kind="tp",
        )
        if ok:
            slot.tp_order_idx = tp_idx
            slot.tp_base_amount = base_amount
            slot.status       = SLOT_FILLED
        else:
            LOGGER.error(
                "[tp:place-failed] side=%s entry=%.4f tp=%.4f coi=%s",
                "LONG" if slot.is_long else "SHORT",
                slot.place_price,
                slot.tp_price,
                tp_idx,
            )
            slot.tp_order_idx = 0
            slot.tp_base_amount = 0
            slot.status = SLOT_FILLED

    # ── Detect TP fills ──────────────────────────────────────────────────────
    # Track TP prices that fired this cycle to avoid placing a new entry at the
    # exact same price in the same cycle (which would immediately re-open the
    # position that was just closed).
    filled_tp_prices: set = set()
    all_slots = list(active_slots)
    for slot in all_slots:
        if slot.status != SLOT_FILLED:
            continue
        if slot.tp_order_idx <= 0:
            continue
        if slot.tp_order_idx in active_set:
            continue
        record_order_lifecycle(
            monitor, slot.tp_order_idx,
            f"{'LONG' if slot.is_long else 'SHORT'} TP @{slot.tp_price:.4f}",
            "disappeared-from-active", slot.is_long, True, slot=slot, slot_kind="tp",
        )
        if not evidence_confirms_tp_fill(slot, evidence):
            current_position = position_size_signed(evidence.position_after)
            if current_position == 0.0:
                LOGGER.warning(
                    "[tp:no-evidence-but-zero-position] side=%s tp_price=%.4f coi=%s "
                    "position=0 → resetting slot to IDLE (not counted as successful TP)",
                    "LONG" if slot.is_long else "SHORT", slot.tp_price, slot.tp_order_idx,
                )
                record_order_lifecycle(
                    monitor, slot.tp_order_idx,
                    f"{'LONG' if slot.is_long else 'SHORT'} TP @{slot.tp_price:.4f}",
                    "reset-idle-zero-position", slot.is_long, True, slot=slot, slot_kind="tp",
                )
                slot.status = SLOT_IDLE
                filled_tp_prices.add(slot.tp_price)
                continue
            LOGGER.error(
                "[tp:rejected] side=%s tp_price=%.4f coi=%s reason=no trade/position evidence",
                "LONG" if slot.is_long else "SHORT", slot.tp_price, slot.tp_order_idx,
            )
            record_order_lifecycle(
                monitor, slot.tp_order_idx,
                f"{'LONG' if slot.is_long else 'SHORT'} TP @{slot.tp_price:.4f}",
                "disappeared-without-fill-evidence", slot.is_long, True, slot=slot, slot_kind="tp",
            )
            continue

        slot.status = SLOT_IDLE
        filled_tp_prices.add(slot.tp_price)
        state.success_count += 1
        _today = _dt.date.today().isoformat()
        if state.today_tp_date != _today:
            state.today_tp_count = 0
            state.today_tp_date  = _today
        state.today_tp_count += 1

        record_order_lifecycle(
            monitor, slot.tp_order_idx,
            f"{'LONG' if slot.is_long else 'SHORT'} TP @{slot.tp_price:.4f}",
            "fill-confirmed", slot.is_long, True, slot=slot, slot_kind="tp",
        )
        append_filled_order_trace_record(
            market_id=cfg.market_id, order_kind="tp",
            label=f"{'LONG' if slot.is_long else 'SHORT'} TP @{slot.tp_price:.4f}",
            client_order_index=slot.tp_order_idx,
            linked_place_order_index=slot.place_order_idx,
            price_wire=price_to_wire(slot.tp_price, price_decimals),
            price_decimals=price_decimals, base_amount=base_amount,
            is_ask=slot.is_long, reduce_only=True,
            slot=slot, monitor=monitor,
            place_time=monitor.order_submit_times.get(slot.tp_order_idx, ""),
            fill_time=now_iso_ms(),
        )
        monitor.order_submit_times.pop(slot.tp_order_idx, None)
        LOGGER.info(
            "[trade:slot-closed] side=%s total_tp=%s today_tp=%s(%s) entry=%.4f tp=%.4f",
            "LONG" if slot.is_long else "SHORT", state.success_count,
            state.today_tp_count, state.today_tp_date, slot.place_price, slot.tp_price,
        )

    # ── Refill missing TP only when TP level is far enough from current price ─
    all_slots = list(active_slots)
    refill_candidates = sorted(
        all_slots,
        key=lambda slot: abs(slot.tp_price - current_price),
    )
    for slot in refill_candidates:
        active_tp_count = count_side_tp_orders(state, side)
        signed_position = position_size_signed(evidence.position_after)
        has_side_position = signed_position > 0 if side == SIDE_LONG else signed_position < 0
        allow_zero_tp_recovery = active_tp_count == 0 and has_side_position
        if not should_refill_tp_for_slot(
            slot,
            current_price,
            cfg,
            allow_any_distance=allow_zero_tp_recovery,
        ):
            continue
        if allow_zero_tp_recovery:
            LOGGER.info(
                "[tp:recover-zero] side=%s position=%s tp_price=%.4f entry=%.4f",
                side,
                signed_position,
                slot.tp_price,
                slot.place_price,
            )
        if not await ensure_tp_capacity_for_new_order(
            monitor=monitor,
            client=client,
            state=state,
            cfg=cfg,
            side=side,
            active_set=active_set,
            current_price=current_price,
            desired_tp_price=slot.tp_price,
            reason=f"tp-refill@{slot.tp_price:.4f}",
        ):
            continue
        tp_idx = state.alloc_idx()
        label = f"{'LONG' if slot.is_long else 'SHORT'} TP(refill) @{slot.tp_price:.4f}"
        ok = await do_place_order(
            monitor, client, cfg.market_id, tp_idx, base_amount, price_decimals,
            price_to_wire(slot.tp_price, price_decimals),
            is_ask=slot.is_long, reduce_only=True, dry_run=cfg.dry_run,
            label=label, slot=slot, slot_kind="tp",
        )
        if ok:
            slot.tp_order_idx = tp_idx
            slot.tp_base_amount = base_amount
        else:
            LOGGER.error(
                "[tp:refill-failed] side=%s tp_price=%.4f min_steps=%s",
                "LONG" if slot.is_long else "SHORT",
                slot.tp_price,
                cfg.tp_refill_min_steps,
            )

    # ── Place new entry orders to fill grid ──────────────────────────────────
    if side == SIDE_LONG:
        for i in range(cfg.levels):
            place_price = aligned - cfg.price_step * i
            if place_price <= 0 or place_price >= current_price:
                continue
            if place_price + cfg.price_step in filled_tp_prices:
                LOGGER.info(
                    "[entry:skip-tp-reopen] side=long place_price=%.4f tp=%.4f is a tp level that just fired this cycle",
                    place_price, place_price + cfg.price_step,
                )
                continue
            k    = GridState.price_key(place_price)
            slot = state.long_slots.get(k)
            if slot is None:
                slot = GridSlot(place_price=place_price, tp_price=place_price + cfg.price_step, is_long=True)
                state.long_slots[k] = slot
            if slot.status != SLOT_IDLE:
                continue
            place_idx = state.alloc_idx()
            ok = await do_place_order(
                monitor, client, cfg.market_id, place_idx, base_amount, price_decimals,
                price_to_wire(place_price, price_decimals),
                is_ask=False, reduce_only=False, dry_run=cfg.dry_run,
                label=f"LONG entry @{place_price:.4f}", slot=slot, slot_kind="entry",
            )
            if ok:
                slot.place_order_idx = place_idx
                slot.place_base_amount = base_amount
                slot.status          = SLOT_NEW
    else:
        for i in range(1, cfg.levels + 1):
            place_price = aligned + cfg.price_step * i
            if place_price <= current_price:
                continue
            if place_price - cfg.price_step in filled_tp_prices:
                LOGGER.info(
                    "[entry:skip-tp-reopen] side=short place_price=%.4f tp=%.4f is a tp level that just fired this cycle",
                    place_price, place_price - cfg.price_step,
                )
                continue
            k    = GridState.price_key(place_price)
            slot = state.short_slots.get(k)
            if slot is None:
                slot = GridSlot(place_price=place_price, tp_price=place_price - cfg.price_step, is_long=False)
                state.short_slots[k] = slot
            if slot.status != SLOT_IDLE:
                continue
            place_idx = state.alloc_idx()
            ok = await do_place_order(
                monitor, client, cfg.market_id, place_idx, base_amount, price_decimals,
                price_to_wire(place_price, price_decimals),
                is_ask=True, reduce_only=False, dry_run=cfg.dry_run,
                label=f"SHORT entry @{place_price:.4f}", slot=slot, slot_kind="entry",
            )
            if ok:
                slot.place_order_idx = place_idx
                slot.place_base_amount = base_amount
                slot.status          = SLOT_NEW
