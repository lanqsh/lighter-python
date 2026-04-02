from typing import List, Optional, Tuple

from examples.grid_strategy.models import GridConfig, GridSlot, SIDE_LONG, SLOT_NEW, SLOT_FILLED


def price_to_wire(price: float, price_decimals: int) -> int:
    return int(round(price * (10 ** price_decimals)))


def wire_price_to_float(wire_str: str, price_decimals: int) -> float:
    return int(wire_str) / (10 ** price_decimals)


def size_to_wire(size: float, size_decimals: int) -> int:
    return int(round(size * (10 ** size_decimals)))


def ceil_div(numerator: int, denominator: int) -> int:
    if denominator <= 0:
        raise ValueError("denominator must be > 0")
    return (numerator + denominator - 1) // denominator


def build_entry_prices_for_side(current_price: float, cfg: GridConfig) -> List[float]:
    aligned = (int(current_price / cfg.price_step)) * cfg.price_step
    prices: List[float] = []
    if cfg.side == SIDE_LONG:
        for i in range(1, cfg.levels + 1):
            place_price = aligned - cfg.price_step * i
            if place_price <= 0 or place_price >= current_price:
                continue
            prices.append(place_price)
    else:
        for i in range(1, cfg.levels + 1):
            place_price = aligned + cfg.price_step * i
            if place_price <= current_price:
                continue
            prices.append(place_price)
    return prices


def resolve_effective_base_amount(
    configured_base_amount: int,
    current_price: float,
    cfg: GridConfig,
    min_base_amount: float,
    min_quote_amount: float,
    price_decimals: int,
    size_decimals: int,
    quote_multiplier: int,
) -> Tuple[int, int, int, Optional[float]]:
    min_base_wire = max(1, size_to_wire(min_base_amount, size_decimals))
    min_quote_wire = int(round(min_quote_amount * (10 ** price_decimals)))
    entry_prices = build_entry_prices_for_side(current_price, cfg)

    required_base = min_base_wire
    min_entry_price: Optional[float] = min(entry_prices) if entry_prices else None
    for place_price in entry_prices:
        price_wire = price_to_wire(place_price, price_decimals)
        if price_wire <= 0:
            continue
        required_by_quote = ceil_div(min_quote_wire * quote_multiplier, price_wire)
        required_base = max(required_base, required_by_quote)

    effective_base = required_base if configured_base_amount <= 0 else max(configured_base_amount, required_base)
    return effective_base, required_base, len(entry_prices), min_entry_price


def split_position_amounts(total_amount: int, chunk_amount: int, min_base_amount: int) -> List[int]:
    if total_amount <= 0 or chunk_amount <= 0:
        return []
    chunks: List[int] = []
    remaining = total_amount
    while remaining > 0:
        next_chunk = min(chunk_amount, remaining)
        remainder = remaining - next_chunk
        if 0 < remainder < min_base_amount:
            next_chunk += remainder
            remainder = 0
        if next_chunk < min_base_amount:
            if chunks:
                chunks[-1] += next_chunk
            else:
                chunks.append(next_chunk)
            break
        chunks.append(next_chunk)
        remaining = remainder
    return chunks


def should_cancel_far_order(
    slot: GridSlot, side: str, aligned: float, far_threshold: float
) -> Tuple[bool, str, int, float]:
    if side == SIDE_LONG:
        if slot.status == SLOT_NEW and slot.place_price < aligned - far_threshold:
            return True, "entry", slot.place_order_idx, slot.place_price
        if slot.status == SLOT_FILLED and slot.tp_order_idx > 0 and slot.tp_price > aligned + far_threshold:
            return True, "tp", slot.tp_order_idx, slot.tp_price
        return False, "", 0, 0.0

    if slot.status == SLOT_NEW and slot.place_price > aligned + far_threshold:
        return True, "entry", slot.place_order_idx, slot.place_price
    if slot.status == SLOT_FILLED and slot.tp_order_idx > 0 and slot.tp_price < aligned - far_threshold:
        return True, "tp", slot.tp_order_idx, slot.tp_price
    return False, "", 0, 0.0
