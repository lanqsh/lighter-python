from typing import Any, List, Tuple

import lighter


async def query_markets_by_selector(order_api: lighter.OrderApi, selector: str) -> List[Any]:
    selector_norm = str(selector).strip().upper()
    if not selector_norm:
        raise ValueError("selector must not be empty")

    resp = await order_api.order_books()
    order_books = resp.order_books or []

    if selector_norm.isdigit():
        market_id = int(selector_norm)
        return [ob for ob in order_books if int(ob.market_id) == market_id]

    return [ob for ob in order_books if selector_norm in str(ob.symbol).upper()]


async def resolve_market_id_by_selector(order_api: lighter.OrderApi, selector: str) -> Tuple[int, str]:
    rows = await query_markets_by_selector(order_api, selector)
    if not rows:
        raise ValueError(f"No market found for selector={selector}")

    selector_norm = str(selector).strip().upper()
    if selector_norm.isdigit():
        chosen = rows[0]
    else:
        exact_rows = [ob for ob in rows if str(ob.symbol).upper().startswith(selector_norm)]
        chosen = sorted(exact_rows or rows, key=lambda x: int(x.market_id))[0]
    return int(chosen.market_id), str(chosen.symbol)


