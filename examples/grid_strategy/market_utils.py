import asyncio
from typing import Any, List, Tuple

import lighter


HOST = "https://mainnet.zklighter.elliot.ai"
TARGET = "DOGE"


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


async def main() -> None:
    cfg = lighter.Configuration(host=HOST)
    api_client = lighter.ApiClient(cfg)
    api = lighter.OrderApi(api_client)

    try:
        rows = await query_markets_by_selector(api, TARGET)

        if not rows:
            print("No DOGE market found")
            return

        for ob in sorted(rows, key=lambda x: x.market_id):
            sd = int(ob.supported_size_decimals)
            base_amount_for_10 = 10 * (10 ** sd)
            print(
                f"symbol={ob.symbol} marketId={ob.market_id} marketType={ob.market_type} "
                f"size_decimals={sd} min_base_amount={ob.min_base_amount} "
                f"baseAmount_for_10_DOGE={base_amount_for_10}"
            )
    finally:
        await api_client.close()


if __name__ == "__main__":
    asyncio.run(main())
