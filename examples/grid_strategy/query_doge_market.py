import asyncio

import lighter


HOST = "https://mainnet.zklighter.elliot.ai"
TARGET = "DOGE"


async def main() -> None:
    cfg = lighter.Configuration(host=HOST)
    api_client = lighter.ApiClient(cfg)
    api = lighter.OrderApi(api_client)

    try:
        resp = await api.order_books()
        rows = [ob for ob in (resp.order_books or []) if TARGET in str(ob.symbol).upper()]

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
