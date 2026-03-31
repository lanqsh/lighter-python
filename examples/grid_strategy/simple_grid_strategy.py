import argparse
import asyncio
import json
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Tuple, Any

ROOT_DIR = Path(__file__).resolve().parents[2]
EXAMPLES_DIR = ROOT_DIR / "examples"
if str(ROOT_DIR) not in sys.path:
    sys.path.append(str(ROOT_DIR))
if str(EXAMPLES_DIR) not in sys.path:
    sys.path.append(str(EXAMPLES_DIR))

import lighter


@dataclass
class GridConfig:
    market_id: int
    levels: int
    price_step: float
    rebalance_threshold: float
    base_amount: int
    clear_on_start: bool
    poll_interval_sec: float
    max_cycles: int
    start_order_index: int
    dry_run: bool
    config_file: str
    leverage: int = 1


def parse_args() -> GridConfig:
    parser = argparse.ArgumentParser(description="Simple limit-order grid strategy for Lighter.")
    parser.add_argument("--market-id", type=int, default=0, help="Market ID. 0 = ETH perp in existing examples.")
    parser.add_argument("--levels", type=int, default=3, help="Grid levels on each side of anchor price.")
    parser.add_argument("--price-step", type=float, default=5.0, help="Absolute price gap between adjacent levels, e.g. 5 means +/-5, +/-10...")
    parser.add_argument("--rebalance-threshold", type=float, default=10.0, help="Rebuild grid when anchor moves by this absolute price amount.")
    parser.add_argument("--base-amount", type=int, default=0, help="Order base amount in SDK units. 0 means auto-use market minimum size.")
    parser.add_argument("--clear-on-start", action="store_true", help="On startup, cancel all active orders for this market only.")
    parser.add_argument("--no-clear-on-start", action="store_true", help="Disable startup order cleanup for this market.")
    parser.add_argument("--poll-interval-sec", type=float, default=5.0, help="Seconds between strategy checks.")
    parser.add_argument("--max-cycles", type=int, default=200, help="Max loop iterations before graceful stop.")
    parser.add_argument("--start-order-index", type=int, default=100000, help="Starting order index used by strategy.")
    parser.add_argument("--config-file", type=str, default="", help="Path to api_key_config.json. If empty, auto-detect.")
    parser.add_argument("--dry-run", action="store_true", help="Print actions but do not send create/cancel orders.")

    args = parser.parse_args()
    clear_on_start = True
    if args.clear_on_start:
        clear_on_start = True
    if args.no_clear_on_start:
        clear_on_start = False

    return GridConfig(
        market_id=args.market_id,
        levels=args.levels,
        price_step=args.price_step,
        rebalance_threshold=args.rebalance_threshold,
        base_amount=args.base_amount,
        clear_on_start=clear_on_start,
        poll_interval_sec=args.poll_interval_sec,
        max_cycles=args.max_cycles,
        start_order_index=args.start_order_index,
        dry_run=args.dry_run,
        config_file=args.config_file,
    )


def load_api_key_config(config_file: str) -> Tuple[str, int, Dict[int, str]]:
    candidates: List[Path] = []
    if config_file:
        candidates.append(Path(config_file).expanduser().resolve())
    candidates.append(Path.cwd() / "api_key_config.json")
    candidates.append(EXAMPLES_DIR / "api_key_config.json")
    candidates.append(ROOT_DIR / "api_key_config.json")

    config_path = None
    for candidate in candidates:
        if candidate.exists():
            config_path = candidate
            break

    if config_path is None:
        raise FileNotFoundError(
            "api_key_config.json not found. Pass --config-file or place it in current dir/examples/repo root."
        )

    with config_path.open("r", encoding="utf-8") as f:
        cfg = json.load(f)

    private_keys = {int(k): v for k, v in cfg["privateKeys"].items()}
    return cfg["baseUrl"], int(cfg["accountIndex"]), private_keys


async def fetch_market_snapshot(order_api: lighter.OrderApi, market_id: int) -> Tuple[str, float, int]:
    response = await order_api.order_book_details(market_id=market_id)

    if response.order_book_details:
        detail = response.order_book_details[0]
    elif response.spot_order_book_details:
        detail = response.spot_order_book_details[0]
    else:
        raise RuntimeError(f"No market details found for market_id={market_id}")

    symbol = detail.symbol
    last_trade_price = float(detail.last_trade_price)
    price_decimals = int(detail.supported_price_decimals)
    return symbol, last_trade_price, price_decimals


async def fetch_market_detail(order_api: lighter.OrderApi, market_id: int) -> Any:
    response = await order_api.order_book_details(market_id=market_id)

    if response.order_book_details:
        return response.order_book_details[0]
    if response.spot_order_book_details:
        return response.spot_order_book_details[0]

    raise RuntimeError(f"No market details found for market_id={market_id}")


def price_to_wire(price: float, price_decimals: int) -> int:
    scale = 10 ** price_decimals
    return int(round(price * scale))


def size_to_wire(size: float, size_decimals: int) -> int:
    scale = 10 ** size_decimals
    return int(round(size * scale))


def build_grid_prices(anchor_price: float, levels: int, price_step: float) -> List[Tuple[float, float]]:
    rows: List[Tuple[float, float]] = []
    for level in range(1, levels + 1):
        diff = price_step * level
        buy_price = anchor_price - diff
        sell_price = anchor_price + diff
        if buy_price <= 0:
            continue
        rows.append((buy_price, sell_price))
    return rows


async def cancel_all_market_orders(
    client: lighter.SignerClient,
    order_api: lighter.OrderApi,
    account_index: int,
    market_id: int,
    dry_run: bool,
    api_key: str,
) -> List[int]:
    print(f"DEBUG: cancel_all_market_orders - api_key length={len(api_key)}, first 20 chars={api_key[:20]}...")
    print(f"DEBUG: Calling account_active_orders with authorization header")

    try:
        orders_response = await order_api.account_active_orders(
            account_index=account_index,
            market_id=market_id,
            authorization=api_key
        )
    except Exception as e:
        print(f"DEBUG: account_active_orders failed with error: {e}")
        print(f"DEBUG: authorization param was: {api_key[:20]}...")
        raise

    return order_indexes


async def cancel_orders(client: lighter.SignerClient, order_ids: List[int], market_id: int, dry_run: bool) -> None:
    if not order_ids:
        return

    for order_id in order_ids:
        if dry_run:
            print(f"[DRY RUN] cancel order_index={order_id}")
            continue

        _, tx_hash, err = await client.cancel_order(market_index=market_id, order_index=order_id)
        print(f"cancel order_index={order_id} tx_hash={tx_hash} err={err}")


async def place_grid(
    client: lighter.SignerClient,
    market_id: int,
    price_decimals: int,
    base_amount: int,
    anchor_price: float,
    levels: int,
    price_step: float,
    next_order_index: int,
    dry_run: bool,
) -> Tuple[List[int], int]:
    placed_order_ids: List[int] = []
    rows = build_grid_prices(anchor_price, levels, price_step)

    for buy_price, sell_price in rows:
        buy_order_id = next_order_index
        next_order_index += 1
        sell_order_id = next_order_index
        next_order_index += 1

        buy_wire = price_to_wire(buy_price, price_decimals)
        sell_wire = price_to_wire(sell_price, price_decimals)

        if dry_run:
            print(f"[DRY RUN] LONG-grid BUY id={buy_order_id} base_amount={base_amount} price={buy_price:.6f} wire={buy_wire}")
            print(f"[DRY RUN] SHORT-grid SELL id={sell_order_id} base_amount={base_amount} price={sell_price:.6f} wire={sell_wire}")
            placed_order_ids.extend([buy_order_id, sell_order_id])
            continue

        _, tx_hash, err = await client.create_order(
            market_index=market_id,
            client_order_index=buy_order_id,
            base_amount=base_amount,
            price=buy_wire,
            is_ask=False,
            order_type=client.ORDER_TYPE_LIMIT,
            time_in_force=client.ORDER_TIME_IN_FORCE_GOOD_TILL_TIME,
            reduce_only=False,
            trigger_price=0,
        )
        print(f"create LONG-grid BUY id={buy_order_id} tx_hash={tx_hash} err={err}")
        if err is None:
            placed_order_ids.append(buy_order_id)

        _, tx_hash, err = await client.create_order(
            market_index=market_id,
            client_order_index=sell_order_id,
            base_amount=base_amount,
            price=sell_wire,
            is_ask=True,
            order_type=client.ORDER_TYPE_LIMIT,
            time_in_force=client.ORDER_TIME_IN_FORCE_GOOD_TILL_TIME,
            reduce_only=False,
            trigger_price=0,
        )
        print(f"create SHORT-grid SELL id={sell_order_id} tx_hash={tx_hash} err={err}")
        if err is None:
            placed_order_ids.append(sell_order_id)

    return placed_order_ids, next_order_index


async def run_strategy(cfg: GridConfig) -> None:
    if cfg.levels <= 0:
        raise ValueError("levels must be > 0")
    if cfg.price_step <= 0:
        raise ValueError("price-step must be > 0")
    if cfg.rebalance_threshold <= 0:
        raise ValueError("rebalance-threshold must be > 0")

    base_url, account_index, private_keys = load_api_key_config(cfg.config_file)
    file_cfg = read_strategy_overrides(cfg.config_file)
    if file_cfg.get("marketId") is not None:
        cfg.market_id = int(file_cfg["marketId"])
    if file_cfg.get("levels") is not None:
        cfg.levels = int(file_cfg["levels"])
    if file_cfg.get("priceStep") is not None:
        cfg.price_step = float(file_cfg["priceStep"])
    if file_cfg.get("rebalanceThreshold") is not None:
        cfg.rebalance_threshold = float(file_cfg["rebalanceThreshold"])
    if file_cfg.get("clearOnStart") is not None:
        cfg.clear_on_start = bool(file_cfg["clearOnStart"])
    if file_cfg.get("leverage") is not None:
        cfg.leverage = int(file_cfg["leverage"])

    # Setup API client with authentication
    configuration = lighter.Configuration(host=base_url)
    # Use the first available API key for OrderApi authentication
    first_api_key_index = min(private_keys.keys())
    first_api_key_value = private_keys[first_api_key_index]
    print(f"DEBUG: Configuring ApiClient with API key index={first_api_key_index}")
    print(f"DEBUG: API key length={len(first_api_key_value)}, first 20 chars={first_api_key_value[:20]}...")
    configuration.api_key = {"default": first_api_key_value}
    api_client = lighter.ApiClient(configuration=configuration)

    client = lighter.SignerClient(
        url=base_url,
        account_index=account_index,
        api_private_keys=private_keys,
    )
    order_api = lighter.OrderApi(api_client)

    # Keep only strategy-created order ids for final cleanup.
    active_order_ids: List[int] = []
    next_order_index = cfg.start_order_index

    try:
        err = client.check_client()
        if err is not None:
            raise RuntimeError(f"check_client failed: {err}")

        # Set leverage if configured
        if cfg.leverage > 1:
            print(f"Setting leverage to {cfg.leverage}x for market {cfg.market_id}...")
            margin_mode = 1  # 1 = cross margin, 0 = isolated margin
            _, err = await client.update_leverage(
                market_index=cfg.market_id,
                margin_mode=margin_mode,
                leverage=cfg.leverage,
            )
            if err is not None:
                print(f"Warning: Failed to set leverage: {err}")
            else:
                print(f"Leverage set to {cfg.leverage}x successfully")

        market_detail = await fetch_market_detail(order_api, cfg.market_id)
        symbol = market_detail.symbol
        anchor_price = float(market_detail.last_trade_price)
        price_decimals = int(market_detail.supported_price_decimals)
        size_decimals = int(market_detail.supported_size_decimals)
        min_base_amount = float(market_detail.min_base_amount)
        effective_base_amount = cfg.base_amount
        if effective_base_amount <= 0:
            effective_base_amount = size_to_wire(min_base_amount, size_decimals)
        if file_cfg.get("baseAmount") is not None:
            effective_base_amount = int(file_cfg["baseAmount"])
        if cfg.clear_on_start:
            # Get the first API key for authorization
            first_api_key = private_keys[min(private_keys.keys())]
            print(f"DEBUG: Using API key index {min(private_keys.keys())} for cancel_all_market_orders")
            print(f"DEBUG: First API key length={len(first_api_key)}, first 20 chars={first_api_key[:20]}...")
            await cancel_all_market_orders(
                client=client,
                order_api=order_api,
                account_index=account_index,
                market_id=cfg.market_id,
                dry_run=cfg.dry_run,
                api_key=first_api_key,
            )

        print(f"start symbol={symbol} market_id={cfg.market_id} anchor={anchor_price:.6f} leverage={cfg.leverage}x dry_run={cfg.dry_run}")
        print(
            f"grid mode=long+short levels={cfg.levels} price_step={cfg.price_step} "
            f"rebalance_threshold={cfg.rebalance_threshold} base_amount={effective_base_amount}"
        )

        active_order_ids, next_order_index = await place_grid(
            client=client,
            market_id=cfg.market_id,
            price_decimals=price_decimals,
            base_amount=effective_base_amount,
            anchor_price=anchor_price,
            levels=cfg.levels,
            price_step=cfg.price_step,
            next_order_index=next_order_index,
            dry_run=cfg.dry_run,
        )

        for cycle in range(1, cfg.max_cycles + 1):
            await asyncio.sleep(cfg.poll_interval_sec)
            _, current_price, _ = await fetch_market_snapshot(order_api, cfg.market_id)

            move_abs = abs(current_price - anchor_price)
            print(
                f"cycle={cycle} current_price={current_price:.6f} anchor={anchor_price:.6f} move_abs={move_abs:.6f}"
            )

            if move_abs < cfg.rebalance_threshold:
                continue

            print("rebalance: cancel old grid and place new grid")
            await cancel_orders(client, active_order_ids, cfg.market_id, cfg.dry_run)

            anchor_price = current_price
            active_order_ids, next_order_index = await place_grid(
                client=client,
                market_id=cfg.market_id,
                price_decimals=price_decimals,
                base_amount=effective_base_amount,
                anchor_price=anchor_price,
                levels=cfg.levels,
                price_step=cfg.price_step,
                next_order_index=next_order_index,
                dry_run=cfg.dry_run,
            )

    finally:
        print("cleanup: cancel active strategy orders")
        try:
            await cancel_orders(client, active_order_ids, cfg.market_id, cfg.dry_run)
        finally:
            await client.close()
            await api_client.close()


def read_strategy_overrides(config_file: str) -> Dict[str, Any]:
    if not config_file:
        return {}

    config_path = Path(config_file).expanduser().resolve()
    if not config_path.exists():
        return {}

    with config_path.open("r", encoding="utf-8") as f:
        cfg = json.load(f)

    grid_cfg = cfg.get("grid", {})
    if isinstance(grid_cfg, dict):
        return grid_cfg
    return {}


if __name__ == "__main__":
    asyncio.run(run_strategy(parse_args()))
