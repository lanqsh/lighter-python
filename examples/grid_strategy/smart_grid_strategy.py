
import asyncio
import datetime as _dt
import logging
import sys
import urllib.request
from pathlib import Path
from typing import Optional
from zoneinfo import ZoneInfo

ROOT_DIR = Path(__file__).resolve().parents[2]
EXAMPLES_DIR = ROOT_DIR / "examples"
if str(ROOT_DIR) not in sys.path:
    sys.path.append(str(ROOT_DIR))
if str(EXAMPLES_DIR) not in sys.path:
    sys.path.append(str(EXAMPLES_DIR))

import lighter

from auth import AuthTokenManager
from config import load_api_key_config, load_grid_config, normalize_side
from exchange import (
    fetch_account_snapshot,
    fetch_market_detail,
    format_api_exception,
    initialize_runtime_monitor,
    is_rate_limited_exception,
    is_retryable_exception,
)
from grid_engine import run_one_cycle, seed_startup_position_take_profits
from market_utils import resolve_market_id_by_selector
from models import GridState, RuntimeMonitor, position_size_signed
from order_executor import cancel_all_active_orders_for_market
from price_utils import resolve_effective_base_amount, size_to_wire
from trace import LOGGER, setup_logging, setup_order_trace_file

SHANGHAI_TZ = ZoneInfo("Asia/Shanghai")
_BARK_ESCAPING_MAP = {
    " ": "%20", '"': "%22", "#": "%23", "%": "%25", "&": "%26",
    "(": "%28", ")": "%29", "+": "%2B", ",": "%2C", "/": "%2F",
    ":": "%3A", ";": "%3B", "<": "%3C", "=": "%3D", ">": "%3E",
    "?": "%3F", "@": "%40", "\\": "%5C", "|": "%7C", "`": "\\`",
    "*": "\\*", "$": "\\$", "[": "%5B", "]": "%5D", "^": "%5E",
    "{": "%7B", "}": "%7D", "~": "%7E", "\n": "%0A",
}


def _escape_bark_message(message: str) -> str:
    return "".join(_BARK_ESCAPING_MAP.get(ch, ch) for ch in message)


def _send_bark_message(bark_server: str, message: str) -> None:
    if not bark_server:
        return
    endpoint = bark_server.rstrip("/") + "/"
    ring = "?level=critical&volume=1"
    url = endpoint + _escape_bark_message(message) + ring
    with urllib.request.urlopen(url, timeout=10):
        pass


async def maybe_send_daily_bark_report(
    bark_server: str,
    last_report_date: str,
    account_api: lighter.AccountApi,
    account_index: int,
    market_symbol: str,
    grid_side: str,
    monitor: RuntimeMonitor,
    state: GridState,
    current_price: float,
) -> str:
    if not bark_server:
        return last_report_date

    now_sh = _dt.datetime.now(SHANGHAI_TZ)
    today_sh = now_sh.date().isoformat()
    if now_sh.hour < 8 or last_report_date == today_sh:
        return last_report_date

    account_snapshot = await fetch_account_snapshot(account_api, account_index)
    total_asset_value = account_snapshot.total_asset_value if account_snapshot is not None else 0.0
    available_balance = account_snapshot.available_balance if account_snapshot is not None else 0.0
    signed_position = position_size_signed(monitor.last_position)
    liquidation_price = monitor.last_position.liquidation_price if monitor.last_position is not None else 0.0
    report_symbol = monitor.last_position.symbol if monitor.last_position is not None and monitor.last_position.symbol else market_symbol

    today_tp = state.today_tp_count if state.today_tp_date == today_sh else 0
    message = (
        f"lighter {report_symbol} "
        f"{grid_side} "
        f"position={signed_position:.4f} "
        f"tp={today_tp} "
        f"liq={liquidation_price:.2f} "
        f"total={total_asset_value:.2f} "
        f"available={available_balance:.2f} "
        f"price={current_price:.2f}"
    )

    await asyncio.to_thread(_send_bark_message, bark_server, message)
    LOGGER.info("[bark:daily-report] sent date=%s shanghai_time=%s", today_sh, now_sh.isoformat())
    return today_sh

async def run_strategy() -> None:
    base_url, account_index, private_keys, resolved_cfg_path, bark_server = load_api_key_config()
    cfg = load_grid_config(resolved_cfg_path)
    if cfg.levels <= 0:
        raise ValueError("levels must be > 0")
    if cfg.price_step <= 0:
        raise ValueError("price-step must be > 0")
    cfg.side = normalize_side(cfg.side)

    configuration = lighter.Configuration(host=base_url)
    configuration.api_key = {"default": private_keys[min(private_keys.keys())]}
    api_client  = lighter.ApiClient(configuration=configuration)
    account_api = lighter.AccountApi(api_client)
    order_api   = lighter.OrderApi(api_client)
    client      = lighter.SignerClient(
        url=base_url,
        account_index=account_index,
        api_private_keys=private_keys,
    )

    resolved_market_id, resolved_symbol = await resolve_market_id_by_selector(order_api, cfg.market_symbol)
    cfg.market_id = resolved_market_id

    log_path = setup_logging(cfg.market_id, cfg.side)
    trace_path = setup_order_trace_file(cfg.market_id, cfg.side)
    LOGGER.info("[config] using: %s", resolved_cfg_path)
    LOGGER.info("[config] bark_enabled=%s", bool(bark_server))
    LOGGER.info(
        "[config] market_selector=%s resolved_symbol=%s market_id=%s levels=%s price_step=%s leverage=%sx base_amount=%s side=%s poll_interval=%ss max_cycles=%s start_order_index=%s dry_run=%s tp_refill_min_steps=%s tp_refill_max_steps=%s",
        cfg.market_symbol, resolved_symbol, cfg.market_id, cfg.levels, cfg.price_step, cfg.leverage, cfg.base_amount, cfg.side,
        cfg.poll_interval_sec, cfg.max_cycles, cfg.start_order_index, cfg.dry_run, cfg.tp_refill_min_steps, cfg.tp_refill_max_steps,
    )
    LOGGER.info("[logger] active log file: %s", log_path)
    LOGGER.info("[trace:file] active order trace file: %s", trace_path)

    state:      Optional[GridState] = None
    monitor = RuntimeMonitor()
    auth_mgr = AuthTokenManager(client, ttl_sec=3600)

    try:
        err = client.check_client()
        if err is not None:
            raise RuntimeError(f"check_client failed: {err}")

        market_detail  = await fetch_market_detail(order_api, cfg.market_id)
        symbol         = market_detail.symbol
        price_decimals = int(market_detail.supported_price_decimals)
        size_decimals  = int(market_detail.supported_size_decimals)
        current_price  = float(market_detail.last_trade_price)
        min_base_amount  = float(str(market_detail.min_base_amount))
        min_quote_amount = float(str(market_detail.min_quote_amount))
        quote_multiplier = int(market_detail.quote_multiplier)

        LOGGER.info(
            "[market] symbol=%s market_id=%s price_decimals=%s size_decimals=%s quote_multiplier=%s min_base_amount=%s min_quote_amount=%s last_price=%s",
            symbol, cfg.market_id, price_decimals, size_decimals, quote_multiplier,
            min_base_amount, min_quote_amount, current_price,
        )

        if cfg.leverage > 1:
            min_imf = int(getattr(market_detail, "min_initial_margin_fraction", 0) or 0)
            if min_imf > 0:
                max_leverage = max(1, 10_000 // min_imf)
                effective_leverage = min(cfg.leverage, max_leverage)
                if cfg.leverage > max_leverage:
                    LOGGER.warning(
                        "Requested leverage=%sx exceeds market max=%sx (min_initial_margin_fraction=%s). Using max leverage.",
                        cfg.leverage,
                        max_leverage,
                        min_imf,
                    )
                cfg.leverage = effective_leverage
            else:
                LOGGER.warning(
                    "min_initial_margin_fraction missing/invalid for market_id=%s, using configured leverage=%sx as-is.",
                    cfg.market_id,
                    cfg.leverage,
                )

            LOGGER.info("Setting leverage to %sx (margin_mode=cross) ...", cfg.leverage)
            tx_info, api_response, err = await client.update_leverage(
                market_index=cfg.market_id,
                margin_mode=client.CROSS_MARGIN_MODE,
                leverage=cfg.leverage,
            )
            if err:
                LOGGER.error("set leverage failed: %s", err)
            else:
                LOGGER.info("[leverage] updated tx_info=%s response=%s", tx_info, api_response)
        elif cfg.leverage <= 0:
            LOGGER.warning("Configured leverage=%s is invalid; fallback to 1x.", cfg.leverage)
            cfg.leverage = 1

        base_amount, required_base_amount, entry_count, min_entry_price = resolve_effective_base_amount(
            configured_base_amount=cfg.base_amount,
            current_price=current_price,
            cfg=cfg,
            min_base_amount=min_base_amount,
            min_quote_amount=min_quote_amount,
            price_decimals=price_decimals,
            size_decimals=size_decimals,
            quote_multiplier=quote_multiplier,
        )
        min_entry_text = f"{min_entry_price:.6f}" if min_entry_price is not None else "n/a"
        configured_base_wire = size_to_wire(cfg.base_amount, size_decimals) if cfg.base_amount > 0 else 0
        if configured_base_wire <= 0:
            LOGGER.info(
                "[base_amount] auto=%s required=%s side=%s entry_count=%s min_entry=%s",
                base_amount,
                required_base_amount,
                cfg.side,
                entry_count,
                min_entry_text,
            )
        elif configured_base_wire < required_base_amount:
            LOGGER.warning(
                "[base_amount] configured=%s (wire=%s) too small for deepest grid price; using auto=%s required=%s side=%s entry_count=%s min_entry=%s",
                cfg.base_amount,
                configured_base_wire,
                base_amount,
                required_base_amount,
                cfg.side,
                entry_count,
                min_entry_text,
            )
        else:
            LOGGER.info(
                "[base_amount] configured=%s (wire=%s) accepted required=%s side=%s entry_count=%s min_entry=%s",
                cfg.base_amount,
                configured_base_wire,
                required_base_amount,
                cfg.side,
                entry_count,
                min_entry_text,
            )

        canceled_on_start = await cancel_all_active_orders_for_market(
            order_api=order_api,
            client=client,
            auth_mgr=auth_mgr,
            account_index=account_index,
            market_id=cfg.market_id,
            reason="startup",
            dry_run=cfg.dry_run,
        )
        LOGGER.info(
            "[cleanup:startup] done market_id=%s canceled=%s (positions untouched)",
            cfg.market_id,
            canceled_on_start,
        )

        state = GridState(cfg.start_order_index)
        LOGGER.info("[state] fresh start enabled (no state file load/save)")

        await initialize_runtime_monitor(
            monitor=monitor,
            account_api=account_api,
            order_api=order_api,
            auth_mgr=auth_mgr,
            account_index=account_index,
            market_id=cfg.market_id,
        )

        seeded_tp_orders = await seed_startup_position_take_profits(
            monitor=monitor,
            client=client,
            state=state,
            cfg=cfg,
            current_price=current_price,
            price_decimals=price_decimals,
            size_decimals=size_decimals,
            base_amount=base_amount,
            min_base_amount=min_base_amount,
        )
        LOGGER.info("[startup:position-seed] completed seeded_tp_orders=%s", seeded_tp_orders)

        aligned = (int(current_price / cfg.price_step)) * cfg.price_step
        LOGGER.info(
            "start symbol=%s market_id=%s price=%.4f aligned=%.4f price_step=%s levels=%s base_amount=%s side=%s leverage=%sx dry_run=%s",
            symbol, cfg.market_id, current_price, aligned, cfg.price_step, cfg.levels,
            base_amount, cfg.side, cfg.leverage, cfg.dry_run,
        )
        LOGGER.info("state: %s", state.summary())

        cycle = 0
        last_bark_report_date = ""
        _POLL_BACKOFF_FACTOR     = 1.5
        _POLL_MAX_SEC            = cfg.poll_interval_sec * 3.0
        _RATE_LIMIT_COOLDOWN_SEC = 60.0
        _in_rate_limit           = False

        while cfg.max_cycles == 0 or cycle < cfg.max_cycles:
            await asyncio.sleep(cfg.poll_interval_sec)

            try:
                market_detail = await fetch_market_detail(order_api, cfg.market_id)
            except Exception as e:
                if is_retryable_exception(e):
                    if is_rate_limited_exception(e):
                        if not _in_rate_limit:
                            new_interval = min(cfg.poll_interval_sec * _POLL_BACKOFF_FACTOR, _POLL_MAX_SEC)
                            LOGGER.warning(
                                "[cycle:rate-limit] cycle=%s reason=%s poll_interval %.1fs -> %.1fs cooldown=%.0fs",
                                cycle, format_api_exception(e), cfg.poll_interval_sec, new_interval, _RATE_LIMIT_COOLDOWN_SEC,
                            )
                            cfg.poll_interval_sec = new_interval
                            _in_rate_limit = True
                        else:
                            LOGGER.warning(
                                "[cycle:rate-limit] cycle=%s still rate-limited, cooldown=%.0fs",
                                cycle, _RATE_LIMIT_COOLDOWN_SEC,
                            )
                        await asyncio.sleep(_RATE_LIMIT_COOLDOWN_SEC)
                    else:
                        LOGGER.warning("[cycle:market-detail-error] cycle=%s reason=%s", cycle, format_api_exception(e))
                    continue
                raise
            current_price = float(market_detail.last_trade_price)

            LOGGER.info("cycle=%s price=%.4f %s", cycle, current_price, state.summary())

            cycle_base_amount, cycle_required_base, cycle_entry_count, cycle_min_entry = resolve_effective_base_amount(
                configured_base_amount=cfg.base_amount,
                current_price=current_price,
                cfg=cfg,
                min_base_amount=min_base_amount,
                min_quote_amount=min_quote_amount,
                price_decimals=price_decimals,
                size_decimals=size_decimals,
                quote_multiplier=quote_multiplier,
            )
            if cycle_base_amount != base_amount:
                cycle_min_entry_text = f"{cycle_min_entry:.6f}" if cycle_min_entry is not None else "n/a"
                LOGGER.info(
                    "[base_amount] cycle-adjust %s -> %s required=%s side=%s entry_count=%s min_entry=%s",
                    base_amount,
                    cycle_base_amount,
                    cycle_required_base,
                    cfg.side,
                    cycle_entry_count,
                    cycle_min_entry_text,
                )
                base_amount = cycle_base_amount

            try:
                await run_one_cycle(
                    monitor=monitor,
                    account_api=account_api,
                    client=client,
                    order_api=order_api,
                    state=state,
                    cfg=cfg,
                    current_price=current_price,
                    price_decimals=price_decimals,
                    base_amount=cycle_base_amount,
                    size_decimals=size_decimals,
                    account_index=account_index,
                    auth_mgr=auth_mgr,
                    bark_server=bark_server,
                )
            except Exception as e:
                if is_retryable_exception(e):
                    if is_rate_limited_exception(e):
                        if not _in_rate_limit:
                            new_interval = min(cfg.poll_interval_sec * _POLL_BACKOFF_FACTOR, _POLL_MAX_SEC)
                            LOGGER.warning(
                                "[cycle:rate-limit] cycle=%s reason=%s poll_interval %.1fs -> %.1fs cooldown=%.0fs",
                                cycle, format_api_exception(e), cfg.poll_interval_sec, new_interval, _RATE_LIMIT_COOLDOWN_SEC,
                            )
                            cfg.poll_interval_sec = new_interval
                            _in_rate_limit = True
                        else:
                            LOGGER.warning(
                                "[cycle:rate-limit] cycle=%s still rate-limited, cooldown=%.0fs",
                                cycle, _RATE_LIMIT_COOLDOWN_SEC,
                            )
                        await asyncio.sleep(_RATE_LIMIT_COOLDOWN_SEC)
                    else:
                        LOGGER.warning("[cycle:transient-error] cycle=%s reason=%s", cycle, format_api_exception(e))
                else:
                    raise
            else:
                _in_rate_limit = False

            today = _dt.date.today().isoformat()
            today_tp = state.today_tp_count if state.today_tp_date == today else 0
            LOGGER.info(
                "[tp:summary] cycle=%s total_tp=%s today_tp=%s(%s)",
                cycle,
                state.success_count,
                today_tp,
                today,
            )

            try:
                last_bark_report_date = await maybe_send_daily_bark_report(
                    bark_server=bark_server,
                    last_report_date=last_bark_report_date,
                    account_api=account_api,
                    account_index=account_index,
                    market_symbol=symbol,
                    grid_side=cfg.side,
                    monitor=monitor,
                    state=state,
                    current_price=current_price,
                )
            except Exception as bark_exc:
                LOGGER.warning("[bark:daily-report] failed reason=%s", bark_exc)

            cycle += 1

    finally:
        trades = state.success_count if state is not None else 0
        LOGGER.info("Exiting. Completed trades: %s", trades)
        try:
            canceled_on_exit = await cancel_all_active_orders_for_market(
                order_api=order_api,
                client=client,
                auth_mgr=auth_mgr,
                account_index=account_index,
                market_id=cfg.market_id,
                reason="shutdown",
                dry_run=cfg.dry_run,
            )
            LOGGER.info(
                "[cleanup:shutdown] done market_id=%s canceled=%s (positions untouched)",
                cfg.market_id,
                canceled_on_exit,
            )
        except Exception as cleanup_exc:
            LOGGER.warning("[cleanup:shutdown] failed market_id=%s reason=%s", cfg.market_id, format_api_exception(cleanup_exc))
        for c, name in [(client, "SignerClient"), (api_client, "ApiClient")]:
            try:
                await c.close()
            except Exception as e:
                LOGGER.warning("Error closing %s: %s", name, e)


def main() -> None:
    try:
        asyncio.run(run_strategy())
    except KeyboardInterrupt:
        LOGGER.info("Shutdown (Ctrl+C). Exiting gracefully...")
    except Exception as e:
        LOGGER.exception("Fatal error: %s", e)
        raise

if __name__ == "__main__":
    main()

