import asyncio
import logging
from typing import Any, List, Tuple

import lighter
from lighter.exceptions import ApiException


LOGGER = logging.getLogger("smart_grid")


def _is_retryable_exception(exc: Exception) -> bool:
    if isinstance(exc, (TimeoutError, asyncio.TimeoutError)):
        return True
    exc_name = exc.__class__.__name__
    exc_module = exc.__class__.__module__
    if (exc_name in {"ClientPayloadError", "ClientConnectionError"} and exc_module.startswith("aiohttp")):
        return True
    if isinstance(exc, ConnectionResetError):
        return True
    if isinstance(exc, ApiException):
        status = getattr(exc, "status", None)
        return isinstance(status, int) and status in {408, 429, 500, 502, 503, 504}
    text = str(exc).lower()
    return "timed out" in text or "timeout" in text or "gateway timeout" in text


async def query_markets_by_selector(order_api: lighter.OrderApi, selector: str) -> List[Any]:
    selector_norm = str(selector).strip().upper()
    if not selector_norm:
        raise ValueError("selector must not be empty")

    max_attempts = 3
    last_exc: Exception | None = None
    for attempt in range(1, max_attempts + 1):
        try:
            resp = await order_api.order_books()
            break
        except Exception as exc:
            last_exc = exc
            if not _is_retryable_exception(exc) or attempt >= max_attempts:
                raise
            delay = 0.6 * attempt
            LOGGER.warning(
                "[market-selector:retry] selector=%s attempt=%s/%s reason=%s sleep=%.1fs",
                selector_norm,
                attempt,
                max_attempts,
                exc,
                delay,
            )
            await asyncio.sleep(delay)
    else:
        if last_exc is not None:
            raise last_exc
        raise RuntimeError("order_books failed without exception")

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


