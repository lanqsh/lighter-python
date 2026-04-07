import csv
import logging
from datetime import datetime
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Optional

from models import GridSlot, RuntimeMonitor, position_size_signed
from price_utils import wire_price_to_float

LOGGER = logging.getLogger("smart_grid")

ORDER_TRACE_FILE: Optional[Path] = None
ORDER_TRACE_HEADERS = [
    "place_time",
    "fill_time",
    "market_id",
    "order_kind",
    "label",
    "client_order_index",
    "linked_place_order_index",
    "price",
    "price_wire",
    "base_amount",
    "is_ask",
    "reduce_only",
    "slot_side",
    "entry_price",
    "tp_price",
    "position_signed",
    "position_abs",
    "open_order_count",
    "pending_order_count",
]


def setup_logging(market_id: int, side: str) -> Path:
    log_dir = Path.cwd() / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / f"smart_grid_market{market_id}_{side}.log"

    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(filename)s:%(lineno)d | %(message)s")
    LOGGER.setLevel(logging.INFO)
    LOGGER.propagate = False
    LOGGER.handlers.clear()

    file_handler = RotatingFileHandler(
        log_path, maxBytes=10 * 1024 * 1024, backupCount=5, encoding="utf-8"
    )
    file_handler.setFormatter(formatter)
    file_handler.setLevel(logging.INFO)

    LOGGER.addHandler(file_handler)
    LOGGER.info("[logger] initialized path=%s", log_path)
    return log_path


def setup_order_trace_file(market_id: int, side: str) -> Path:
    global ORDER_TRACE_FILE
    trace_dir = Path.cwd() / "logs"
    trace_dir.mkdir(parents=True, exist_ok=True)
    trace_path = trace_dir / f"smart_grid_market{market_id}_{side}_orders.csv"
    if not trace_path.exists() or trace_path.stat().st_size == 0:
        with trace_path.open("w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=ORDER_TRACE_HEADERS)
            writer.writeheader()
    ORDER_TRACE_FILE = trace_path
    LOGGER.info("[trace:file] active order trace file: %s", ORDER_TRACE_FILE)
    return trace_path


def now_iso_ms() -> str:
    return datetime.now().isoformat(timespec="milliseconds")


def append_filled_order_trace_record(
    market_id: int,
    order_kind: str,
    label: str,
    client_order_index: int,
    linked_place_order_index: int,
    price_wire: int,
    price_decimals: int,
    base_amount: int,
    is_ask: bool,
    reduce_only: bool,
    slot: Optional[GridSlot],
    monitor: RuntimeMonitor,
    place_time: str,
    fill_time: str,
) -> None:
    if ORDER_TRACE_FILE is None:
        return
    snapshot = monitor.last_position
    record = {
        "place_time": place_time,
        "fill_time": fill_time,
        "market_id": market_id,
        "order_kind": order_kind,
        "label": label,
        "client_order_index": client_order_index,
        "linked_place_order_index": linked_place_order_index,
        "price": wire_price_to_float(str(price_wire), price_decimals),
        "price_wire": price_wire,
        "base_amount": base_amount,
        "is_ask": is_ask,
        "reduce_only": reduce_only,
        "slot_side": (
            "LONG" if slot is not None and slot.is_long else
            ("SHORT" if slot is not None else "")
        ),
        "entry_price": slot.place_price if slot is not None else 0.0,
        "tp_price": slot.tp_price if slot is not None else 0.0,
        "position_signed": position_size_signed(snapshot),
        "position_abs": snapshot.position if snapshot is not None else 0.0,
        "open_order_count": snapshot.open_order_count if snapshot is not None else 0,
        "pending_order_count": snapshot.pending_order_count if snapshot is not None else 0,
    }
    try:
        with ORDER_TRACE_FILE.open("a", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=ORDER_TRACE_HEADERS)
            writer.writerow(record)
    except Exception as trace_exc:
        LOGGER.warning("[trace:file] write failed path=%s reason=%s", ORDER_TRACE_FILE, trace_exc)
