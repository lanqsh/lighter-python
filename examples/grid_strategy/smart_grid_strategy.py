
import asyncio
import json
import logging
import sys
import time
from logging.handlers import RotatingFileHandler
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple, Any

ROOT_DIR = Path(__file__).resolve().parents[2]
EXAMPLES_DIR = ROOT_DIR / "examples"
if str(ROOT_DIR) not in sys.path:
    sys.path.append(str(ROOT_DIR))
if str(EXAMPLES_DIR) not in sys.path:
    sys.path.append(str(EXAMPLES_DIR))

import lighter
from lighter.exceptions import ApiException

SLOT_IDLE   = "IDLE"
SLOT_NEW    = "NEW"
SLOT_FILLED = "FILLED"

ACTIVE_STATUSES = {"open", "in-progress", "pending"}
SIDE_LONG = "long"
SIDE_SHORT = "short"
LOGGER = logging.getLogger("smart_grid")
RETRYABLE_HTTP_STATUS = {408, 429, 500, 502, 503, 504}

def is_retryable_exception(exc: Exception) -> bool:
    if isinstance(exc, (TimeoutError, asyncio.TimeoutError)):
        return True
    if isinstance(exc, ApiException):
        status = getattr(exc, "status", None)
        if isinstance(status, int) and status in RETRYABLE_HTTP_STATUS:
            return True
    text = str(exc).lower()
    return (
        "gateway time-out" in text
        or "gateway timeout" in text
        or "timed out" in text
        or "timeout" in text
    )

def setup_logging(market_id: int, side: str) -> Path:
    log_dir = Path.cwd() / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / f"smart_grid_market{market_id}_{side}.log"

    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(filename)s:%(lineno)d | %(message)s")
    LOGGER.setLevel(logging.INFO)
    LOGGER.propagate = False
    LOGGER.handlers.clear()

    file_handler = RotatingFileHandler(
        log_path,
        maxBytes=10 * 1024 * 1024,
        backupCount=5,
        encoding="utf-8",
    )
    file_handler.setFormatter(formatter)
    file_handler.setLevel(logging.INFO)

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    console_handler.setLevel(logging.INFO)

    LOGGER.addHandler(file_handler)
    LOGGER.addHandler(console_handler)
    LOGGER.info("[logger] initialized path=%s", log_path)
    return log_path

@dataclass
class GridSlot:
    place_price:      float
    tp_price:         float
    is_long:          bool
    status:           str = SLOT_IDLE
    place_order_idx:  int = 0
    tp_order_idx:     int = 0

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "GridSlot":
        return cls(**d)

@dataclass
class PositionSnapshot:
    market_id: int
    symbol: str
    sign: int
    position: float
    avg_entry_price: float
    unrealized_pnl: float
    realized_pnl: float
    open_order_count: int
    pending_order_count: int

@dataclass
class RuntimeMonitor:
    last_position: Optional[PositionSnapshot] = None
    seen_trade_ids: Set[int] = field(default_factory=set)
    recent_trade_client_ids: Set[int] = field(default_factory=set)
    order_lifecycles: Dict[int, "OrderLifecycle"] = field(default_factory=dict)

@dataclass
class OrderLifecycle:
    client_order_index: int
    label: str
    event: str
    is_ask: bool
    reduce_only: bool
    slot_side: str = ""
    slot_kind: str = ""
    entry_price: float = 0.0
    tp_price: float = 0.0
    tx_hash: str = ""
    error: str = ""

@dataclass
class TradeEvidence:
    new_trade_client_ids: Set[int] = field(default_factory=set)
    position_before: Optional[PositionSnapshot] = None
    position_after: Optional[PositionSnapshot] = None

class GridState:
    def __init__(self, start_order_index: int):
        self.long_slots:    Dict[str, GridSlot] = {}
        self.short_slots:   Dict[str, GridSlot] = {}
        self.next_order_idx: int = start_order_index
        self.success_count:  int = 0
        self.today_tp_count: int = 0
        self.today_tp_date:  str = ""

    @staticmethod
    def price_key(price: float) -> str:
        return f"{price:.6f}"

    def alloc_idx(self) -> int:
        idx = self.next_order_idx
        self.next_order_idx += 1
        return idx

    def summary(self) -> str:
        ln = sum(1 for s in self.long_slots.values()  if s.status == SLOT_NEW)
        lf = sum(1 for s in self.long_slots.values()  if s.status == SLOT_FILLED)
        sn = sum(1 for s in self.short_slots.values() if s.status == SLOT_NEW)
        sf = sum(1 for s in self.short_slots.values() if s.status == SLOT_FILLED)
        return (f"long(new={ln} filled={lf}) "
                f"short(new={sn} filled={sf}) "
                f"next_idx={self.next_order_idx} trades={self.success_count}")

@dataclass
class GridConfig:
    market_id:         int
    levels:            int
    price_step:        float
    base_amount:       int
    side:              str
    poll_interval_sec: float
    max_cycles:        int
    start_order_index: int
    dry_run:           bool
    leverage:          int = 1

def default_grid_config() -> GridConfig:
    return GridConfig(
        market_id=0,
        levels=10,
        price_step=10.0,
        base_amount=0,
        side=SIDE_LONG,
        poll_interval_sec=5.0,
        max_cycles=0,
        start_order_index=200000,
        dry_run=False,
    )

def load_grid_config(resolved_config_file: str) -> GridConfig:
    cfg = default_grid_config()
    file_cfg = read_strategy_overrides(resolved_config_file)
    for attr, key, conv in [
        ("market_id", "marketId", int),
        ("levels", "levels", int),
        ("price_step", "priceStep", float),
        ("leverage", "leverage", int),
        ("base_amount", "baseAmount", int),
        ("poll_interval_sec", "pollIntervalSec", float),
        ("max_cycles", "maxCycles", int),
        ("start_order_index", "startOrderIndex", int),
        ("dry_run", "dryRun", bool),
    ]:
        if file_cfg.get(key) is not None:
            setattr(cfg, attr, conv(file_cfg[key]))
    if file_cfg.get("side") is not None:
        cfg.side = normalize_side(file_cfg["side"])
    return cfg

def load_api_key_config() -> Tuple[str, int, Dict[int, str], str]:
    p = Path("api_key_config.json").resolve()
    with p.open("r", encoding="utf-8") as f:
        cfg = json.load(f)
    private_keys = {int(k): v for k, v in cfg["privateKeys"].items()}
    return cfg["baseUrl"], int(cfg["accountIndex"]), private_keys, str(p)

def read_strategy_overrides(resolved_config_file: str) -> Dict[str, Any]:
    if not resolved_config_file:
        return {}
    p = Path(resolved_config_file)
    if not p.exists():
        return {}
    with p.open("r", encoding="utf-8") as f:
        cfg = json.load(f)
    v = cfg.get("grid", {})
    return v if isinstance(v, dict) else {}

def normalize_side(side: str) -> str:
    side_norm = str(side).strip().lower()
    if side_norm not in {SIDE_LONG, SIDE_SHORT}:
        raise ValueError(f"side must be '{SIDE_LONG}' or '{SIDE_SHORT}', got: {side}")
    return side_norm

def format_position_snapshot(snapshot: Optional[PositionSnapshot]) -> str:
    if snapshot is None:
        return "position=none"
    return (
        f"symbol={snapshot.symbol} market_id={snapshot.market_id} sign={snapshot.sign} "
        f"position={snapshot.position} avg_entry={snapshot.avg_entry_price} "
        f"upl={snapshot.unrealized_pnl} rpl={snapshot.realized_pnl} "
        f"open_orders={snapshot.open_order_count} pending_orders={snapshot.pending_order_count}"
    )

def position_size_signed(snapshot: Optional[PositionSnapshot]) -> float:
    if snapshot is None:
        return 0.0
    return snapshot.position * snapshot.sign

def record_order_lifecycle(
    monitor: RuntimeMonitor,
    client_order_index: int,
    label: str,
    event: str,
    is_ask: bool,
    reduce_only: bool,
    slot: Optional[GridSlot] = None,
    slot_kind: str = "",
    tx_hash: str = "",
    error: str = "",
) -> None:
    prev = monitor.order_lifecycles.get(client_order_index)
    effective_slot_side = "LONG" if slot is not None and slot.is_long else ("SHORT" if slot is not None else (prev.slot_side if prev is not None else ""))
    effective_slot_kind = slot_kind or (prev.slot_kind if prev is not None else "")
    effective_entry_price = slot.place_price if slot is not None else (prev.entry_price if prev is not None else 0.0)
    effective_tp_price = slot.tp_price if slot is not None else (prev.tp_price if prev is not None else 0.0)
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
        client_order_index,
        event,
        effective_slot_side,
        effective_slot_kind,
        effective_entry_price,
        effective_tp_price,
        is_ask,
        reduce_only,
        tx_hash,
        error,
        label,
    )

def summarize_active_slots(
    state: GridState,
    side: str,
    active_set: Dict[int, Any],
    max_items: int = 12,
) -> str:
    slots = state.long_slots.values() if side == SIDE_LONG else state.short_slots.values()
    rows: List[Tuple[float, str]] = []
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
    after = position_size_signed(evidence.position_after)
    delta = after - before
    return delta > 0 if slot.is_long else delta < 0

def evidence_confirms_tp_fill(slot: GridSlot, evidence: TradeEvidence) -> bool:
    if slot.tp_order_idx in evidence.new_trade_client_ids:
        return True
    before = position_size_signed(evidence.position_before)
    after = position_size_signed(evidence.position_after)
    delta = after - before
    return delta < 0 if slot.is_long else delta > 0

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

def build_entry_prices_for_side(current_price: float, cfg: "GridConfig") -> List[float]:
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
    cfg: "GridConfig",
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

async def fetch_market_detail(order_api: lighter.OrderApi, market_id: int) -> Any:
    resp = await order_api.order_book_details(market_id=market_id)
    if resp.order_book_details:
        return resp.order_book_details[0]
    if resp.spot_order_book_details:
        return resp.spot_order_book_details[0]
    raise RuntimeError(f"No market detail for market_id={market_id}")

async def fetch_active_orders(
    order_api:     lighter.OrderApi,
    account_index: int,
    market_id:     int,
    auth_token:    str,
) -> List[Any]:
    max_attempts = 3
    for attempt in range(1, max_attempts + 1):
        try:
            resp = await order_api.account_active_orders(
                account_index=account_index,
                market_id=market_id,
                auth=auth_token,
            )
            return resp.orders or []
        except Exception as exc:
            if not is_retryable_exception(exc) or attempt >= max_attempts:
                raise
            delay = 0.6 * attempt
            LOGGER.warning(
                "[orders:retry] market_id=%s account=%s attempt=%s/%s reason=%s sleep=%.1fs",
                market_id,
                account_index,
                attempt,
                max_attempts,
                exc,
                delay,
            )
            await asyncio.sleep(delay)
    return []

async def fetch_position_snapshot(
    account_api: lighter.AccountApi,
    account_index: int,
    market_id: int,
) -> Optional[PositionSnapshot]:
    resp = await account_api.account(by="index", value=str(account_index))
    accounts = resp.accounts or []
    if not accounts:
        return None

    for pos in accounts[0].positions or []:
        if int(pos.market_id) != market_id:
            continue
        return PositionSnapshot(
            market_id=int(pos.market_id),
            symbol=str(pos.symbol),
            sign=int(pos.sign),
            position=float(str(pos.position)),
            avg_entry_price=float(str(pos.avg_entry_price)),
            unrealized_pnl=float(str(pos.unrealized_pnl)),
            realized_pnl=float(str(pos.realized_pnl)),
            open_order_count=int(pos.open_order_count),
            pending_order_count=int(pos.pending_order_count),
        )

    return PositionSnapshot(
        market_id=market_id,
        symbol="",
        sign=0,
        position=0.0,
        avg_entry_price=0.0,
        unrealized_pnl=0.0,
        realized_pnl=0.0,
        open_order_count=0,
        pending_order_count=0,
    )

async def fetch_recent_trades(
    order_api: lighter.OrderApi,
    account_index: int,
    market_id: int,
    auth_token: str,
    limit: int = 20,
) -> List[Any]:
    max_attempts = 3
    for attempt in range(1, max_attempts + 1):
        try:
            resp = await order_api.trades(
                sort_by="timestamp",
                limit=limit,
                account_index=account_index,
                market_id=market_id,
                sort_dir="desc",
                auth=auth_token,
            )
            return resp.trades or []
        except Exception as exc:
            if not is_retryable_exception(exc) or attempt >= max_attempts:
                raise
            delay = 0.6 * attempt
            LOGGER.warning(
                "[trade:retry] market_id=%s account=%s attempt=%s/%s reason=%s sleep=%.1fs",
                market_id,
                account_index,
                attempt,
                max_attempts,
                exc,
                delay,
            )
            await asyncio.sleep(delay)
    return []

def summarize_trade(trade: Any, account_index: int) -> str:
    ask_account_id = int(trade.ask_account_id)
    bid_account_id = int(trade.bid_account_id)
    if ask_account_id == account_index:
        side = "sell"
        client_order_index = int(trade.ask_client_id)
    elif bid_account_id == account_index:
        side = "buy"
        client_order_index = int(trade.bid_client_id)
    else:
        side = "unknown"
        client_order_index = 0
    return (
        f"trade_id={int(trade.trade_id)} side={side} client_order_index={client_order_index} "
        f"price={trade.price} size={trade.size} usd_amount={trade.usd_amount} tx_hash={trade.tx_hash}"
    )

async def initialize_runtime_monitor(
    monitor: RuntimeMonitor,
    account_api: lighter.AccountApi,
    order_api: lighter.OrderApi,
    auth_mgr: "AuthTokenManager",
    account_index: int,
    market_id: int,
) -> None:
    monitor.last_position = await fetch_position_snapshot(account_api, account_index, market_id)
    LOGGER.info("[position:init] %s", format_position_snapshot(monitor.last_position))

    auth_token = await auth_mgr.get()
    trades = await fetch_recent_trades(order_api, account_index, market_id, auth_token)
    monitor.seen_trade_ids = {int(t.trade_id) for t in trades}
    monitor.recent_trade_client_ids = {
        int(t.ask_client_id) for t in trades if int(t.ask_account_id) == account_index
    } | {
        int(t.bid_client_id) for t in trades if int(t.bid_account_id) == account_index
    }
    LOGGER.info("[trade:init] loaded recent trade baseline count=%s", len(monitor.seen_trade_ids))

async def collect_trade_evidence(
    monitor: RuntimeMonitor,
    account_api: lighter.AccountApi,
    order_api: lighter.OrderApi,
    auth_mgr: "AuthTokenManager",
    account_index: int,
    market_id: int,
) -> TradeEvidence:
    evidence = TradeEvidence(position_before=monitor.last_position)
    snapshot = await fetch_position_snapshot(account_api, account_index, market_id)
    evidence.position_after = snapshot
    if format_position_snapshot(snapshot) != format_position_snapshot(monitor.last_position):
        LOGGER.info(
            "[position:change] before=(%s) after=(%s)",
            format_position_snapshot(monitor.last_position),
            format_position_snapshot(snapshot),
        )

    auth_token = await auth_mgr.get()
    trades = await fetch_recent_trades(order_api, account_index, market_id, auth_token)
    new_trades = [t for t in reversed(trades) if int(t.trade_id) not in monitor.seen_trade_ids]
    for trade in new_trades:
        trade_id = int(trade.trade_id)
        monitor.seen_trade_ids.add(trade_id)
        LOGGER.info("[trade:new] %s", summarize_trade(trade, account_index))
        if int(trade.ask_account_id) == account_index:
            evidence.new_trade_client_ids.add(int(trade.ask_client_id))
        if int(trade.bid_account_id) == account_index:
            evidence.new_trade_client_ids.add(int(trade.bid_client_id))

    if len(monitor.seen_trade_ids) > 500:
        monitor.seen_trade_ids = {int(t.trade_id) for t in trades[:200]}

    monitor.recent_trade_client_ids = {
        int(t.ask_client_id) for t in trades if int(t.ask_account_id) == account_index
    } | {
        int(t.bid_client_id) for t in trades if int(t.bid_account_id) == account_index
    }
    monitor.last_position = snapshot
    return evidence

class AuthTokenManager:
    REFRESH_BEFORE_SEC = 120

    def __init__(self, client: lighter.SignerClient, ttl_sec: int = 3600):
        self._client     = client
        self._ttl        = ttl_sec
        self._token:     str   = ""
        self._expire_at: float = 0.0

    async def get(self) -> str:
        if time.time() + self.REFRESH_BEFORE_SEC >= self._expire_at:
            token, err = self._client.create_auth_token_with_expiry(deadline=self._ttl)
            if err is not None:
                raise RuntimeError(f"Failed to create auth token: {err}")
            self._token     = token
            self._expire_at = time.time() + self._ttl
            LOGGER.info("[auth] token refreshed (valid %ss)", self._ttl)
        return self._token

async def do_place_order(
    monitor:     RuntimeMonitor,
    client:      lighter.SignerClient,
    market_id:   int,
    order_idx:   int,
    base_amount: int,
    wire_price:  int,
    is_ask:      bool,
    reduce_only: bool,
    dry_run:     bool,
    label:       str,
    slot:        Optional[GridSlot] = None,
    slot_kind:   str = "",
) -> bool:
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
        LOGGER.warning("[order:resp] label=%s coi=%s tx_hash=%s err=%s", label, order_idx, tx_hash, err)
        record_order_lifecycle(monitor, order_idx, label, "rejected", is_ask, reduce_only, slot=slot, slot_kind=slot_kind, tx_hash=str(tx_hash or ""), error=str(err))
        return False
    LOGGER.info("[order:resp] label=%s coi=%s tx_hash=%s err=None", label, order_idx, tx_hash)
    record_order_lifecycle(monitor, order_idx, label, "accepted", is_ask, reduce_only, slot=slot, slot_kind=slot_kind, tx_hash=str(tx_hash or ""))
    return True

async def do_cancel_order(
    monitor:    RuntimeMonitor,
    client:    lighter.SignerClient,
    market_id: int,
    order_idx: int,
    dry_run:   bool,
    label:     str,
) -> None:
    if dry_run:
        LOGGER.info("[cancel:dry-run] label=%s coi=%s", label, order_idx)
        record_order_lifecycle(monitor, order_idx, label, "cancel-dry-run", False, False)
        return
    LOGGER.info("[cancel:req] label=%s coi=%s market=%s", label, order_idx, market_id)
    existing = monitor.order_lifecycles.get(order_idx)
    record_order_lifecycle(
        monitor,
        order_idx,
        label,
        "cancel-request",
        existing.is_ask if existing is not None else False,
        existing.reduce_only if existing is not None else False,
        slot_kind=existing.slot_kind if existing is not None else "",
    )
    _, tx_hash, err = await client.cancel_order(
        market_index=market_id, order_index=order_idx)
    LOGGER.info("[cancel:resp] label=%s coi=%s tx_hash=%s err=%s", label, order_idx, tx_hash, err)
    record_order_lifecycle(
        monitor,
        order_idx,
        label,
        "cancel-confirmed" if err is None else "cancel-failed",
        existing.is_ask if existing is not None else False,
        existing.reduce_only if existing is not None else False,
        slot_kind=existing.slot_kind if existing is not None else "",
        tx_hash=str(tx_hash or ""),
        error="" if err is None else str(err),
    )

async def cancel_all_active_orders_for_market(
    order_api: lighter.OrderApi,
    client: lighter.SignerClient,
    auth_mgr: "AuthTokenManager",
    account_index: int,
    market_id: int,
    reason: str,
    dry_run: bool,
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
        client_order_index = int(order.client_order_index)
        if dry_run:
            LOGGER.info(
                "[cleanup:%s:dry-run] market_id=%s order_index=%s coi=%s",
                reason,
                market_id,
                exchange_order_index,
                client_order_index,
            )
            continue
        try:
            _, tx_hash, err = await client.cancel_order(
                market_index=market_id,
                order_index=exchange_order_index,
            )
            LOGGER.info(
                "[cleanup:%s] cancel market=%s order_index=%s coi=%s tx_hash=%s err=%s",
                reason,
                market_id,
                exchange_order_index,
                client_order_index,
                tx_hash,
                err,
            )
            if err is None:
                canceled += 1
        except Exception as exc:
            LOGGER.warning(
                "[cleanup:%s] cancel failed market=%s order_index=%s coi=%s reason=%s",
                reason,
                market_id,
                exchange_order_index,
                client_order_index,
                exc,
            )
    return canceled

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
) -> None:
    side = normalize_side(cfg.side)

    auth_token    = await auth_mgr.get()
    active_orders = await fetch_active_orders(
        order_api, account_index, cfg.market_id, auth_token)
    active_set: Dict[int, Any] = {int(o.client_order_index): o for o in active_orders}
    LOGGER.info("[orders:active] count=%s market_id=%s side=%s", len(active_orders), cfg.market_id, side)
    LOGGER.info("[slots:active] %s", summarize_active_slots(state, side, active_set))
    evidence = await collect_trade_evidence(
        monitor=monitor,
        account_api=account_api,
        order_api=order_api,
        auth_mgr=auth_mgr,
        account_index=account_index,
        market_id=cfg.market_id,
    )
    for active_order in active_orders:
        active_coi = int(active_order.client_order_index)
        lifecycle = monitor.order_lifecycles.get(active_coi)
        record_order_lifecycle(
            monitor,
            active_coi,
            lifecycle.label if lifecycle is not None else f"exchange-order-{active_coi}",
            f"active:{active_order.status}",
            bool(active_order.is_ask),
            bool(active_order.reduce_only),
            slot_kind=lifecycle.slot_kind if lifecycle is not None else "",
        )

    aligned       = (int(current_price / cfg.price_step)) * cfg.price_step
    far_threshold = cfg.price_step * cfg.levels * 2

    active_slots = state.long_slots.values() if side == SIDE_LONG else state.short_slots.values()
    for slot in list(active_slots):
        should_cancel = (
            side == SIDE_LONG
            and slot.status == SLOT_NEW
            and slot.place_price < aligned - far_threshold
            and slot.place_order_idx in active_set
        ) or (
            side == SIDE_SHORT
            and slot.status == SLOT_NEW
            and slot.place_price > aligned + far_threshold
            and slot.place_order_idx in active_set
        )
        if should_cancel:
            await do_cancel_order(
                monitor, client, cfg.market_id, slot.place_order_idx, cfg.dry_run,
                f"{'LONG' if slot.is_long else 'SHORT'} entry(far) @{slot.place_price:.4f}")
            slot.status = SLOT_IDLE

    all_slots = list(active_slots)
    for slot in all_slots:
        if slot.status != SLOT_NEW:
            continue
        if slot.place_order_idx in active_set:
            continue
        LOGGER.info(
            "[fill:candidate] entry_order_disappeared side=%s entry_price=%.4f coi=%s",
            "LONG" if slot.is_long else "SHORT",
            slot.place_price,
            slot.place_order_idx,
        )
        record_order_lifecycle(
            monitor,
            slot.place_order_idx,
            f"{'LONG' if slot.is_long else 'SHORT'} entry @{slot.place_price:.4f}",
            "disappeared-from-active",
            not slot.is_long,
            False,
            slot=slot,
            slot_kind="entry",
        )
        if not evidence_confirms_entry_fill(slot, evidence):
            LOGGER.warning(
                "[fill:rejected] side=%s entry_price=%.4f coi=%s reason=no trade/position evidence",
                "LONG" if slot.is_long else "SHORT",
                slot.place_price,
                slot.place_order_idx,
            )
            record_order_lifecycle(
                monitor,
                slot.place_order_idx,
                f"{'LONG' if slot.is_long else 'SHORT'} entry @{slot.place_price:.4f}",
                "disappeared-without-fill-evidence",
                not slot.is_long,
                False,
                slot=slot,
                slot_kind="entry",
            )
            slot.status = SLOT_IDLE
            continue
        record_order_lifecycle(
            monitor,
            slot.place_order_idx,
            f"{'LONG' if slot.is_long else 'SHORT'} entry @{slot.place_price:.4f}",
            "fill-confirmed",
            not slot.is_long,
            False,
            slot=slot,
            slot_kind="entry",
        )

        tp_idx  = state.alloc_idx()
        tp_wire = price_to_wire(slot.tp_price, price_decimals)
        label   = f"{'LONG' if slot.is_long else 'SHORT'} TP @{slot.tp_price:.4f}"
        ok = await do_place_order(
            monitor, client, cfg.market_id, tp_idx, base_amount, tp_wire,
            is_ask=slot.is_long,
            reduce_only=True,
            dry_run=cfg.dry_run,
            label=label,
            slot=slot,
            slot_kind="tp",
        )
        if ok:
            slot.tp_order_idx = tp_idx
            slot.status       = SLOT_FILLED
        else:

            slot.status = SLOT_IDLE

    all_slots = list(active_slots)
    for slot in all_slots:
        if slot.status != SLOT_FILLED:
            continue
        if slot.tp_order_idx in active_set:
            continue
        record_order_lifecycle(
            monitor,
            slot.tp_order_idx,
            f"{'LONG' if slot.is_long else 'SHORT'} TP @{slot.tp_price:.4f}",
            "disappeared-from-active",
            slot.is_long,
            True,
            slot=slot,
            slot_kind="tp",
        )
        if not evidence_confirms_tp_fill(slot, evidence):

            current_position = position_size_signed(evidence.position_after)
            if current_position == 0.0:
                LOGGER.warning(
                    "[tp:no-evidence-but-zero-position] side=%s tp_price=%.4f coi=%s "
                    "position=0 → resetting slot to IDLE (not counted as successful TP)",
                    "LONG" if slot.is_long else "SHORT",
                    slot.tp_price,
                    slot.tp_order_idx,
                )
                record_order_lifecycle(
                    monitor,
                    slot.tp_order_idx,
                    f"{'LONG' if slot.is_long else 'SHORT'} TP @{slot.tp_price:.4f}",
                    "reset-idle-zero-position",
                    slot.is_long,
                    True,
                    slot=slot,
                    slot_kind="tp",
                )
                slot.status = SLOT_IDLE
                continue
            LOGGER.warning(
                "[tp:rejected] side=%s tp_price=%.4f coi=%s reason=no trade/position evidence",
                "LONG" if slot.is_long else "SHORT",
                slot.tp_price,
                slot.tp_order_idx,
            )
            record_order_lifecycle(
                monitor,
                slot.tp_order_idx,
                f"{'LONG' if slot.is_long else 'SHORT'} TP @{slot.tp_price:.4f}",
                "disappeared-without-fill-evidence",
                slot.is_long,
                True,
                slot=slot,
                slot_kind="tp",
            )
            continue

        slot.status = SLOT_IDLE
        state.success_count += 1

        import datetime as _dt
        _today = _dt.date.today().isoformat()
        if state.today_tp_date != _today:
            state.today_tp_count = 0
            state.today_tp_date  = _today
        state.today_tp_count += 1
        record_order_lifecycle(
            monitor,
            slot.tp_order_idx,
            f"{'LONG' if slot.is_long else 'SHORT'} TP @{slot.tp_price:.4f}",
            "fill-confirmed",
            slot.is_long,
            True,
            slot=slot,
            slot_kind="tp",
        )
        LOGGER.info(
            "[trade:slot-closed] side=%s total_tp=%s today_tp=%s(%s) entry=%.4f tp=%.4f",
            "LONG" if slot.is_long else "SHORT",
            state.success_count,
            state.today_tp_count,
            state.today_tp_date,
            slot.place_price,
            slot.tp_price,
        )

    if side == SIDE_LONG:
        for i in range(1, cfg.levels + 1):
            place_price = aligned - cfg.price_step * i
            if place_price <= 0 or place_price >= current_price:
                continue
            k    = GridState.price_key(place_price)
            slot = state.long_slots.get(k)
            if slot is None:
                slot = GridSlot(
                    place_price=place_price,
                    tp_price=place_price + cfg.price_step,
                    is_long=True,
                )
                state.long_slots[k] = slot
            if slot.status != SLOT_IDLE:
                continue
            place_idx = state.alloc_idx()
            ok = await do_place_order(
                monitor, client, cfg.market_id, place_idx, base_amount,
                price_to_wire(place_price, price_decimals),
                is_ask=False, reduce_only=False,
                dry_run=cfg.dry_run,
                label=f"LONG entry @{place_price:.4f}",
                slot=slot,
                slot_kind="entry",
            )
            if ok:
                slot.place_order_idx = place_idx
                slot.status          = SLOT_NEW
    else:
        for i in range(1, cfg.levels + 1):
            place_price = aligned + cfg.price_step * i
            if place_price <= current_price:
                continue
            k    = GridState.price_key(place_price)
            slot = state.short_slots.get(k)
            if slot is None:
                slot = GridSlot(
                    place_price=place_price,
                    tp_price=place_price - cfg.price_step,
                    is_long=False,
                )
                state.short_slots[k] = slot
            if slot.status != SLOT_IDLE:
                continue
            place_idx = state.alloc_idx()
            ok = await do_place_order(
                monitor, client, cfg.market_id, place_idx, base_amount,
                price_to_wire(place_price, price_decimals),
                is_ask=True, reduce_only=False,
                dry_run=cfg.dry_run,
                label=f"SHORT entry @{place_price:.4f}",
                slot=slot,
                slot_kind="entry",
            )
            if ok:
                slot.place_order_idx = place_idx
                slot.status          = SLOT_NEW

async def run_strategy() -> None:
    base_url, account_index, private_keys, resolved_cfg_path = load_api_key_config()
    cfg = load_grid_config(resolved_cfg_path)
    if cfg.levels <= 0:
        raise ValueError("levels must be > 0")
    if cfg.price_step <= 0:
        raise ValueError("price-step must be > 0")
    cfg.side = normalize_side(cfg.side)
    log_path = setup_logging(cfg.market_id, cfg.side)
    LOGGER.info("[config] using: %s", resolved_cfg_path)

    LOGGER.info(
        "[config] market_id=%s levels=%s price_step=%s leverage=%sx base_amount=%s side=%s poll_interval=%ss max_cycles=%s start_order_index=%s dry_run=%s",
        cfg.market_id, cfg.levels, cfg.price_step, cfg.leverage, cfg.base_amount, cfg.side,
        cfg.poll_interval_sec, cfg.max_cycles, cfg.start_order_index, cfg.dry_run,
    )
    LOGGER.info("[logger] active log file: %s", log_path)

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
                LOGGER.warning("set leverage failed: %s", err)
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
        if cfg.base_amount <= 0:
            LOGGER.info(
                "[base_amount] auto=%s required=%s side=%s entry_count=%s min_entry=%s",
                base_amount,
                required_base_amount,
                cfg.side,
                entry_count,
                min_entry_text,
            )
        elif cfg.base_amount < required_base_amount:
            LOGGER.warning(
                "[base_amount] configured=%s too small for deepest grid price; using auto=%s required=%s side=%s entry_count=%s min_entry=%s",
                cfg.base_amount,
                base_amount,
                required_base_amount,
                cfg.side,
                entry_count,
                min_entry_text,
            )
        else:
            LOGGER.info(
                "[base_amount] configured=%s accepted required=%s side=%s entry_count=%s min_entry=%s",
                cfg.base_amount,
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

        aligned = (int(current_price / cfg.price_step)) * cfg.price_step
        LOGGER.info(
            "start symbol=%s market_id=%s price=%.4f aligned=%.4f price_step=%s levels=%s base_amount=%s side=%s leverage=%sx dry_run=%s",
            symbol, cfg.market_id, current_price, aligned, cfg.price_step, cfg.levels,
            base_amount, cfg.side, cfg.leverage, cfg.dry_run,
        )
        LOGGER.info("state: %s", state.summary())

        cycle = 0
        while cfg.max_cycles == 0 or cycle < cfg.max_cycles:
            await asyncio.sleep(cfg.poll_interval_sec)

            market_detail = await fetch_market_detail(order_api, cfg.market_id)
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
                    account_index=account_index,
                    auth_mgr=auth_mgr,
                )
            except Exception as e:
                if is_retryable_exception(e):
                    LOGGER.warning("[cycle:transient-error] cycle=%s reason=%s", cycle, e)
                else:
                    raise
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
            LOGGER.warning("[cleanup:shutdown] failed market_id=%s reason=%s", cfg.market_id, cleanup_exc)
        for c, name in [(client, "SignerClient"), (api_client, "ApiClient")]:
            try:
                await c.close()
            except Exception as e:
                LOGGER.warning("Error closing %s: %s", name, e)

if __name__ == "__main__":
    try:
        asyncio.run(run_strategy())
    except KeyboardInterrupt:
        LOGGER.info("Shutdown (Ctrl+C). Exiting gracefully...")
    except Exception as e:
        LOGGER.exception("Fatal error: %s", e)
        raise

