"""
Smart Grid Strategy for Lighter  (stateful, auto-TP, restartable)
==================================================================

设计要点
--------
1. 状态持久化（state JSON 文件）—— 可随时 Ctrl+C 后重启，自动接管未成交订单。
2. 每个格子独立状态机：IDLE → NEW → FILLED → IDLE（对应 C++ 版本）。
3. 开仓单成交后自动挂止盈单；止盈成交后格子重置为 IDLE 并自动补单。
4. 退出时不撤单，所有挂单继续留在交易所。

Lighter SDK 与 Binance 的关键差异适配
--------------------------------------
- Order.price 是 StrictStr（wire 格式字符串，如 "2035000"），需 int() 再除10^decimals。
- Order.status 值：'open'/'in-progress'/'pending'（活跃），'filled'/'canceled*'（非活跃）。
- PerpsOrderBookDetail.last_trade_price 已是人类可读价格（float/int），非 wire 格式。
- PerpsOrderBookDetail.min_base_amount 是小数字符串（如 "0.001"），需 size_to_wire 换算。
- Lighter perp 为单向模式（无 Binance 对冲模式的 positionSide=LONG/SHORT）。
  → 止盈单使用 reduce_only=True 以便重启时识别类型；失败则重置 IDLE。
- account_active_orders 需要 auth token，且只返回指定 market 的活跃订单。

格子识别规则（无状态文件时从交易所重建）
-----------------------------------------
  is_ask=False, reduce_only=False  → 多方开仓单（long entry）
  is_ask=True,  reduce_only=True   → 多方止盈单（long TP）
  is_ask=True,  reduce_only=False  → 空方开仓单（short entry）
  is_ask=False, reduce_only=True   → 空方止盈单（short TP）
"""

import argparse
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

# ════════════════════════════════════════════════════════════
#  常量
# ════════════════════════════════════════════════════════════
SLOT_IDLE   = "IDLE"
SLOT_NEW    = "NEW"     # 开仓单已提交，在交易所活跃
SLOT_FILLED = "FILLED"  # 开仓单成交，止盈单已提交，在交易所活跃

# Lighter Order.status 中表示活跃的枚举值
ACTIVE_STATUSES = {"open", "in-progress", "pending"}
SIDE_LONG = "long"
SIDE_SHORT = "short"
LOGGER = logging.getLogger("smart_grid")


def setup_logging(market_id: int, side: str) -> Path:
    log_dir = Path.cwd() / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / f"smart_grid_market{market_id}_{side}.log"

    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
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


# ════════════════════════════════════════════════════════════
#  GridSlot  —— 对应 C++ long_grid_order_list_ / short_grid_order_list_ 的 value
# ════════════════════════════════════════════════════════════
@dataclass
class GridSlot:
    place_price:      float       # 开仓限价（human units）
    tp_price:         float       # 止盈限价（human units）
    is_long:          bool        # True=多方, False=空方
    status:           str = SLOT_IDLE
    place_order_idx:  int = 0     # client_order_index for 开仓单
    tp_order_idx:     int = 0     # client_order_index for 止盈单

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


# ════════════════════════════════════════════════════════════
#  GridState  —— 持有全部格子，负责序列化/反序列化
# ════════════════════════════════════════════════════════════
class GridState:
    def __init__(self, start_order_index: int):
        self.long_slots:    Dict[str, GridSlot] = {}   # key = f"{place_price:.6f}"
        self.short_slots:   Dict[str, GridSlot] = {}
        self.next_order_idx: int = start_order_index
        self.success_count:  int = 0

    # ── key helpers ─────────────────────────────────────────
    @staticmethod
    def price_key(price: float) -> str:
        return f"{price:.6f}"

    def alloc_idx(self) -> int:
        idx = self.next_order_idx
        self.next_order_idx += 1
        return idx

    # ── summary ─────────────────────────────────────────────
    def summary(self) -> str:
        ln = sum(1 for s in self.long_slots.values()  if s.status == SLOT_NEW)
        lf = sum(1 for s in self.long_slots.values()  if s.status == SLOT_FILLED)
        sn = sum(1 for s in self.short_slots.values() if s.status == SLOT_NEW)
        sf = sum(1 for s in self.short_slots.values() if s.status == SLOT_FILLED)
        return (f"long(new={ln} filled={lf}) "
                f"short(new={sn} filled={sf}) "
                f"next_idx={self.next_order_idx} trades={self.success_count}")

    # ── persistence ─────────────────────────────────────────
    def save(self, path: Path) -> None:
        data = {
            "next_order_idx": self.next_order_idx,
            "success_count":  self.success_count,
            "long_slots":  {k: v.to_dict() for k, v in self.long_slots.items()},
            "short_slots": {k: v.to_dict() for k, v in self.short_slots.items()},
        }
        tmp = path.with_suffix(".tmp")
        with tmp.open("w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
        tmp.replace(path)

    @classmethod
    def load(cls, path: Path, start_order_index: int) -> Optional["GridState"]:
        if not path.exists():
            return None
        try:
            with path.open("r", encoding="utf-8") as f:
                data = json.load(f)
            state = cls(start_order_index)
            state.next_order_idx = int(data.get("next_order_idx", start_order_index))
            state.success_count  = int(data.get("success_count", 0))
            state.long_slots  = {k: GridSlot.from_dict(v) for k, v in data.get("long_slots", {}).items()}
            state.short_slots = {k: GridSlot.from_dict(v) for k, v in data.get("short_slots", {}).items()}
            return state
        except Exception as e:
            LOGGER.warning("Failed to load state file (%s), will rebuild from exchange.", e)
            return None


# ════════════════════════════════════════════════════════════
#  Config
# ════════════════════════════════════════════════════════════
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
    config_file:       str
    leverage:          int = 1


def parse_args() -> GridConfig:
    p = argparse.ArgumentParser(
        description="Smart Grid Strategy (stateful, auto-TP, restartable) for Lighter."
    )
    p.add_argument("--market-id",         type=int,   default=0)
    p.add_argument("--levels",            type=int,   default=10,    help="每侧格子数量")
    p.add_argument("--price-step",        type=float, default=10.0,  help="相邻格子绝对价差(human units)")
    p.add_argument("--base-amount",       type=int,   default=0,     help="每格数量(wire). 0=自动最小值")
    p.add_argument("--side",              type=str,   default=SIDE_LONG, choices=[SIDE_LONG, SIDE_SHORT], help="单向仓位方向: long 或 short")
    p.add_argument("--poll-interval-sec", type=float, default=5.0)
    p.add_argument("--max-cycles",        type=int,   default=0,     help="0=永久运行")
    p.add_argument("--start-order-index", type=int,   default=200000)
    p.add_argument("--dry-run",           action="store_true")
    p.add_argument("--config-file",       type=str,   default="")
    args = p.parse_args()
    return GridConfig(
        market_id=args.market_id,
        levels=args.levels,
        price_step=args.price_step,
        base_amount=args.base_amount,
        side=args.side,
        poll_interval_sec=args.poll_interval_sec,
        max_cycles=args.max_cycles,
        start_order_index=args.start_order_index,
        dry_run=args.dry_run,
        config_file=args.config_file,
    )


# ════════════════════════════════════════════════════════════
#  共用工具函数
# ════════════════════════════════════════════════════════════
def load_api_key_config(config_file: str) -> Tuple[str, int, Dict[int, str], str]:
    p = Path("api_key_config.json").resolve()
    with p.open("r", encoding="utf-8") as f:
        cfg = json.load(f)
    private_keys = {int(k): v for k, v in cfg["privateKeys"].items()}
    return cfg["baseUrl"], int(cfg["accountIndex"]), private_keys, str(p)


def read_strategy_overrides(resolved_config_file: str) -> Dict[str, Any]:
    """读取 api_key_config.json 中的 grid 配置段。resolved_config_file 必须是已解析的绝对路径。"""
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
    """人类可读价格 → wire 整数"""
    return int(round(price * (10 ** price_decimals)))


def wire_price_to_float(wire_str: str, price_decimals: int) -> float:
    """Order.price (StrictStr, wire 格式) → 人类可读 float

    Lighter Order.price 字段类型为 StrictStr，存储的是 wire 整数的字符串表示。
    例如 price_decimals=2 时，"203500" 表示 $2035.00。
    与 PerpsOrderBookDetail.last_trade_price（已是 float）不同，必须手动换算。
    """
    return int(wire_str) / (10 ** price_decimals)


def size_to_wire(size: float, size_decimals: int) -> int:
    """人类可读数量 → wire 整数"""
    return int(round(size * (10 ** size_decimals)))


def state_file_path(config_file: str, market_id: int, side: str) -> Path:
    """状态文件路径：固定存放在当前工作目录，并按方向隔离。"""
    return Path.cwd() / f"grid_state_market{market_id}_{side}.json"


# ════════════════════════════════════════════════════════════
#  交易所 helpers
# ════════════════════════════════════════════════════════════
async def fetch_market_detail(order_api: lighter.OrderApi, market_id: int) -> Any:
    """返回 PerpsOrderBookDetail 或 SpotOrderBookDetail。

    重要：PerpsOrderBookDetail.last_trade_price 类型为 Union[StrictFloat, StrictInt]，
    已是人类可读价格（如 2035.5），不是 wire 格式，直接 float() 即可。
    """
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
    """返回 List[Order]（Lighter SDK 中当前活跃挂单）。

    Lighter Order 字段（与 Binance 的主要差异）：
      o.price             : StrictStr  —— wire 格式字符串，如 "2035000"
                                          需用 wire_price_to_float() 换算，不能直接用
      o.is_ask            : bool       —— True=SELL/ASK, False=BUY/BID
      o.reduce_only       : bool       —— 是否平仓专用单
      o.client_order_index: int        —— 策略自定义 order index（我们的主要追踪键）
      o.order_index       : int        —— 交易所分配的 order id
      o.status            : str        —— 'open'/'in-progress'/'pending' 等
    """
    resp = await order_api.account_active_orders(
        account_index=account_index,
        market_id=market_id,
        auth=auth_token,
    )
    return resp.orders or []


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
    resp = await order_api.trades(
        sort_by="timestamp",
        limit=limit,
        account_index=account_index,
        market_id=market_id,
        sort_dir="desc",
        auth=auth_token,
    )
    return resp.trades or []


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


# ════════════════════════════════════════════════════════════
#  Auth token 自动刷新管理器
# ════════════════════════════════════════════════════════════
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


# ════════════════════════════════════════════════════════════
#  从交易所活跃订单重建 GridState（无状态文件时调用）
#  对应 C++: InitLongPlaceOrders / InitLongTpOrders / ...
# ════════════════════════════════════════════════════════════
def build_state_from_exchange(
    active_orders:     List[Any],
    price_step:        float,
    price_decimals:    int,
    start_order_index: int,
    side:              str,
) -> GridState:
    """
    识别规则（依赖 reduce_only=True 标记止盈单）：
      is_ask=False, reduce_only=False  →  多方开仓单  → long_slots[entry_price].NEW
      is_ask=True,  reduce_only=True   →  多方止盈单  → long_slots[price-step].FILLED
      is_ask=True,  reduce_only=False  →  空方开仓单  → short_slots[entry_price].NEW
      is_ask=False, reduce_only=True   →  空方止盈单  → short_slots[price+step].FILLED

    注意：Order.price 是 StrictStr（wire 格式），必须通过 wire_price_to_float() 换算。
    """
    state   = GridState(start_order_index)
    max_idx = start_order_index - 1
    side = normalize_side(side)

    for o in active_orders:
        coi         = int(o.client_order_index)
        # Order.price 是 wire 格式字符串，必须换算为人类可读价格
        price       = wire_price_to_float(o.price, price_decimals)
        is_ask      = bool(o.is_ask)
        reduce_only = bool(o.reduce_only)
        max_idx     = max(max_idx, coi)

        if side == SIDE_LONG and not is_ask and not reduce_only:
            # 多方开仓 BUY open
            k    = GridState.price_key(price)
            slot = state.long_slots.setdefault(
                k, GridSlot(place_price=price, tp_price=price + price_step, is_long=True))
            slot.status          = SLOT_NEW
            slot.place_order_idx = coi
            LOGGER.info("[rebuild] LONG entry @%.4f coi=%s", price, coi)

        elif side == SIDE_LONG and is_ask and reduce_only:
            # 多方止盈 SELL reduce_only，对应开仓价 = tp_price - price_step
            entry_price = price - price_step
            k    = GridState.price_key(entry_price)
            slot = state.long_slots.setdefault(
                k, GridSlot(place_price=entry_price, tp_price=price, is_long=True))
            slot.status       = SLOT_FILLED
            slot.tp_order_idx = coi
            slot.tp_price     = price
            LOGGER.info("[rebuild] LONG tp @%.4f entry=%.4f coi=%s", price, entry_price, coi)

        elif side == SIDE_SHORT and is_ask and not reduce_only:
            # 空方开仓 SELL open
            k    = GridState.price_key(price)
            slot = state.short_slots.setdefault(
                k, GridSlot(place_price=price, tp_price=price - price_step, is_long=False))
            slot.status          = SLOT_NEW
            slot.place_order_idx = coi
            LOGGER.info("[rebuild] SHORT entry @%.4f coi=%s", price, coi)

        elif side == SIDE_SHORT and not is_ask and reduce_only:
            # 空方止盈 BUY reduce_only，对应开仓价 = tp_price + price_step
            entry_price = price + price_step
            k    = GridState.price_key(entry_price)
            slot = state.short_slots.setdefault(
                k, GridSlot(place_price=entry_price, tp_price=price, is_long=False))
            slot.status       = SLOT_FILLED
            slot.tp_order_idx = coi
            slot.tp_price     = price
            LOGGER.info("[rebuild] SHORT tp @%.4f entry=%.4f coi=%s", price, entry_price, coi)

    state.next_order_idx = max_idx + 1
    return state


# ════════════════════════════════════════════════════════════
#  下单 / 撤单封装
# ════════════════════════════════════════════════════════════
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


# ════════════════════════════════════════════════════════════
#  单轮主循环  ——  对应 C++ RunGrid()
# ════════════════════════════════════════════════════════════
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
    state_path:     Path,
) -> None:
    side = normalize_side(cfg.side)

    # ──────────────────────────────────────────────────────
    # 0. 获取交易所当前活跃订单
    #    active_set: client_order_index → Order
    #    Order.price 为 wire 格式字符串，本函数内通过 do_place_order 下单时转换
    # ──────────────────────────────────────────────────────
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

    # ──────────────────────────────────────────────────────
    # 1. DeleteLong/ShortPlaceOrders：撤销距当前价过远的开仓单
    # ──────────────────────────────────────────────────────
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

    # ──────────────────────────────────────────────────────
    # 2a. CheckFilledOrders Step A：
    #     开仓单已不在活跃列表 → 视为成交 → 挂止盈单（reduce_only=True）
    # ──────────────────────────────────────────────────────
    all_slots = list(active_slots)
    for slot in all_slots:
        if slot.status != SLOT_NEW:
            continue
        if slot.place_order_idx in active_set:
            continue  # 仍在挂单中
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
            is_ask=slot.is_long,     # 多方止盈=SELL(ask=True); 空方止盈=BUY(ask=False)
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
            # 下单失败（仓位不足说明开仓单被撤而非成交，或并发空仓情形）→ 重置IDLE重新开仓
            slot.status = SLOT_IDLE

    # ──────────────────────────────────────────────────────
    # 2b. CheckFilledOrders Step B：
    #     止盈单已不在活跃列表 → 止盈已成交 → 重置 IDLE + 计数
    # ──────────────────────────────────────────────────────
    all_slots = list(active_slots)
    for slot in all_slots:
        if slot.status != SLOT_FILLED:
            continue
        if slot.tp_order_idx in active_set:
            continue  # 止盈单仍在挂单中
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
            "[trade:slot-closed] side=%s count=%s entry=%.4f tp=%.4f",
            "LONG" if slot.is_long else "SHORT",
            state.success_count,
            slot.place_price,
            slot.tp_price,
        )

    # ──────────────────────────────────────────────────────
    # 3. MakeLong/ShortPlaceOrders：为 IDLE 格子补挂开仓单
    # ──────────────────────────────────────────────────────

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

    # ──────────────────────────────────────────────────────
    # 4. 持久化状态
    # ──────────────────────────────────────────────────────
    if not cfg.dry_run:
        state.save(state_path)


# ════════════════════════════════════════════════════════════
#  主入口
# ════════════════════════════════════════════════════════════
async def run_strategy(cfg: GridConfig) -> None:
    if cfg.levels <= 0:
        raise ValueError("levels must be > 0")
    if cfg.price_step <= 0:
        raise ValueError("price-step must be > 0")
    cfg.side = normalize_side(cfg.side)
    log_path = setup_logging(cfg.market_id, cfg.side)

    base_url, account_index, private_keys, resolved_cfg_path = load_api_key_config(cfg.config_file)
    LOGGER.info("[config] using: %s", resolved_cfg_path)
    file_cfg = read_strategy_overrides(resolved_cfg_path)

    # 从配置文件覆盖参数
    for attr, key, conv in [
        ("market_id",   "marketId",   int),
        ("levels",      "levels",     int),
        ("price_step",  "priceStep",  float),
        ("leverage",    "leverage",   int),
        ("base_amount", "baseAmount", int),
    ]:
        if file_cfg.get(key) is not None:
            setattr(cfg, attr, conv(file_cfg[key]))
    if file_cfg.get("side") is not None:
        cfg.side = normalize_side(file_cfg["side"])
        log_path = setup_logging(cfg.market_id, cfg.side)

    LOGGER.info(
        "[config] market_id=%s levels=%s price_step=%s leverage=%sx base_amount=%s side=%s poll_interval=%ss max_cycles=%s start_order_index=%s dry_run=%s",
        cfg.market_id, cfg.levels, cfg.price_step, cfg.leverage, cfg.base_amount, cfg.side,
        cfg.poll_interval_sec, cfg.max_cycles, cfg.start_order_index, cfg.dry_run,
    )
    LOGGER.info("[logger] active log file: %s", log_path)

    # ── SDK 初始化 ───────────────────────────────────────
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
    state_path: Path                = state_file_path(cfg.config_file, cfg.market_id, cfg.side)
    monitor = RuntimeMonitor()

    try:
        err = client.check_client()
        if err is not None:
            raise RuntimeError(f"check_client failed: {err}")

        # ── 市场信息 ─────────────────────────────────────
        # PerpsOrderBookDetail 关键字段：
        #   last_trade_price:         Union[float,int]  已是人类可读价格，直接 float()
        #   min_base_amount:          StrictStr         小数字符串如 "0.001"
        #   min_quote_amount:         StrictStr         最小 quote 金额，字符串
        #   supported_price_decimals: int               wire 价格小数位数
        #   supported_size_decimals:  int               wire 数量小数位数
        #   quote_multiplier:         int               quote wire 换算倍数
        market_detail  = await fetch_market_detail(order_api, cfg.market_id)
        symbol         = market_detail.symbol
        price_decimals = int(market_detail.supported_price_decimals)
        size_decimals  = int(market_detail.supported_size_decimals)
        current_price  = float(market_detail.last_trade_price)
        min_base_amount  = float(str(market_detail.min_base_amount))
        min_quote_amount = float(str(market_detail.min_quote_amount))
        quote_multiplier = int(market_detail.quote_multiplier)

        # 打印完整市场信息，方便诊断下单失败问题
        LOGGER.info(
            "[market] symbol=%s market_id=%s price_decimals=%s size_decimals=%s quote_multiplier=%s min_base_amount=%s min_quote_amount=%s last_price=%s",
            symbol, cfg.market_id, price_decimals, size_decimals, quote_multiplier,
            min_base_amount, min_quote_amount, current_price,
        )

        # 设置杠杆（配置超过市场上限时，自动降到允许的最大值）
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

            LOGGER.info("Setting leverage to %sx ...", cfg.leverage)
            tx_info, api_response, err = await client.update_leverage(
                market_index=cfg.market_id, margin_mode=1, leverage=cfg.leverage)
            if err:
                LOGGER.warning("set leverage failed: %s", err)
            else:
                LOGGER.info("[leverage] updated tx_info=%s response=%s", tx_info, api_response)
        elif cfg.leverage <= 0:
            LOGGER.warning("Configured leverage=%s is invalid; fallback to 1x.", cfg.leverage)
            cfg.leverage = 1

        base_amount = cfg.base_amount
        if base_amount <= 0:
            # 先满足 min_base_amount
            base_amount = max(1, size_to_wire(min_base_amount, size_decimals))
            # 再检查对应的 quote_amount 是否满足 min_quote_amount
            # quote_wire = base_amount * price_wire / quote_multiplier
            # 这里用当前价估算（price_wire = price * 10^price_decimals）
            price_wire_now  = price_to_wire(current_price, price_decimals)
            quote_wire      = base_amount * price_wire_now // quote_multiplier
            min_quote_wire  = int(round(min_quote_amount * (10 ** price_decimals)))
            if quote_wire < min_quote_wire and price_wire_now > 0:
                # 向上调整 base_amount 直到 quote_wire >= min_quote_wire
                base_amount = (min_quote_wire * quote_multiplier + price_wire_now - 1) // price_wire_now
            LOGGER.info(
                "[base_amount] auto=%s quote_wire_est=%s min_quote_wire=%s",
                base_amount,
                base_amount * price_wire_now // quote_multiplier,
                min_quote_wire,
            )

        # ── Auth token ───────────────────────────────────
        auth_mgr = AuthTokenManager(client, ttl_sec=3600)

        # ── 加载 / 重建状态 ──────────────────────────────
        LOGGER.info("Loading strategy state: %s", state_path)
        state = GridState.load(state_path, cfg.start_order_index)

        if state is not None:
            LOGGER.info("State loaded: %s", state.summary())
            LOGGER.info("Will verify against exchange on first cycle ...")
        else:
            LOGGER.info("No state file. Rebuilding from exchange active orders ...")
            auth_token    = await auth_mgr.get()
            active_orders = await fetch_active_orders(
                order_api, account_index, cfg.market_id, auth_token)
            LOGGER.info("Found %s active orders on exchange.", len(active_orders))
            state = build_state_from_exchange(
                active_orders, cfg.price_step, price_decimals, cfg.start_order_index, cfg.side)

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

        # ── 主循环 ────────────────────────────────────────
        cycle = 0
        while cfg.max_cycles == 0 or cycle < cfg.max_cycles:
            await asyncio.sleep(cfg.poll_interval_sec)

            # last_trade_price 已是 float/int，无需 wire 换算
            market_detail = await fetch_market_detail(order_api, cfg.market_id)
            current_price = float(market_detail.last_trade_price)

            LOGGER.info("cycle=%s price=%.4f %s", cycle, current_price, state.summary())

            await run_one_cycle(
                monitor=monitor,
                account_api=account_api,
                client=client,
                order_api=order_api,
                state=state,
                cfg=cfg,
                current_price=current_price,
                price_decimals=price_decimals,
                base_amount=base_amount,
                account_index=account_index,
                auth_mgr=auth_mgr,
                state_path=state_path,
            )
            cycle += 1

    finally:
        trades = state.success_count if state is not None else 0
        LOGGER.info("Exiting. Completed trades: %s", trades)
        LOGGER.info("Active orders remain on exchange (no cancellation on exit).")
        if state is not None and not cfg.dry_run:
            state.save(state_path)
            LOGGER.info("State saved: %s", state_path)
        for c, name in [(client, "SignerClient"), (api_client, "ApiClient")]:
            try:
                await c.close()
            except Exception as e:
                LOGGER.warning("Error closing %s: %s", name, e)


if __name__ == "__main__":
    try:
        asyncio.run(run_strategy(parse_args()))
    except KeyboardInterrupt:
        LOGGER.info("Shutdown (Ctrl+C). Exiting gracefully...")
    except Exception as e:
        LOGGER.exception("Fatal error: %s", e)
        raise

