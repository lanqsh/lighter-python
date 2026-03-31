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
import sys
import time
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
            print(f"[warn] Failed to load state file ({e}), will rebuild from exchange.")
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
        poll_interval_sec=args.poll_interval_sec,
        max_cycles=args.max_cycles,
        start_order_index=args.start_order_index,
        dry_run=args.dry_run,
        config_file=args.config_file,
    )


# ════════════════════════════════════════════════════════════
#  共用工具函数
# ════════════════════════════════════════════════════════════
def load_api_key_config(config_file: str) -> Tuple[str, int, Dict[int, str]]:
    candidates: List[Path] = []
    if config_file:
        candidates.append(Path(config_file).expanduser().resolve())
    candidates.append(Path.cwd() / "api_key_config.json")
    candidates.append(EXAMPLES_DIR / "api_key_config.json")
    candidates.append(ROOT_DIR / "api_key_config.json")
    for c in candidates:
        if c.exists():
            with c.open("r", encoding="utf-8") as f:
                cfg = json.load(f)
            private_keys = {int(k): v for k, v in cfg["privateKeys"].items()}
            return cfg["baseUrl"], int(cfg["accountIndex"]), private_keys
    raise FileNotFoundError("api_key_config.json not found")


def read_strategy_overrides(config_file: str) -> Dict[str, Any]:
    if not config_file:
        return {}
    p = Path(config_file).expanduser().resolve()
    if not p.exists():
        return {}
    with p.open("r", encoding="utf-8") as f:
        cfg = json.load(f)
    v = cfg.get("grid", {})
    return v if isinstance(v, dict) else {}


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


def state_file_path(config_file: str, market_id: int) -> Path:
    """状态文件路径：与配置文件同目录，或 cwd"""
    if config_file:
        base = Path(config_file).expanduser().resolve().parent
    else:
        base = Path.cwd()
    return base / f"grid_state_market{market_id}.json"


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
            print(f"[auth] token refreshed (valid {self._ttl}s)")
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

    for o in active_orders:
        coi         = int(o.client_order_index)
        # Order.price 是 wire 格式字符串，必须换算为人类可读价格
        price       = wire_price_to_float(o.price, price_decimals)
        is_ask      = bool(o.is_ask)
        reduce_only = bool(o.reduce_only)
        max_idx     = max(max_idx, coi)

        if not is_ask and not reduce_only:
            # 多方开仓 BUY open
            k    = GridState.price_key(price)
            slot = state.long_slots.setdefault(
                k, GridSlot(place_price=price, tp_price=price + price_step, is_long=True))
            slot.status          = SLOT_NEW
            slot.place_order_idx = coi
            print(f"  [rebuild] LONG  entry @{price:.4f}  coi={coi}")

        elif is_ask and reduce_only:
            # 多方止盈 SELL reduce_only，对应开仓价 = tp_price - price_step
            entry_price = price - price_step
            k    = GridState.price_key(entry_price)
            slot = state.long_slots.setdefault(
                k, GridSlot(place_price=entry_price, tp_price=price, is_long=True))
            slot.status       = SLOT_FILLED
            slot.tp_order_idx = coi
            slot.tp_price     = price
            print(f"  [rebuild] LONG  tp    @{price:.4f}  (entry={entry_price:.4f})  coi={coi}")

        elif is_ask and not reduce_only:
            # 空方开仓 SELL open
            k    = GridState.price_key(price)
            slot = state.short_slots.setdefault(
                k, GridSlot(place_price=price, tp_price=price - price_step, is_long=False))
            slot.status          = SLOT_NEW
            slot.place_order_idx = coi
            print(f"  [rebuild] SHORT entry @{price:.4f}  coi={coi}")

        elif not is_ask and reduce_only:
            # 空方止盈 BUY reduce_only，对应开仓价 = tp_price + price_step
            entry_price = price + price_step
            k    = GridState.price_key(entry_price)
            slot = state.short_slots.setdefault(
                k, GridSlot(place_price=entry_price, tp_price=price, is_long=False))
            slot.status       = SLOT_FILLED
            slot.tp_order_idx = coi
            slot.tp_price     = price
            print(f"  [rebuild] SHORT tp    @{price:.4f}  (entry={entry_price:.4f})  coi={coi}")

    state.next_order_idx = max_idx + 1
    return state


# ════════════════════════════════════════════════════════════
#  下单 / 撤单封装
# ════════════════════════════════════════════════════════════
async def do_place_order(
    client:      lighter.SignerClient,
    market_id:   int,
    order_idx:   int,
    base_amount: int,
    wire_price:  int,
    is_ask:      bool,
    reduce_only: bool,
    dry_run:     bool,
    label:       str,
) -> bool:
    print(f"[order] {label}  coi={order_idx}  market={market_id}  "
          f"base_amount={base_amount}  price_wire={wire_price}  "
          f"is_ask={is_ask}  reduce_only={reduce_only}")
    if dry_run:
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
        print(f"[warn] {label} FAILED  coi={order_idx}  err={err}")
        return False
    print(f"[ok]   {label}  coi={order_idx}  tx={tx_hash}")
    return True


async def do_cancel_order(
    client:    lighter.SignerClient,
    market_id: int,
    order_idx: int,
    dry_run:   bool,
    label:     str,
) -> None:
    if dry_run:
        print(f"[DRY] cancel {label}  coi={order_idx}")
        return
    _, tx_hash, err = await client.cancel_order(
        market_index=market_id, order_index=order_idx)
    print(f"cancel {label}  coi={order_idx}  tx={tx_hash}  err={err}")


# ════════════════════════════════════════════════════════════
#  单轮主循环  ——  对应 C++ RunGrid()
# ════════════════════════════════════════════════════════════
async def run_one_cycle(
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
    # ──────────────────────────────────────────────────────
    # 0. 获取交易所当前活跃订单
    #    active_set: client_order_index → Order
    #    Order.price 为 wire 格式字符串，本函数内通过 do_place_order 下单时转换
    # ──────────────────────────────────────────────────────
    auth_token    = await auth_mgr.get()
    active_orders = await fetch_active_orders(
        order_api, account_index, cfg.market_id, auth_token)
    active_set: Dict[int, Any] = {int(o.client_order_index): o for o in active_orders}

    aligned       = (int(current_price / cfg.price_step)) * cfg.price_step
    far_threshold = cfg.price_step * cfg.levels * 2

    # ──────────────────────────────────────────────────────
    # 1. DeleteLong/ShortPlaceOrders：撤销距当前价过远的开仓单
    # ──────────────────────────────────────────────────────
    for slot in list(state.long_slots.values()):
        if (slot.status == SLOT_NEW
                and slot.place_price < aligned - far_threshold
                and slot.place_order_idx in active_set):
            await do_cancel_order(
                client, cfg.market_id, slot.place_order_idx, cfg.dry_run,
                f"LONG entry(far) @{slot.place_price:.4f}")
            slot.status = SLOT_IDLE

    for slot in list(state.short_slots.values()):
        if (slot.status == SLOT_NEW
                and slot.place_price > aligned + far_threshold
                and slot.place_order_idx in active_set):
            await do_cancel_order(
                client, cfg.market_id, slot.place_order_idx, cfg.dry_run,
                f"SHORT entry(far) @{slot.place_price:.4f}")
            slot.status = SLOT_IDLE

    # ──────────────────────────────────────────────────────
    # 2a. CheckFilledOrders Step A：
    #     开仓单已不在活跃列表 → 视为成交 → 挂止盈单（reduce_only=True）
    # ──────────────────────────────────────────────────────
    all_slots = list(state.long_slots.values()) + list(state.short_slots.values())
    for slot in all_slots:
        if slot.status != SLOT_NEW:
            continue
        if slot.place_order_idx in active_set:
            continue  # 仍在挂单中

        tp_idx  = state.alloc_idx()
        tp_wire = price_to_wire(slot.tp_price, price_decimals)
        label   = f"{'LONG' if slot.is_long else 'SHORT'} TP @{slot.tp_price:.4f}"
        ok = await do_place_order(
            client, cfg.market_id, tp_idx, base_amount, tp_wire,
            is_ask=slot.is_long,     # 多方止盈=SELL(ask=True); 空方止盈=BUY(ask=False)
            reduce_only=True,
            dry_run=cfg.dry_run,
            label=label,
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
    all_slots = list(state.long_slots.values()) + list(state.short_slots.values())
    for slot in all_slots:
        if slot.status != SLOT_FILLED:
            continue
        if slot.tp_order_idx in active_set:
            continue  # 止盈单仍在挂单中

        slot.status = SLOT_IDLE
        state.success_count += 1
        print(f"TRADE {'LONG' if slot.is_long else 'SHORT'} "
              f"#{state.success_count}  "
              f"entry={slot.place_price:.4f} → tp={slot.tp_price:.4f}")

    # ──────────────────────────────────────────────────────
    # 3. MakeLong/ShortPlaceOrders：为 IDLE 格子补挂开仓单
    # ──────────────────────────────────────────────────────

    # 多方：BUY below current price
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
            client, cfg.market_id, place_idx, base_amount,
            price_to_wire(place_price, price_decimals),
            is_ask=False, reduce_only=False,
            dry_run=cfg.dry_run,
            label=f"LONG entry @{place_price:.4f}",
        )
        if ok:
            slot.place_order_idx = place_idx
            slot.status          = SLOT_NEW

    # 空方：SELL above current price
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
            client, cfg.market_id, place_idx, base_amount,
            price_to_wire(place_price, price_decimals),
            is_ask=True, reduce_only=False,
            dry_run=cfg.dry_run,
            label=f"SHORT entry @{place_price:.4f}",
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

    base_url, account_index, private_keys = load_api_key_config(cfg.config_file)
    file_cfg = read_strategy_overrides(cfg.config_file)

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

    # ── SDK 初始化 ───────────────────────────────────────
    configuration = lighter.Configuration(host=base_url)
    configuration.api_key = {"default": private_keys[min(private_keys.keys())]}
    api_client  = lighter.ApiClient(configuration=configuration)
    order_api   = lighter.OrderApi(api_client)
    client      = lighter.SignerClient(
        url=base_url,
        account_index=account_index,
        api_private_keys=private_keys,
    )

    state:      Optional[GridState] = None
    state_path: Path                = state_file_path(cfg.config_file, cfg.market_id)

    try:
        err = client.check_client()
        if err is not None:
            raise RuntimeError(f"check_client failed: {err}")

        # 设置杠杆
        if cfg.leverage > 1:
            print(f"Setting leverage to {cfg.leverage}x ...")
            _, err = await client.update_leverage(
                market_index=cfg.market_id, margin_mode=1, leverage=cfg.leverage)
            if err:
                print(f"[warn] set leverage failed: {err}")
            else:
                print(f"Leverage set to {cfg.leverage}x")

        # ── 市场信息 ─────────────────────────────────────
        # PerpsOrderBookDetail 关键字段：
        #   last_trade_price:        Union[float, int]  已是人类可读价格，直接 float() 即可
        #   min_base_amount:         StrictStr          小数字符串如 "0.001"，需 size_to_wire 换算
        #   supported_price_decimals: int               wire 价格小数位数
        #   supported_size_decimals:  int               wire 数量小数位数
        market_detail  = await fetch_market_detail(order_api, cfg.market_id)
        symbol         = market_detail.symbol
        price_decimals = int(market_detail.supported_price_decimals)
        size_decimals  = int(market_detail.supported_size_decimals)
        # last_trade_price 已是 float/int 人类可读价格，无需换算
        current_price  = float(market_detail.last_trade_price)
        # min_base_amount 是小数字符串（如 "0.001"），需通过 size_to_wire 转为 wire 整数
        min_base_amount = float(str(market_detail.min_base_amount))

        base_amount = cfg.base_amount
        if base_amount <= 0:
            base_amount = max(1, size_to_wire(min_base_amount, size_decimals))

        # ── Auth token ───────────────────────────────────
        auth_mgr = AuthTokenManager(client, ttl_sec=3600)

        # ── 加载 / 重建状态 ──────────────────────────────
        print(f"\nLoading strategy state: {state_path}")
        state = GridState.load(state_path, cfg.start_order_index)

        if state is not None:
            print(f"State loaded: {state.summary()}")
            print("Will verify against exchange on first cycle ...")
        else:
            print("No state file. Rebuilding from exchange active orders ...")
            auth_token    = await auth_mgr.get()
            active_orders = await fetch_active_orders(
                order_api, account_index, cfg.market_id, auth_token)
            print(f"Found {len(active_orders)} active orders on exchange.")
            state = build_state_from_exchange(
                active_orders, cfg.price_step, price_decimals, cfg.start_order_index)

        aligned = (int(current_price / cfg.price_step)) * cfg.price_step
        print(
            f"\nstart  symbol={symbol}  market_id={cfg.market_id}  "
            f"price={current_price:.4f}  aligned={aligned:.4f}\n"
            f"       price_step={cfg.price_step}  levels={cfg.levels}  "
            f"base_amount={base_amount}  leverage={cfg.leverage}x  "
            f"dry_run={cfg.dry_run}"
        )
        print(f"state: {state.summary()}\n")

        # ── 主循环 ────────────────────────────────────────
        cycle = 0
        while cfg.max_cycles == 0 or cycle < cfg.max_cycles:
            await asyncio.sleep(cfg.poll_interval_sec)

            # last_trade_price 已是 float/int，无需 wire 换算
            market_detail = await fetch_market_detail(order_api, cfg.market_id)
            current_price = float(market_detail.last_trade_price)

            print(f"cycle={cycle}  price={current_price:.4f}  {state.summary()}")

            await run_one_cycle(
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
        print(f"\nExiting. Completed trades: {trades}")
        print("Active orders remain on exchange (no cancellation on exit).")
        if state is not None and not cfg.dry_run:
            state.save(state_path)
            print(f"State saved: {state_path}")
        for c, name in [(client, "SignerClient"), (api_client, "ApiClient")]:
            try:
                await c.close()
            except Exception as e:
                print(f"Error closing {name}: {e}")


if __name__ == "__main__":
    try:
        asyncio.run(run_strategy(parse_args()))
    except KeyboardInterrupt:
        print("\nShutdown (Ctrl+C). Exiting gracefully...")
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()

