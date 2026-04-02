from dataclasses import dataclass, field, asdict
from typing import Dict, Optional, Set, Any

SLOT_IDLE   = "IDLE"
SLOT_NEW    = "NEW"
SLOT_FILLED = "FILLED"

ACTIVE_STATUSES = {"open", "in-progress", "pending"}
SIDE_LONG  = "long"
SIDE_SHORT = "short"
RETRYABLE_HTTP_STATUS = {408, 429, 500, 502, 503, 504}


@dataclass
class GridSlot:
    place_price:     float
    tp_price:        float
    is_long:         bool
    status:          str = SLOT_IDLE
    place_order_idx: int = 0
    tp_order_idx:    int = 0

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "GridSlot":
        return cls(**d)


@dataclass
class PositionSnapshot:
    market_id:          int
    symbol:             str
    sign:               int
    position:           float
    avg_entry_price:    float
    unrealized_pnl:     float
    realized_pnl:       float
    open_order_count:   int
    pending_order_count: int


@dataclass
class OrderLifecycle:
    client_order_index: int
    label:       str
    event:       str
    is_ask:      bool
    reduce_only: bool
    slot_side:   str   = ""
    slot_kind:   str   = ""
    entry_price: float = 0.0
    tp_price:    float = 0.0
    tx_hash:     str   = ""
    error:       str   = ""


@dataclass
class RuntimeMonitor:
    last_position:           Optional[PositionSnapshot] = None
    seen_trade_ids:          Set[int] = field(default_factory=set)
    recent_trade_client_ids: Set[int] = field(default_factory=set)
    order_lifecycles:        Dict[int, OrderLifecycle] = field(default_factory=dict)
    order_submit_times:      Dict[int, str] = field(default_factory=dict)


@dataclass
class TradeEvidence:
    new_trade_client_ids: Set[int] = field(default_factory=set)
    position_before:      Optional[PositionSnapshot] = None
    position_after:       Optional[PositionSnapshot] = None


class GridState:
    def __init__(self, start_order_index: int):
        self.long_slots:     Dict[str, GridSlot] = {}
        self.short_slots:    Dict[str, GridSlot] = {}
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
        return (
            f"long(new={ln} filled={lf}) "
            f"short(new={sn} filled={sf}) "
            f"next_idx={self.next_order_idx} trades={self.success_count}"
        )


@dataclass
class GridConfig:
    market_symbol:     str
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
    tp_refill_min_steps: int = 3
    tp_refill_max_steps: int = 0
    tp_refill_max_steps: int = 0


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
