import json
from pathlib import Path
from typing import Any, Dict, Tuple

from examples.grid_strategy.models import GridConfig, SIDE_LONG, SIDE_SHORT


GRID_STRATEGY_DIR = Path(__file__).resolve().parent


def normalize_side(side: str) -> str:
    side_norm = str(side).strip().lower()
    if side_norm not in {SIDE_LONG, SIDE_SHORT}:
        raise ValueError(f"side must be '{SIDE_LONG}' or '{SIDE_SHORT}', got: {side}")
    return side_norm


def default_grid_config() -> GridConfig:
    return GridConfig(
        market_symbol="0",
        market_id=0,
        levels=10,
        price_step=10.0,
        base_amount=0,
        side=SIDE_LONG,
        poll_interval_sec=5.0,
        max_cycles=0,
        start_order_index=10000,
        dry_run=False,
    )


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


def load_grid_config(resolved_config_file: str) -> GridConfig:
    cfg = default_grid_config()
    file_cfg = read_strategy_overrides(resolved_config_file)

    if file_cfg.get("marketId") is not None:
        cfg.market_symbol = str(file_cfg["marketId"]).strip()

    for attr, key, conv in [
        ("levels",            "levels",          int),
        ("price_step",        "priceStep",        float),
        ("leverage",          "leverage",         int),
        ("base_amount",       "baseAmount",       int),
        ("poll_interval_sec", "pollIntervalSec",  float),
    ]:
        if file_cfg.get(key) is not None:
            setattr(cfg, attr, conv(file_cfg[key]))

    if file_cfg.get("side") is not None:
        cfg.side = normalize_side(file_cfg["side"])
    return cfg


def load_api_key_config() -> Tuple[str, int, Dict[int, str], str]:
    candidates = [
        Path.cwd() / "api_key_config.json",
        GRID_STRATEGY_DIR / "api_key_config.json",
    ]
    p = next((candidate.resolve() for candidate in candidates if candidate.exists()), None)
    if p is None:
        searched = ", ".join(str(candidate) for candidate in candidates)
        raise FileNotFoundError(f"api_key_config.json not found. searched: {searched}")
    with p.open("r", encoding="utf-8") as f:
        cfg = json.load(f)
    private_keys = {int(k): v for k, v in cfg["privateKeys"].items()}
    return cfg["baseUrl"], int(cfg["accountIndex"]), private_keys, str(p)
