# Grid Strategy Example

This folder contains a simple grid strategy for the Lighter Python SDK.

## What It Does

- Reads your API config from `api_key_config.json`
- Supports `grid` overrides inside `api_key_config.json`
- Fetches market `last_trade_price` as the grid anchor
- Places symmetric limit BUY/SELL orders around the anchor with fixed price step
- Long grid and short grid run at the same time:
  - Long grid side: BUY below anchor
  - Short grid side: SELL above anchor
- Rebalances by canceling and recreating all grid orders when price moves beyond a threshold
- On restart, cancels all active orders for current market before running (does not close positions)
- Cancels strategy orders on exit

## File

- `simple_grid_strategy.py`: runnable strategy script
- `api_key_config.example.json`: ETH example config

## Run

From repository root:

```bash
python examples/grid_strategy/simple_grid_strategy.py --dry-run
```

Live trading (remove `--dry-run`):

```bash
python examples/grid_strategy/simple_grid_strategy.py \
  --market-id 0 \
  --levels 4 \
  --price-step 5 \
  --rebalance-threshold 10 \
  --base-amount 0 \
  --poll-interval-sec 5
```

Or use config overrides in `api_key_config.json`:

```json
{
  "baseUrl": "https://testnet.zklighter.elliot.ai",
  "accountIndex": 123,
  "privateKeys": {
    "0": "0xyour_api_private_key_hex"
  },
  "grid": {
    "marketId": 0,
    "levels": 4,
    "priceStep": 5,
    "rebalanceThreshold": 10,
    "baseAmount": 0,
    "clearOnStart": true
  }
}
```

`baseAmount = 0` means: use exchange minimum size (`min_base_amount`) automatically.

## Important Parameters

- `--market-id`: market identifier
- `--levels`: number of levels on each side of anchor
- `--price-step`: absolute price spacing between adjacent grid orders
- `--rebalance-threshold`: absolute anchor move to trigger grid rebuild
- `--base-amount`: base asset size in SDK native units, `0` means auto minimum size
- `--clear-on-start`: cancel all active orders in this market at startup
- `--no-clear-on-start`: disable startup cancel behavior
- `--start-order-index`: first order index used by strategy

## Safety Notes

- Start with `--dry-run` to verify behavior.
- Use testnet first.
- This is an example strategy, not investment advice.
