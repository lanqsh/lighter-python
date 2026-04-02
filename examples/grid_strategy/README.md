# Grid Strategy Example

This folder contains two grid strategy implementations for the Lighter Python SDK.

| 文件 | 说明 |
|------|------|
| `simple_grid_strategy.py` | 基础网格策略（双向，无状态持久化） |
| `smart_grid_strategy.py` | **推荐** 智能单向网格策略（无状态新网格、自动止盈、日志监控） |
| `api_key_config.example.json` | 配置文件示例 |
| `query_doge_market.py` | 查询指定市场的 marketId、精度、最小下单量等信息 |

---

## smart_grid_strategy.py

### 功能特性

- **单向网格**：通过 `side=long/short` 配置，只做多或只做空，适配 DEX 单向持仓模式
- **自动止盈**：开仓单成交后自动在 `entry+price_step` 挂止盈单
- **无状态新网格**：每次启动都视为新的网格会话，不读取历史状态文件
- **启动/退出自动清挂单**：仅取消当前交易对挂单，保留已有仓位不动
- **全仓模式**：设置杠杆时自动使用全仓（cross margin）
- **最小下单量兜底**：`baseAmount=0` 时按最深网格价自动计算满足交易所最小名义金额的数量；配置值偏小时自动抬升并打印警告
- **杠杆上限保护**：超过市场允许的最大杠杆时自动降至上限
- **成交确认**：通过成交记录和仓位变化双重证据确认开仓/止盈是否真实成交
- **日志监控**：滚动文件日志 + 控制台双输出，记录代码行号、下单请求/响应、仓位变化、成交情况、每格生命周期
- **TP 统计**：每次止盈成交后打印 `total_tp`（累计总次数）和 `today_tp`（当日次数，每天自动归零）

### 快速开始

将示例配置复制为实际配置：

```bash
cp examples/grid_strategy/api_key_config.example.json \
   examples/grid_strategy/api_key_config.json
# 编辑 api_key_config.json，填入真实的 baseUrl / accountIndex / privateKeys
```

模拟运行（不下真实订单）：

```bash
cd examples/grid_strategy
python smart_grid_strategy.py --dry-run
```

实盘运行（先用测试网验证）：

```bash
cd examples/grid_strategy
python smart_grid_strategy.py
```

> 策略运行目录必须包含 `api_key_config.json`，日志会写入该目录下的 `logs/`。

### 配置文件

在 `api_key_config.json` 的 `grid` 字段中覆盖策略参数：

```json
{
  "baseUrl": "https://mainnet.zklighter.elliot.ai",
  "accountIndex": 123,
  "privateKeys": {
    "0": "0xyour_api_private_key_hex"
  },
  "grid": {
    "marketId": 0,
    "side": "long",
    "levels": 5,
    "priceStep": 10,
    "baseAmount": 0,
    "leverage": 3
  }
}
```

### 参数说明

| 参数（命令行） | 配置文件键 | 默认值 | 说明 |
|---|---|---|---|
| `--market-id` | `marketId` | `0` | 市场 ID（可用 `query_doge_market.py` 查询） |
| `--side` | `side` | `long` | 仓位方向：`long` 或 `short` |
| `--levels` | `levels` | `10` | 网格层数 |
| `--price-step` | `priceStep` | `10.0` | 相邻格子价差（human units，如 ETH 填 `10` 表示 $10） |
| `--base-amount` | `baseAmount` | `0` | 每格下单数量（wire 整数）。`0` = 按最深网格价自动计算最小合法数量 |
| `--leverage` | `leverage` | `1` | 杠杆倍数；超过市场上限自动降至上限 |
| `--poll-interval-sec` | — | `5.0` | 每轮轮询间隔（秒） |
| `--max-cycles` | — | `0` | 最大循环次数，`0` = 永久运行 |
| `--start-order-index` | — | `200000` | 策略使用的起始 client_order_index |
| `--dry-run` | — | `false` | 模拟运行，不提交真实订单 |

### baseAmount 填写说明

`baseAmount` 填写的是 **wire 整数**（非小数），换算公式：

```
baseAmount = 目标数量 × 10^size_decimals
```

例如 ETH `size_decimals=4`，下单 0.006 ETH → `baseAmount=60`

设为 `0` 时，策略会自动根据最深网格价和交易所 `min_quote_amount` 计算出所有档位都合法的最小数量。

### 日志与运行行为

| 文件 | 说明 |
|------|------|
| `logs/smart_grid_market{id}_{side}.log` | 滚动日志（最大 10MB × 5 个备份） |

运行行为：
- 启动时：取消当前交易对全部挂单，不处理已有仓位
- 运行中：按当前参数创建新的单向网格
- 退出时：再次取消当前交易对全部挂单，不处理已有仓位

TP 成交日志示例：
```
[trade:slot-closed] side=LONG total_tp=12 today_tp=3(2026-04-01) entry=2070.0000 tp=2080.0000
```

### 安全建议

- 先用 `--dry-run` 验证参数和行为
- 先在测试网（testnet）运行稳定后再切主网
- 本策略仅为示例，不构成投资建议
