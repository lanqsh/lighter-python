# Grid Strategy 示例

本目录提供 Lighter Python SDK 的网格策略实现。

| 文件 | 说明 |
|------|------|
| `smart_grid_strategy.py` | 主策略入口，推荐用于实盘运行。 |
| `__main__.py` | 包级入口，执行 `smart_grid_strategy.main()`。 |
| `api_key_config.example.json` | 配置文件模板。 |
| `market_utils.py` | 按市场选择器（marketId 或 symbol 前缀）查询市场信息与精度。 |
| `config.py` | 读取并校验 `api_key_config.json`。 |

---

## smart_grid_strategy.py

### 功能特性

- 单向网格：通过 `side=long/short` 控制方向
- 自动止盈：开仓成交后自动挂 TP
- TP 容量治理：同方向 TP 达到上限（`levels`）时优先替换更远 TP
- TP 自恢复：TP 下单失败或出现“有仓位无 TP”时自动补挂
- 远端挂单清理：清理有效价格带之外的陈旧 entry/TP
- 无状态运行：不读取/保存状态文件
- 启动/退出清挂单：仅取消当前 market 的挂单，不动现有仓位
- 杠杆保护：自动按市场上限约束 leverage
- 下单量兜底：`baseAmount=0` 时自动计算合法最小下单量
- 成交确认：结合成交记录与仓位变化双重确认
- 完整日志：策略日志 + 下单生命周期日志
- Bark 日报：配置 `barkServer` 后，上海时间每天 08:00 推送
- 429 限流自适应：
  - 限流阶段首次 429：`pollIntervalSec *= 1.5`
  - 上限：请求间隔不超过初始配置值的 `3x`
  - 冷却：429 后固定等待 60 秒再继续
  - 持续限流：不重复乘系数，只继续 60 秒冷却
  - 不自动恢复旧速率（成功后保持当前间隔）

## 快速开始

```bash
apt update
apt install -y python3-venv python3-pip
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -U pip setuptools wheel
python -m pip install -r requirements.txt
python -m pip install -e .
```

复制配置模板：

```bash
cp examples/grid_strategy/api_key_config.example.json \
   examples/grid_strategy/api_key_config.json
# 编辑 api_key_config.json，填入真实 baseUrl / accountIndex / privateKeys
```

运行策略：

```bash
python -m examples.grid_strategy.smart_grid_strategy
```

或使用包入口：

```bash
python -m examples.grid_strategy
```

## 配置文件

所有运行参数来自 `api_key_config.json`。
配置文件查找顺序：

1. 当前工作目录
2. `examples/grid_strategy/`

日志输出到当前工作目录下的 `logs/`。

配置示例：

```json
{
  "baseUrl": "https://mainnet.zklighter.elliot.ai",
  "accountIndex": 123,
  "barkServer": "https://api.day.app/your_device_key",
  "privateKeys": {
    "0": "0xyour_api_private_key_hex"
  },
  "grid": {
    "marketId": "ETH",
    "side": "long",
    "levels": 5,
    "priceStep": 10,
    "baseAmount": 0,
    "leverage": 3,
    "pollIntervalSec": 5,
    "tpRefillMinSteps": 3,
    "tpRefillMaxSteps": 0
  }
}
```

### 支持的配置项

| 配置项 | 默认值 | 说明 |
|---|---|---|
| `barkServer` | `""` | Bark 推送地址（顶层字段，不在 `grid` 内） |
| `marketId` | `"0"` | 市场选择器：marketId 或 symbol 前缀（如 `"ETH"`） |
| `side` | `long` | 网格方向：`long` 或 `short` |
| `levels` | `10` | 网格层数 |
| `priceStep` | `10.0` | 相邻网格价差（human 单位） |
| `baseAmount` | `0` | 下单数量（wire 整数）；`0` 表示自动计算最小合法值 |
| `leverage` | `1` | 杠杆倍数；超过市场上限会自动降级 |
| `pollIntervalSec` | `5.0` | 基础轮询间隔（秒） |
| `tpRefillMinSteps` | `3` | TP 重挂最小距离（单位：格） |
| `tpRefillMaxSteps` | `0` | TP 重挂最大距离（单位：格），`0` 表示策略自动上限 |

说明：当前版本不会从配置文件读取 `dryRun`。

## Bark 日报

- 触发时间：Asia/Shanghai 每天 08:00
- 触发条件：配置了 `barkServer` 且当天未发送
- 推送字段：`symbol`、`side`、`position`、`today_tp`、`liq_price`、`total_balance`、`available_balance`、`price`

## baseAmount 说明

`baseAmount` 是 wire 整数，不是小数数量：

```text
baseAmount = 目标数量 * 10^size_decimals
```

示例：若 `size_decimals=4`，下单 `0.006 ETH`，则 `baseAmount=60`。

当 `baseAmount=0` 时，策略会按最深网格价格自动计算满足最小名义金额的合法下单量。

## 日志与运行行为

| 文件 | 说明 |
|------|------|
| `logs/smart_grid_market{id}_{side}.log` | 滚动日志（10MB x 5 份） |

运行行为：

- 启动：取消当前 market 全部挂单，不处理已有仓位
- 运行：按配置维护单向网格
- 退出：再次取消当前 market 全部挂单，不处理已有仓位

TP 成交日志示例：

```text
[trade:slot-closed] side=LONG total_tp=12 today_tp=3(2026-04-01) entry=2070.0000 tp=2080.0000
```

## 风险提示

- 先核对配置参数再运行
- 建议先在测试环境验证稳定性
- 本策略仅为示例，不构成投资建议
