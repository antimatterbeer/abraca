# abraca

abraca 是一个市场数据聚合器，从多个交易所获取市场数据，通过统一的 TCP + JSON 协议聚合并推送给客户端。

## 功能概览

- **多交易所**：按需连接交易所（当前已实现币安 U 本位期货 BinanceFutures）。
- **TCP 服务**：客户端通过 TCP 连接，按行发送 JSON 请求、按行接收 JSON 响应与推送。
- **订阅/取消订阅**：支持按主题订阅实时数据，服务端按订阅关系向对应客户端推送。
- **请求/响应**：支持带 `id` 的请求（如获取交易对信息），响应通过同一连接按 `id` 回传。

## 运行方式

```bash
cargo run -- --port 8080
```

服务默认监听 `0.0.0.0:8080`。日志同时输出到控制台和 `./logs` 目录（按天滚动），可通过环境变量 `RUST_LOG` 调整级别（如 `RUST_LOG=debug`）。

## 协议说明

### 连接与消息格式

- 连接：TCP 连接到服务端（默认 `127.0.0.1:8080`）。
- 请求：每行一条 JSON，以 `\n` 结尾。
- 响应/推送：服务端每行一条 JSON，以 `\n` 结尾。

### 请求结构（客户端 → 服务端）

所有请求均为 JSON 对象，且包含：

| 字段       | 类型   | 说明                                              |
| ---------- | ------ | ------------------------------------------------- |
| `exchange` | string | 交易所，如 `"BinanceFutures"`                     |
| `id`       | number | 请求 ID，用于匹配响应（订阅/取消订阅也会占用 ID） |
| `req`      | string | 请求类型，见下                                    |
| `data`     | 见下表 | 与 `req` 对应的载荷                               |

| `req`             | 说明           | `data` 类型      |
| ----------------- | -------------- | ---------------- |
| `subscribe`       | 订阅主题       | 字符串数组       |
| `unsubscribe`     | 取消订阅       | 字符串数组       |
| `get_symbol_info` | 获取交易对信息 | 交易对字符串数组 |

**主题格式（BinanceFutures）**：`<symbol>@<数据类型>`，例如：

- `BTCUSDT@Kline` — 1 分钟 K 线
- `BTCUSDT@Depth` — 10 档深度（500ms 推送）
- `BTCUSDT@BestPrice` — 最优买卖价（bookTicker）
- `BTCUSDT@MarkPrice` — 标记价格（1s 推送）
- `BTCUSDT@ForceOrder` — 强平订单（1s 推送）

### 响应结构（服务端 → 客户端）

每条下行 JSON 均包含：

| 字段        | 类型           | 说明                             |
| ----------- | -------------- | -------------------------------- |
| `exchange`  | string         | 交易所                           |
| `timestamp` | number         | 服务端时间戳（毫秒）             |
| `id`        | number \| null | 若有则对应请求 ID；推送为 `null` |
| `data_type` | string         | 数据类型，见下                   |
| `data`      | object         | 具体数据                         |

| `data_type`    | 说明                                       |
| -------------- | ------------------------------------------ |
| `error`        | 错误信息（字符串）                         |
| `kline`        | K 线                                       |
| `depth`        | 深度                                       |
| `best_price`   | 最优挂单                                   |
| `mark_price`   | 标记价格                                   |
| `force_order`  | 强平订单                                   |
| `symbol_infos` | 交易对信息列表（`get_symbol_info` 的响应） |


### 数据类型说明

以下为 `data` 对象在各 `data_type` 下的字段。未列出的类型（如 `error`、`symbol_infos`）含义见上表。

#### `kline` — K 线（1 分钟）

| 字段           | 类型   | 说明     |
| -------------- | ------ | -------- |
| `symbol`       | string | 交易对   |
| `timestamp`    | number | 时间戳   |
| `open`         | number | 开盘价   |
| `high`         | number | 最高价   |
| `low`          | number | 最低价   |
| `close`        | number | 收盘价   |
| `volume`       | number | 成交量   |
| `quote_volume` | number | 成交额   |

#### `depth` — 深度（10 档，约 500ms 推送）

| 字段        | 类型  | 说明                         |
| ----------- | ----- | ---------------------------- |
| `symbol`    | string | 交易对                       |
| `timestamp` | number | 时间戳                       |
| `bids`      | array  | 买单，每项为 `[价格, 数量]`  |
| `asks`      | array  | 卖单，每项为 `[价格, 数量]`  |

#### `best_price` — 最优挂单（bookTicker）

| 字段         | 类型   | 说明   |
| ------------ | ------ | ------ |
| `symbol`     | string | 交易对 |
| `timestamp`  | number | 时间戳 |
| `bid_price`  | number | 买一价 |
| `bid_volume` | number | 买一量 |
| `ask_price`  | number | 卖一价 |
| `ask_volume` | number | 卖一量 |

#### `mark_price` — 标记价格（约 1s 推送）

| 字段                     | 类型   | 说明               |
| ------------------------ | ------ | ------------------ |
| `symbol`                 | string | 交易对             |
| `timestamp`              | number | 时间戳             |
| `mark_price`             | number | 标记价格           |
| `index_price`            | number | 指数价格           |
| `estimated_settle_price` | number | 预估结算价格       |
| `funding_rate`           | number | 资金费率           |
| `next_funding_time`      | number | 下次资金费率时间   |

#### `force_order` — 强平订单

| 字段                   | 类型   | 说明           |
| ---------------------- | ------ | -------------- |
| `symbol`               | string | 交易对         |
| `timestamp`            | number | 时间戳         |
| `side`                 | string | 方向 Buy/Sell  |
| `order_type`           | string | 订单类型       |
| `time_in_force`        | string | 订单有效期     |
| `quantity`             | number | 数量           |
| `price`                | number | 价格           |
| `average_price`        | number | 平均成交价     |
| `status`               | string | 订单状态       |
| `last_filled_quantity` | number | 最后成交数量   |
| `filled_quantity`      | number | 累计成交数量   |


## 客户端示例

项目内提供 Python 示例脚本，使用 asyncio TCP 连接并订阅/拉取数据：

```bash
# 需安装 loguru：pip install loguru
python scripts/client.py
```

示例会：

1. 连接 `127.0.0.1:8080`
2. 请求 `BinanceFutures` 的 `BTCUSDT`、`ETHUSDT` 交易对信息
3. 订阅 `ETHUSDT@ForceOrder`
4. 循环接收并打印服务端下发的 JSON（含请求响应与推送）

可参考 `scripts/client.py` 中的 `send`、`get_symbol_info`、`subscribe`、`unsubscribe`、`recv` 实现自己的客户端。

## 项目结构（简要）

| 路径                            | 说明                                                                        |
| ------------------------------- | --------------------------------------------------------------------------- |
| `src/main.rs`                   | 入口：初始化日志、创建 `Abraca`、在 8080 端口运行                           |
| `src/abraca.rs`                 | 核心服务：TCP 监听、连接管理、请求路由、订阅与推送                          |
| `src/event.rs`                  | 请求/响应类型：`MarketReq`、`MarketRsp`、`MarketReqData`、`MarketRspData`   |
| `src/msg.rs`                    | 业务数据结构：Kline、Depth、BestPrice、MarkPrice、ForceOrder、SymbolInfo 等 |
| `src/def.rs`                    | 枚举与通用定义：Exchange、ContractType、OrderSide 等                        |
| `src/market/mod.rs`             | 市场模块入口：按交易所启动对应 market gateway                               |
| `src/market/binance_futures.rs` | 币安 U 本位期货：REST 拉取交易对信息、WebSocket 订阅与解析、向 abraca 回写  |

## 依赖与构建

- Rust 工具链（edition 2024）。
- 主要依赖：`tokio`、`tokio-tungstenite`、`reqwest`、`serde`/`serde_json`、`tracing`、`dashmap` 等。

```bash
cargo build --release
```

生成的可执行文件可直接用于生产环境（注意端口与日志目录权限）。
