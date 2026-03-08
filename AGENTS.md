# Abraca 项目 - AI Agent 上下文指南

## 项目概述

**abraca** 是一个加密货币市场数据聚合服务（Market Data Service），用 Rust 编写。它从多个交易所获取实时市场数据，通过统一的 TCP + JSON 协议聚合并推送给客户端。

## 项目结构

这是一个 Rust workspace，包含两个 crate：

```
abraca/
├── Cargo.toml           # Workspace 配置
├── abraca-base/         # 基础库：类型定义和消息格式
│   └── src/
│       ├── lib.rs       # 模块导出和 prelude
│       ├── types.rs     # 核心数据类型（Kline, Depth, SymbolInfo 等）
│       ├── message.rs   # 请求/响应消息结构
│       └── utils.rs     # 日志初始化工具
├── abraca-mds/          # 市场数据服务主程序
│   └── src/
│       ├── main.rs      # 程序入口
│       ├── lib.rs       # 模块聚合
│       ├── server.rs    # TCP 服务器核心逻辑
│       ├── error.rs     # 错误类型定义
│       ├── orderbook.rs # 订单簿实现（含聚合逻辑）
│       ├── gateway.rs   # 交易所网关接口
│       └── gateway/binance_futures.rs  # 币安 U 本位期货网关
└── scripts/
    └── client.py        # Python 客户端示例
```

## 核心模块详解

### abraca-base（基础库）

#### types.rs
定义了所有市场数据类型：
- `Exchange`: 交易所枚举（BinanceSpot, BinanceFutures, BinanceFuturesCM）
- `Kline`: K线数据（开高低收、成交量等）
- `Depth`: 深度数据（买卖档位）
- `BestPrice`: 最优买卖价
- `MarkPrice`: 标记价格（含资金费率）
- `ForceOrder`: 强平订单
- `SymbolInfo`: 交易对详细信息
- 各种枚举：订单状态、订单类型、买卖方向、合约类型等

#### message.rs
定义通信协议：
- `Request<T>`: 客户端请求（含 exchange, id, data）
- `Response<T>`: 服务端响应（含 timestamp, exchange, identifier, result）
- `ResponseIdentifier`: 响应标识（Id 用于请求响应，Stream 用于数据流）
- `ResponseResult`: 结果类型（Data 或 Error）

### abraca-mds（市场数据服务）

#### server.rs
核心服务器实现：
- 监听 TCP 连接（默认 8080 端口）
- 管理客户端连接和订阅关系
- 路由请求到对应交易所网关
- 向订阅客户端推送数据流
- 支持订阅主题：`{symbol}@{Kline|Depth|BestPrice|MarkPrice|ForceOrder|AggDepth}`

#### gateway.rs
网关抽象层：
- `start_gateway()`: 按交易所启动对应网关
- `MdsStream`: 数据流类型枚举
  - `Kline(symbol)`: 1分钟K线
  - `Depth(symbol)`: 10档深度（500ms）
  - `BestPrice(symbol)`: 最优挂单
  - `MarkPrice(symbol)`: 标记价格
  - `ForceOrder(symbol)`: 强平订单
  - `AggDepth{symbol, decimal_places, depth}`: 聚合深度（自定义精度）

#### gateway/binance_futures.rs
币安 U 本位期货网关：
- WebSocket 连接到 `wss://fstream.binance.com/stream`
- REST API 获取交易对信息 `https://fapi.binance.com`
- 数据转换：币安原始格式 → 内部统一格式
- 订单簿管理：
  - 通过 REST 获取全量快照
  - WebSocket 接收增量更新
  - 支持价格聚合（AggDepth）功能

#### orderbook.rs
订单簿实现：
- 使用 `BTreeMap` 存储买卖档位（asks 升序，bids 降序）
- 价格使用 `i64` 编码（避免浮点精度问题）
- 支持价格聚合功能（按指定小数位聚合档位）
- 有效性检查（卖价 > 买价）
- 包含完整单元测试

## 通信协议

### 请求格式（客户端 → 服务端）
```json
{
  "exchange": "BinanceFutures",
  "id": 1,
  "req": "subscribe",
  "data": ["BTCUSDT@Kline", "BTCUSDT@Depth"]
}
```

请求类型：
- `subscribe`: 订阅主题
- `unsubscribe`: 取消订阅
- `get_symbol_info`: 获取交易对信息

### 响应格式（服务端 → 客户端）
```json
{
  "timestamp": 1234567890000,
  "exchange": "BinanceFutures",
  "id": 1,
  "data_type": "kline",
  "data": { ... }
}
```

数据类型：
- `kline`, `depth`, `best_price`, `mark_price`, `force_order`
- `symbol_infos`: 交易对信息列表
- `error`: 错误信息

## 关键特性

1. **按需连接**：网关根据客户端请求按需启动
2. **多客户端支持**：同一主题可被多个客户端订阅
3. **自动清理**：客户端断开时自动清理订阅关系
4. **订单簿聚合**：支持按自定义精度聚合深度档位
5. **订单簿同步**：快照 + 增量更新的可靠同步机制

## 运行方式

```bash
# 开发模式
cargo run -- --port 8080

# 生产构建
cargo build --release

# Python 客户端测试（需安装 loguru）
pip install loguru
python scripts/client.py
```

## 依赖说明

- `tokio`: 异步运行时
- `tokio-tungstenite`: WebSocket 客户端
- `reqwest`: HTTP 客户端
- `serde/serde_json`: 序列化
- `dashmap`: 并发哈希表
- `tracing`: 日志和追踪

## 扩展指南

### 添加新交易所

1. 在 `types.rs` 的 `Exchange` 枚举中添加新交易所
2. 在 `gateway.rs` 的 `start_gateway()` 中添加匹配分支
3. 创建新的网关模块（如 `gateway/okx.rs`）
4. 实现网关 trait（WebSocket 连接、数据转换）

### 添加新数据类型

1. 在 `types.rs` 中定义数据结构
2. 在 `gateway.rs` 的 `MdsStream` 中添加流类型
3. 在 `RspData` 中添加对应变体
4. 在网关实现中添加数据解析逻辑

## 注意事项

- 所有价格使用 `f64`，内部订单簿使用 `i64` 编码
- WebSocket 重连和错误处理需要完善
- AggDepth 功能目前仅在 BinanceFutures 中实现
- 日志输出到 `./logs` 目录（按天滚动）
