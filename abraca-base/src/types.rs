use serde::{Deserialize, Serialize};

#[non_exhaustive]
#[derive(Debug, Clone, Copy, Hash, Eq, PartialEq, Deserialize, Serialize)]
pub enum Exchange {
    /// 币安现货
    BinanceSpot,
    /// 币安U本位期货
    BinanceFutures,
    /// 币安币本位期货
    BinanceFuturesCM,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ContractType {
    /// 永续合约
    Perpetual,
    /// 当月合约
    CurrentMonth,
    /// 次月合约
    NextMonth,
    /// 当季度合约
    CurrentQuarter,
    /// 次季度合约
    NextQuarter,
    /// 永续合约交割中
    PerpetualDelivering,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ContractStatus {
    /// 待上市
    PendingTrading,
    /// 交易中
    Trading,
    /// 预交割
    PreDelivering,
    /// 交割中
    Delivering,
    /// 已交割
    Delivered,
    /// 预结算
    PreSettle,
    /// 结算中
    Settling,
    /// 已下架
    Close,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum OrderStatus {
    /// 新订单
    New,
    /// 部分成交
    PartiallyFilled,
    /// 全部成交
    Filled,
    /// 已取消
    Canceled,
    /// 已拒绝
    Rejected,
    /// 已过期
    Expired,
    /// 在匹配时过期
    ExpiredInMatch,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum OrderType {
    /// 限价单
    Limit,
    /// 市价单
    Market,
    /// 限价止损单
    Stop,
    /// 市价止损单
    StopMarket,
    /// 限价止盈单
    TakeProfit,
    /// 市价止盈单
    TakeProfitMarket,
    /// 追踪止损单
    TralingStopMarket,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum OrderSide {
    /// 买单
    Buy,
    /// 卖单
    Sell,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum TimeInForce {
    // 挂单有效直到撤销
    GTC,
    /// 无法立即成交的部分撤消
    IOC,
    /// 无法全部成交则撤消
    FOK,
    /// 无法成为挂单方则撤消
    GTX,
    /// 挂单有效直到指定时间
    GTD,
    /// 仅与来自APP或者网页端的订单成交
    RPI,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SymbolInfo {
    /// 交易对
    pub symbol: String,
    /// 合约类型
    pub contract_type: ContractType,
    /// 交割日期
    pub delivery_date: i64,
    /// 上架日期
    pub onboard_date: i64,
    /// 状态
    pub status: ContractStatus,
    /// 基础资产
    pub base_asset: String,
    /// 计价资产
    pub quote_asset: String,
    /// 保证金资产
    pub margin_asset: String,
    /// 价格精度
    pub price_precision: u8,
    /// 数量精度
    pub quantity_precision: u8,
    /// 基础资产精度
    pub base_asset_precision: u8,
    /// 计价资产精度
    pub quote_precision: u8,
    /// 价格最小变动
    pub tick_size: f64,
    /// 订单最小数量间隔
    pub lot_size: f64,
    /// 市场最小交易量
    pub market_lot_size: f64,
    /// 最小交易额
    pub min_notional: f64,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct MarkPrice {
    /// 时间戳
    pub timestamp: i64,
    /// 交易对
    pub symbol: String,
    /// 标记价格
    pub mark_price: f64,
    /// 指数价格
    pub index_price: f64,
    /// 预估结算价格
    pub estimated_settle_price: f64,
    /// 资金费率
    pub funding_rate: f64,
    /// 下次资金费率时间
    pub next_funding_time: i64,
}

/// 最优挂单信息
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct BestPrice {
    /// 时间戳
    pub timestamp: i64,
    /// 交易对
    pub symbol: String,
    /// 卖一价
    pub ask_price: f64,
    /// 卖一量
    pub ask_volume: f64,
    /// 买一价
    pub bid_price: f64,
    /// 买一量
    pub bid_volume: f64,
}

/// 深度信息
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Depth {
    /// 时间戳
    pub timestamp: i64,
    /// 交易对
    pub symbol: String,
    /// 买单
    pub bids: Vec<(f64, f64)>,
    /// 卖单
    pub asks: Vec<(f64, f64)>,
}

/// K线信息
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Kline {
    /// 时间戳
    pub timestamp: i64,
    /// 交易对
    pub symbol: String,
    /// 开盘价
    pub open: f64,
    /// 最高价
    pub high: f64,
    /// 最低价
    pub low: f64,
    /// 收盘价
    pub close: f64,
    /// 成交量
    pub volume: f64,
    /// 成交额
    pub quote_volume: f64,
}

/// 强平订单信息
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ForceOrder {
    /// 时间戳
    pub timestamp: i64,
    /// 交易对
    pub symbol: String,
    /// 方向
    pub side: OrderSide,
    /// 订单类型
    pub order_type: OrderType,
    /// 时间
    pub time_in_force: TimeInForce,
    /// 数量
    pub quantity: f64,
    /// 价格
    pub price: f64,
    /// 平均价格
    pub average_price: f64,
    /// 订单状态
    pub status: OrderStatus,
    /// 最后成交数量
    pub last_filled_quantity: f64,
    /// 累计成交数量
    pub filled_quantity: f64,
}
