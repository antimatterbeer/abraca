use crate::def::{ContractStatus, ContractType, OrderSide, OrderStatus, OrderType, TimeInForce};
use serde::{Deserialize, Serialize};

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
pub struct Ticker {
    /// 时间戳
    pub timestamp: i64,
    /// 交易对
    pub symbol: String,
    /// 价格
    pub price: f64,
    /// 成交量
    pub volume: f64,
    /// 成交额
    pub quote_volume: f64,
}

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
