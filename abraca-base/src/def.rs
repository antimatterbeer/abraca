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
