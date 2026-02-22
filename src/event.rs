use crate::{
    def::Exchange,
    msg::{BestPrice, Depth, ForceOrder, Kline, MarkPrice, SymbolInfo},
};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::{Receiver, Sender};

/// 市场请求
#[derive(Debug, Deserialize, Serialize)]
pub struct MarketReq {
    /// 交易所
    pub exchange: Exchange,
    /// 请求ID
    pub id: u32,
    /// 请求数据
    #[serde(flatten)]
    pub data: MarketReqData,
}

/// 市场响应
#[derive(Debug, Deserialize, Serialize)]
pub struct MarketRsp {
    /// 交易所
    pub exchange: Exchange,
    /// 请求ID
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<u32>,
    /// 服务器时间戳
    pub timestamp: i64,
    /// 响应数据
    #[serde(flatten)]
    pub data: MarketRspData,
}

/// 市场请求数据
#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "req", content = "data")]
#[serde(rename_all = "snake_case")]
pub enum MarketReqData {
    /// 订阅
    Subscribe(Vec<String>),
    /// 取消订阅
    Unsubscribe(Vec<String>),
    /// 获取交易对信息
    GetSymbolInfo(Vec<String>),
}

/// 市场响应数据
#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "data_type", content = "data")]
#[serde(rename_all = "snake_case")]
pub enum MarketRspData {
    /// 错误
    Error(String),
    /// K线
    Kline(Kline),
    /// 深度
    Depth(Depth),
    /// 最佳价格
    BestPrice(BestPrice),
    /// 标记价格
    MarkPrice(MarkPrice),
    /// 强平订单
    ForceOrder(ForceOrder),
    /// 请示结果
    SymbolInfos(Vec<SymbolInfo>),
}

pub type ReqSender = Sender<MarketReq>;

pub type ReqReceiver = Receiver<MarketReq>;

pub type RspSender = Sender<MarketRsp>;

pub type RspReceiver = Receiver<MarketRsp>;
