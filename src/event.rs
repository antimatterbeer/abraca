use crate::{
    def::Exchange,
    msg::{Depth, Kline, SymbolInfo},
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
#[serde(tag = "rsp", content = "data")]
#[serde(rename_all = "snake_case")]
pub enum MarketRspData {
    /// K线
    Kline(Kline),
    /// 深度
    Depth(Depth),
    /// 交易对信息
    SymbolInfo(SymbolInfo),
    /// 请示响应
    Response(Response),
}

/// 请求响应
#[derive(Debug, Deserialize, Serialize)]
pub struct Response {
    /// 请求ID
    pub id: u32,
    /// 结果
    pub result: bool,
    /// 错误信息
    pub error: Option<String>,
}

pub type ReqSender = Sender<MarketReq>;

pub type ReqReceiver = Receiver<MarketReq>;

pub type RspSender = Sender<MarketRsp>;

pub type RspReceiver = Receiver<MarketRsp>;
