use abraca_base::prelude::*;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::{Receiver, Sender};

/// 市场请求数据
#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "req", content = "data")]
#[serde(rename_all = "snake_case")]
pub enum ReqData {
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
pub enum RspData {
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

pub type ReqSender = Sender<Request<ReqData>>;

pub type ReqReceiver = Receiver<Request<ReqData>>;

pub type RspSender = Sender<Response<RspData>>;

pub type RspReceiver = Receiver<Response<RspData>>;

pub use tokio::sync::mpsc::channel;
