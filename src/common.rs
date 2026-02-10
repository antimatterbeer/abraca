use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::{Receiver, Sender};

#[non_exhaustive]
#[derive(Debug, Clone, Copy, Hash, Eq, PartialEq, Deserialize, Serialize)]
pub enum Exchange {
    BinanceSpot,
    BinanceFutures,
    BinanceFuturesCM,
}

#[derive(Debug, Serialize)]
pub struct Depth {
    pub symbol: String,
    pub bids: Vec<(f64, f64)>,
    pub asks: Vec<(f64, f64)>,
    pub timestamp: i64,
}

#[derive(Debug, Serialize)]
pub struct Kline {
    pub symbol: String,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
    pub amount: f64,
    pub timestamp: i64,
}

#[derive(Debug, Deserialize)]
pub struct MgReq {
    /// 交易所
    pub exchange: Exchange,
    /// 请求数据
    #[serde(flatten)]
    pub data: MgReqData,
    /// 请求ID
    pub id: u32,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "req", content = "data", rename_all = "snake_case")]
pub enum MgReqData {
    /// 订阅
    Subscribe(Vec<String>),
    /// 取消订阅
    Unsubscribe(Vec<String>),
}

/// 响应消息
#[derive(Debug, Serialize)]
#[serde(untagged)]
pub enum MgRsp {
    /// 请求结果
    Result(MgResult),
    /// 数据更新
    Update(MgUpdate),
}

/// 某次请求的响应（与 MgReq.id 对应）
#[derive(Debug, Serialize)]
pub struct MgResult {
    /// 请求ID
    pub id: u32,
    /// 服务器时间戳
    pub timestamp: i64,
    /// 错误信息
    pub error: Option<String>,
    /// 结果
    pub result: bool,
}

/// 更新消息
#[derive(Debug, Serialize)]
pub struct MgUpdate {
    /// 交易所
    pub exchange: Exchange,
    /// 主题
    pub topic: String,
    /// 时间戳
    pub timestamp: i64,
    /// 数据
    pub data: MgUpdateData,
}

/// 更新数据
#[derive(Debug, Serialize)]
pub enum MgUpdateData {
    /// 深度
    Depth(Depth),
    /// K线
    Kline(Kline),
}

pub type ReqSender = Sender<MgReq>;

pub type ReqReceiver = Receiver<MgReq>;

pub type RspSender = Sender<MgRsp>;

pub type RspReceiver = Receiver<MgRsp>;
