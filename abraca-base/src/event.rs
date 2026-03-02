use crate::def::Exchange;

use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize)]
pub struct Request<T> {
    /// 交易所
    pub exchange: Exchange,
    /// 请求ID
    pub id: u32,
    /// 请求数据
    #[serde(flatten)]
    pub data: T,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Response<T> {
    /// 交易所
    pub exchange: Exchange,
    /// 请求ID
    pub id: Option<u32>,
    /// 服务器时间戳
    pub timestamp: i64,
    /// 响应数据
    #[serde(flatten)]
    pub data: T,
}
