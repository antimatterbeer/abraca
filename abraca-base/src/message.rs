use crate::types::Exchange;
use serde::{Deserialize, Serialize};

/// 请求消息
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

/// 响应消息
#[derive(Debug, Serialize)]
pub struct Response<T> {
    /// 时间戳
    pub timestamp: i64,
    /// 交易所
    pub exchange: Exchange,
    /// 标识符
    #[serde(flatten)]
    pub identifier: ResponseIdentifier,
    /// 结果
    #[serde(flatten)]
    pub result: ResponseResult<T>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResponseIdentifier {
    Id(u32),
    Stream(String),
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResponseResult<T> {
    Data(T),
    Error(String),
}
