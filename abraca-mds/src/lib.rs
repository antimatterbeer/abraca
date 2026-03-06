use abraca_base::{
    message::{Request, Response},
    types::{BestPrice, Depth, ForceOrder, Kline, MarkPrice, SymbolInfo},
};
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;

pub mod error;
mod gateway;
pub mod server;

#[allow(unused)]
mod orderbook;

pub use error::{Error, Result};
pub use server::Server;

#[derive(Debug, Deserialize)]
enum ReqData {
    Subscribe(Vec<String>),
    Unsubscribe(Vec<String>),
    GetSymbolInfo(Vec<String>),
}

#[derive(Debug, Serialize)]
enum RspData {
    SymbolInfos(Vec<SymbolInfo>),
    Kline(Kline),
    Depth(Depth),
    BestPrice(BestPrice),
    MarkPrice(MarkPrice),
    ForceOrder(ForceOrder),
}

enum MdsEvent {
    Request(SocketAddr, Request<ReqData>), // 从客户端接收请求
    Response(Response<RspData>),           // 从网关接收响应
    Disconnect(SocketAddr),                // 客户端断开连接
}
