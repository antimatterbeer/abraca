use crate::{
    channel::{ReqData, ReqSender, RspData, RspReceiver, RspSender, channel},
    error::Result,
};
use abraca_base::prelude::*;
use dashmap::DashMap;
use std::{
    collections::{HashMap, HashSet},
    net::{Ipv4Addr, SocketAddr},
    sync::Arc,
};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader, WriteHalf},
    net::{TcpListener, TcpStream},
    sync::RwLock,
};

#[derive(Debug, Default, Clone)]
pub struct MdsState {
    clients: Arc<DashMap<SocketAddr, WriteHalf<TcpStream>>>,
    req_txs: Arc<RwLock<HashMap<Exchange, ReqSender>>>,
    subscribers: Arc<DashMap<(Exchange, String), HashSet<SocketAddr>>>,
    subscriptions: Arc<DashMap<SocketAddr, HashSet<(Exchange, String)>>>,
    requests: Arc<DashMap<u32, SocketAddr>>,
}

pub struct Mds {
    state: MdsState,     // 共享状态
    rsp_tx: RspSender,   // 到客户端的响应通道
    rsp_rx: RspReceiver, // 从客户端的响应通道
}

impl Default for Mds {
    fn default() -> Self {
        Self::new()
    }
}

impl Mds {
    /// 创建一个新的 Mds 实例
    pub fn new() -> Self {
        let (rsp_tx, rsp_rx) = channel(1024);
        Self {
            state: MdsState::default(),
            rsp_tx,
            rsp_rx,
        }
    }

    /// 运行 Mds 实例
    ///
    /// # Arguments
    ///
    /// * `port` - 监听端口
    ///
    /// # Returns
    ///
    /// * `Result<()>` - 运行结果
    pub async fn run(mut self, port: u16) -> Result<()> {
        let addr = SocketAddr::from((Ipv4Addr::UNSPECIFIED, port));
        let listener = TcpListener::bind(addr).await?;
        tracing::info!("Listening on {}", addr);
        loop {
            tokio::select! {
                Ok((stream, addr)) = listener.accept() => self.on_connection(stream, addr).await?,
                Some(rsp) = self.rsp_rx.recv() => self.on_rsp(rsp).await?,
            }
        }
    }

    async fn on_connection(&self, stream: TcpStream, addr: SocketAddr) -> Result<()> {
        tracing::info!("New connection from {addr}");
        let (reader, writer) = tokio::io::split(stream);
        let rsp_tx = self.rsp_tx.clone();
        let state = self.state.clone();
        state.clients.insert(addr, writer);
        tokio::spawn(async move {
            let mut buf = Vec::new();
            let mut reader = BufReader::new(reader);
            loop {
                match reader.read_until(b'\n', &mut buf).await {
                    Ok(0) => {
                        tracing::info!("Connection closed by {}", addr);
                        break;
                    }
                    Ok(_) => {
                        let line = buf.strip_suffix(b"\n").unwrap_or(buf.as_slice());
                        let Ok(req) = serde_json::from_slice::<Request<ReqData>>(line) else {
                            tracing::error!(
                                "Invalid request from {}: {}",
                                addr,
                                String::from_utf8_lossy(line)
                            );
                            buf.clear();
                            continue;
                        };
                        buf.clear();
                        let tx = {
                            let mut g = state.req_txs.write().await;
                            if let Some(t) = g.get(&req.exchange) {
                                t.clone()
                            } else {
                                match crate::gateway::start(req.exchange, rsp_tx.clone()).await {
                                    Ok(tx) => {
                                        g.insert(req.exchange, tx.clone());
                                        tx
                                    }
                                    Err(e) => {
                                        let rsp = Response::<RspData> {
                                            exchange: req.exchange,
                                            timestamp: chrono::Utc::now().timestamp_millis(),
                                            id: Some(req.id),
                                            data: RspData::Error(e.to_string()),
                                        };
                                        let _ = rsp_tx.send(rsp).await;
                                        continue;
                                    }
                                }
                            }
                        };
                        match req.data {
                            ReqData::Subscribe(topics) => {
                                let mut new_topics = Vec::new();
                                for topic in &topics {
                                    let mut addrs = state
                                        .subscribers
                                        .entry((req.exchange, topic.clone()))
                                        .or_default();
                                    addrs.insert(addr);
                                    if addrs.len() == 1 {
                                        new_topics.push(topic.clone());
                                    }
                                }
                                state.subscriptions.insert(
                                    addr,
                                    HashSet::from_iter(
                                        topics.iter().map(|topic| (req.exchange, topic.clone())),
                                    ),
                                );
                                let req = Request::<ReqData> {
                                    exchange: req.exchange,
                                    id: req.id,
                                    data: ReqData::Subscribe(new_topics),
                                };
                                let _ = tx.send(req).await;
                            }
                            ReqData::Unsubscribe(topics) => {
                                let mut removed_topics = Vec::new();
                                for topic in &topics {
                                    let mut addrs = state
                                        .subscribers
                                        .entry((req.exchange, topic.clone()))
                                        .or_default();
                                    addrs.remove(&addr);
                                    if addrs.is_empty() {
                                        removed_topics.push(topic.clone());
                                    }
                                }
                                state.subscriptions.remove(&addr);
                                let req = Request::<ReqData> {
                                    exchange: req.exchange,
                                    id: req.id,
                                    data: ReqData::Unsubscribe(removed_topics),
                                };
                                let _ = tx.send(req).await;
                            }
                            _ => {
                                state.requests.insert(req.id, addr);
                                let _ = tx.send(req).await;
                            }
                        }
                        buf.clear();
                    }
                    Err(e) => {
                        tracing::error!("Error reading from {}: {}", addr, e);
                        break;
                    }
                }
            }
            state.clients.remove(&addr);
        });
        Ok(())
    }

    async fn on_rsp(&self, rsp: Response<RspData>) -> Result<()> {
        if let Some(id) = rsp.id {
            if let Some((_, addr)) = self.state.requests.remove(&id) {
                let data = format!("{}\n", serde_json::to_string(&rsp).unwrap());
                if let Some(mut writer) = self.state.clients.get_mut(&addr) {
                    let _ = writer.write_all(data.as_bytes()).await;
                }
            }
        } else {
            let topic = match &rsp.data {
                RspData::Kline(kline) => {
                    format!("{}@Kline", kline.symbol)
                }
                RspData::Depth(depth) => {
                    format!("{}@Depth", depth.symbol)
                }
                RspData::BestPrice(best_price) => {
                    format!("{}@BestPrice", best_price.symbol)
                }
                RspData::MarkPrice(mark_price) => {
                    format!("{}@MarkPrice", mark_price.symbol)
                }
                RspData::ForceOrder(force_order) => {
                    format!("{}@ForceOrder", force_order.symbol)
                }
                _ => {
                    return Ok(());
                }
            };
            if let Some(subscribers) = self.state.subscribers.get(&(rsp.exchange, topic)) {
                let data = format!("{}\n", serde_json::to_string(&rsp).unwrap());
                for addr in subscribers.iter() {
                    if let Some(mut writer) = self.state.clients.get_mut(addr) {
                        let _ = writer.write_all(data.as_bytes()).await;
                    }
                }
            }
        }
        Ok(())
    }
}
