use crate::{market, prelude::*};
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
pub struct SharedState {
    clients: Arc<DashMap<SocketAddr, WriteHalf<TcpStream>>>,
    req_txs: Arc<RwLock<HashMap<Exchange, ReqSender>>>,
    subscribers: Arc<DashMap<(Exchange, String), HashSet<SocketAddr>>>,
    subscriptions: Arc<DashMap<SocketAddr, HashSet<(Exchange, String)>>>,
    requests: Arc<DashMap<u32, SocketAddr>>,
}

pub struct Abraca {
    state: SharedState,
    rsp_tx: RspSender,   // 到客户端的响应通道
    rsp_rx: RspReceiver, // 从客户端的响应通道
}

impl Default for Abraca {
    fn default() -> Self {
        Self::new()
    }
}

impl Abraca {
    pub fn new() -> Self {
        let (rsp_tx, rsp_rx) = tokio::sync::mpsc::channel(1024);
        Self {
            state: SharedState::default(),
            rsp_tx,
            rsp_rx,
        }
    }

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
                        let Ok(req) = serde_json::from_slice::<MarketReq>(line) else {
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
                                match market::start_mg(req.exchange, rsp_tx.clone()).await {
                                    Ok(tx) => {
                                        g.insert(req.exchange, tx.clone());
                                        tx
                                    }
                                    Err(e) => {
                                        let rsp = MarketRsp {
                                            exchange: req.exchange,
                                            timestamp: chrono::Utc::now().timestamp_millis(),
                                            id: Some(req.id),
                                            data: MarketRspData::Error(e.to_string()),
                                        };
                                        let _ = rsp_tx.send(rsp).await;
                                        continue;
                                    }
                                }
                            }
                        };
                        match req.data {
                            MarketReqData::Subscribe(topics) => {
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
                                let req = MarketReq {
                                    exchange: req.exchange,
                                    id: req.id,
                                    data: MarketReqData::Subscribe(new_topics),
                                };
                                let _ = tx.send(req).await;
                            }
                            MarketReqData::Unsubscribe(topics) => {
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
                                let req = MarketReq {
                                    exchange: req.exchange,
                                    id: req.id,
                                    data: MarketReqData::Unsubscribe(removed_topics),
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

    async fn on_rsp(&self, rsp: MarketRsp) -> Result<()> {
        let data = format!("{}\n", serde_json::to_string(&rsp)?);
        match rsp.data {
            MarketRspData::Kline(kline) => {
                let subscribers = self
                    .state
                    .subscribers
                    .get(&(rsp.exchange, format!("{}@Kline", kline.symbol)))
                    .unwrap();
                for addr in subscribers.iter() {
                    if let Some(mut writer) = self.state.clients.get_mut(addr) {
                        let _ = writer.write_all(data.as_bytes()).await;
                    }
                }
            }
            MarketRspData::Depth(depth) => {
                let subscribers = self
                    .state
                    .subscribers
                    .get(&(rsp.exchange, format!("{}@Depth", depth.symbol)))
                    .unwrap();
                for addr in subscribers.iter() {
                    if let Some(mut writer) = self.state.clients.get_mut(addr) {
                        let _ = writer.write_all(data.as_bytes()).await;
                    }
                }
            }
            _ => {
                let Some(id) = rsp.id else {
                    tracing::error!("Request ID is required");
                    return Ok(());
                };
                let Some((_, addr)) = self.state.requests.remove(&id) else {
                    tracing::error!("Request ID not found");
                    return Ok(());
                };
                if let Some(mut writer) = self.state.clients.get_mut(&addr) {
                    let _ = writer.write_all(data.as_bytes()).await;
                }
            }
        }
        Ok(())
    }
}
