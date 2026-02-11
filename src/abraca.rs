use crate::{
    common::{Exchange, MgReq, MgResult, MgRsp, ReqSender, RspReceiver, RspSender},
    error::Result,
    mg::start_mg,
};
use dashmap::DashMap;
use std::collections::HashMap;
use std::{
    net::{Ipv4Addr, SocketAddr},
    sync::Arc,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, WriteHalf},
    net::{TcpListener, TcpStream},
    sync::RwLock,
};

pub struct Abraca {
    clients: Arc<DashMap<SocketAddr, WriteHalf<TcpStream>>>,
    /// 每个 exchange 只起一个 mg，写锁保证只初始化一次
    req_txs: Arc<RwLock<HashMap<Exchange, ReqSender>>>,
    rsp_tx: RspSender,
    rsp_rx: RspReceiver,
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
            clients: Arc::new(DashMap::new()),
            req_txs: Arc::new(RwLock::new(HashMap::new())),
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
        let (mut reader, writer) = tokio::io::split(stream);
        self.clients.insert(addr, writer);
        let clients = self.clients.clone();
        let req_txs = self.req_txs.clone();
        let rsp_tx = self.rsp_tx.clone();
        tokio::spawn(async move {
            let mut buf = [0u8; 4096];
            loop {
                match reader.read(&mut buf).await {
                    Ok(0) => {
                        tracing::info!("Connection closed by {}", addr);
                        break;
                    }
                    Ok(n) => {
                        if let Ok(req) = serde_json::from_slice::<MgReq>(&buf[..n]) {
                            let tx = {
                                let mut g = req_txs.write().await;
                                if let Some(t) = g.get(&req.exchange) {
                                    t.clone()
                                } else {
                                    match start_mg(req.exchange, rsp_tx.clone()).await {
                                        Ok(t) => {
                                            g.insert(req.exchange, t.clone());
                                            t
                                        }
                                        Err(e) => {
                                            let rsp = MgRsp::Result(MgResult {
                                                id: req.id,
                                                timestamp: chrono::Utc::now().timestamp_millis(),
                                                error: Some(e.to_string()),
                                                result: false,
                                            });
                                            let _ = rsp_tx.send(rsp).await;
                                            continue;
                                        }
                                    }
                                }
                            };
                            if let Err(e) = tx.send(req).await {
                                tracing::error!("Send to mg failed: {}", e);
                            }
                        } else {
                            tracing::error!("Invalid request from {}", addr);
                        }
                    }
                    Err(e) => {
                        tracing::error!("Error reading from {}: {}", addr, e);
                        break;
                    }
                }
            }
            clients.remove(&addr);
        });
        Ok(())
    }

    async fn on_rsp(&self, rsp: MgRsp) -> Result<()> {
        let data = serde_json::to_vec(&rsp)?;
        let mut failed = Vec::new();
        for mut entry in self.clients.iter_mut() {
            if let Err(e) = entry.value_mut().write_all(&data).await {
                tracing::warn!("Write to client {} failed, removing: {}", entry.key(), e);
                failed.push(*entry.key());
            }
        }
        for addr in failed {
            self.clients.remove(&addr);
        }
        Ok(())
    }
}
