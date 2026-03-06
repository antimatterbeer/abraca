use crate::{MdsEvent, ReqData, RspData, error::Result, gateway};
use abraca_base::{
    message::{Request, Response, ResponseIdentifier},
    types::Exchange,
};
use std::{
    collections::{HashMap, HashSet},
    net::{Ipv4Addr, SocketAddr},
};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
    sync::mpsc::{Receiver, Sender, channel},
};

#[derive(Debug)]
pub struct Server {
    tx: Sender<MdsEvent>,
    rx: Receiver<MdsEvent>,
    client_txs: HashMap<SocketAddr, Sender<String>>,
    gateway_txs: HashMap<Exchange, Sender<Request<ReqData>>>,
    requests: HashMap<u32, SocketAddr>,
    subscriptions: HashMap<String, HashSet<SocketAddr>>,
}

impl Server {
    pub fn new() -> Self {
        let (tx, rx) = channel(1024);
        Self {
            tx,
            rx,
            client_txs: HashMap::new(),
            gateway_txs: HashMap::new(),
            requests: HashMap::new(),
            subscriptions: HashMap::new(),
        }
    }

    pub async fn run(mut self, port: u16) -> Result<()> {
        let addr = SocketAddr::from((Ipv4Addr::UNSPECIFIED, port));
        let listener = TcpListener::bind(addr).await?;
        tracing::info!("Listening on {}", addr);
        loop {
            tokio::select! {
                Ok((stream, addr)) = listener.accept() => self.on_connection(stream, addr).await?,
                Some(event) = self.rx.recv() => match event {
                    MdsEvent::Request(addr, req) => self.on_request(addr, req).await?,
                    MdsEvent::Response(rsp) => self.on_response(rsp).await?,
                    MdsEvent::Disconnect(addr) => self.on_disconnect(addr).await?,
                },
            }
        }
    }

    /// 处理新的客户端连接
    ///
    /// # Arguments
    ///
    /// * `stream` - 客户端连接
    /// * `addr` - 客户端地址
    ///
    /// # Returns
    ///
    /// * `Result<()>` - 处理结果
    async fn on_connection(&mut self, stream: TcpStream, addr: SocketAddr) -> Result<()> {
        tracing::info!("New connection from {addr}");
        let (tx, mut rx) = channel(1024);
        self.client_txs.insert(addr, tx);
        let (reader, mut writer) = tokio::io::split(stream);
        let mut buf = Vec::new();
        let mut reader = BufReader::new(reader);
        let req_tx = self.tx.clone();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    res = reader.read_until(b'\n', &mut buf) => {
                        match res {
                            Ok(0) => tracing::info!("Connection closed by {addr}"),
                            Err(e) => tracing::error!("Error reading from {addr}: {e}"),
                            Ok(_) => {
                                let line = buf.strip_suffix(b"\n").unwrap_or(buf.as_slice());
                                match serde_json::from_slice::<Request<ReqData>>(line) {
                                    Ok(req) => {
                                        let _ = req_tx.send(MdsEvent::Request(addr, req)).await;
                                    }
                                    Err(e) => {
                                        tracing::error!("Error parsing request from {addr}: {e}");
                                    }
                                }
                                buf.clear();
                                continue;
                            }
                        }
                        let _ = req_tx.send(MdsEvent::Disconnect(addr)).await;
                        break;
                    }
                    Some(data) = rx.recv() => {
                        let _ = writer.write_all(data.as_bytes()).await;
                    }
                }
            }
        });
        Ok(())
    }

    async fn on_request(&mut self, addr: SocketAddr, req: Request<ReqData>) -> Result<()> {
        tracing::info!("Request from {addr}: {req:?}");
        if !self.gateway_txs.contains_key(&req.exchange) {
            match gateway::start_gateway(req.exchange, self.tx.clone()).await {
                Ok(tx) => {
                    self.gateway_txs.insert(req.exchange, tx);
                }
                Err(e) => tracing::error!("Error starting gateway for {:?}: {e}", req.exchange),
            }
        }
        if let Some(tx) = self.gateway_txs.get(&req.exchange) {
            match &req.data {
                ReqData::Subscribe(streams) => {
                    for stream in streams {
                        self.subscriptions
                            .entry(stream.clone())
                            .or_default()
                            .insert(addr);
                    }
                }
                ReqData::Unsubscribe(streams) => {
                    for stream in streams {
                        if let Some(set) = self.subscriptions.get_mut(stream) {
                            set.remove(&addr);
                        }
                    }
                }
                _ => {}
            }
            self.requests.insert(req.id, addr);
            if let Err(e) = tx.try_send(req) {
                tracing::error!("Error sending request to gateway: {e}");
            }
        }
        Ok(())
    }

    async fn on_response(&mut self, rsp: Response<RspData>) -> Result<()> {
        let data = format!("{}\n", serde_json::to_string(&rsp).unwrap());
        match &rsp.identifier {
            ResponseIdentifier::Id(id) => {
                if let Some(addr) = self.requests.remove(id)
                    && let Some(tx) = self.client_txs.get(&addr)
                    && let Err(e) = tx.try_send(data)
                {
                    tracing::error!("Error sending response to client {addr}: {e}");
                }
            }
            ResponseIdentifier::Stream(stream) => {
                let mut dead = Vec::new();
                if let Some(subscribers) = self.subscriptions.get(stream) {
                    for addr in subscribers {
                        if let Some(tx) = self.client_txs.get(addr) {
                            if let Err(e) = tx.try_send(data.clone()) {
                                tracing::error!("Error sending response to client {addr}: {e}");
                            }
                        } else {
                            dead.push(*addr);
                        }
                    }
                }
                if !dead.is_empty() {
                    let empty = if let Some(subscribers) = self.subscriptions.get_mut(stream) {
                        subscribers.retain(|addr| !dead.contains(addr));
                        subscribers.is_empty()
                    } else {
                        false
                    };
                    if empty {
                        self.subscriptions.remove(stream);
                        let req = Request::<ReqData> {
                            exchange: rsp.exchange,
                            id: 0,
                            data: ReqData::Unsubscribe(vec![stream.clone()]),
                        };
                        if let Some(tx) = self.gateway_txs.get(&rsp.exchange) {
                            let _ = tx.try_send(req);
                        }
                    }
                }
            }
        }
        Ok(())
    }

    async fn on_disconnect(&mut self, addr: SocketAddr) -> Result<()> {
        tracing::info!("Connection closed by {addr}");
        self.client_txs.remove(&addr);
        Ok(())
    }
}

impl Default for Server {
    fn default() -> Self {
        Self::new()
    }
}
