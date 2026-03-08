use super::MdsStream;
use crate::{MdsEvent, ReqData, RspData, error::Result, orderbook::OrderBook};
use abraca_base::prelude::*;
use futures::{SinkExt, StreamExt};
use reqwest::Client;
use serde_json::json;
use std::collections::{HashMap, HashSet};
use tokio::sync::mpsc::{Receiver, Sender};
use tokio_tungstenite::tungstenite::Message;

const HTTP_URL: &str = "https://fapi.binance.com";
const WS_URL: &str = "wss://fstream.binance.com/stream";

struct OrderBookState {
    book: OrderBook,
    last_update_id: i64,
    prev_final_update_id: i64,
    status: ObStatus,
    buffered_events: Vec<inner::DepthDiffEvent>,
    /// 已注册的 (decimal_places, depth) 聚合订阅组合
    subscriptions: HashSet<(u32, usize)>,
}

enum ObStatus {
    /// 正在缓冲 diff 事件，等待 REST 快照
    Buffering,
    /// 快照已加载，但尚未找到第一个桥接事件（U <= lastUpdateId <= u）
    Syncing,
    /// 同步完成，正常接收增量更新
    Ready,
}

pub struct BinanceFutures {
    rx: Receiver<Request<ReqData>>,
    tx: Sender<MdsEvent>,
    http_client: Client,
    req_id: u32,
    id_map: HashMap<u32, u32>,
    symbol_infos: HashMap<String, SymbolInfo>,
    orderbooks: HashMap<String, OrderBookState>,
    snapshot_rx: Receiver<(String, inner::DepthSnapshotRsp)>,
    snapshot_tx: Sender<(String, inner::DepthSnapshotRsp)>,
}

impl BinanceFutures {
    pub fn new(rx: Receiver<Request<ReqData>>, tx: Sender<MdsEvent>) -> Self {
        let (snapshot_tx, snapshot_rx) = tokio::sync::mpsc::channel(16);
        Self {
            rx,
            tx,
            http_client: Client::new(),
            req_id: 0,
            id_map: HashMap::new(),
            symbol_infos: HashMap::new(),
            orderbooks: HashMap::new(),
            snapshot_rx,
            snapshot_tx,
        }
    }

    pub async fn run(mut self) -> Result<()> {
        tracing::info!("Binance futures market started");
        let (ws_stream, _) = tokio_tungstenite::connect_async(WS_URL).await?;
        let (mut writer, mut reader) = ws_stream.split();
        loop {
            tokio::select! {
                biased;
                Some(Ok(msg)) = reader.next() => {
                    if let Message::Text(text) = msg && let Err(e) = self.on_ws_msg(&text).await {
                            tracing::error!("Error processing websocket message: {}", e);
                    }
                }
                Some(req) = self.rx.recv() => {
                    if let Err(e) = self.on_req(&mut writer, req).await {
                        tracing::error!("Error processing request: {}", e);
                    }
                }
                Some((symbol, snapshot)) = self.snapshot_rx.recv() => {
                    if let Err(e) = self.on_snapshot(&symbol, snapshot).await {
                        tracing::error!("Error processing snapshot for {}: {}", symbol, e);
                    }
                }
            }
        }
    }

    async fn on_req<W>(&mut self, writer: &mut W, req: Request<ReqData>) -> Result<()>
    where
        W: SinkExt<Message> + Unpin,
        W::Error: Into<crate::error::Error>,
    {
        match req.data {
            ReqData::Subscribe(streams) => {
                let mut native_params = Vec::new();
                for s in &streams {
                    match s.parse::<MdsStream>() {
                        Ok(MdsStream::AggDepth {
                            symbol,
                            decimal_places,
                            depth,
                        }) => {
                            self.subscribe_agg_depth(writer, &symbol, decimal_places, depth)
                                .await?;
                        }
                        _ => {
                            if let Ok(param) = inner::stream_to_theirs(s) {
                                native_params.push(param);
                            }
                        }
                    }
                }
                if !native_params.is_empty() {
                    let data = json!({
                        "method": "SUBSCRIBE",
                        "params": native_params,
                        "id": self.req_id,
                    });
                    let text = serde_json::to_string(&data).unwrap();
                    writer
                        .send(Message::Text(text.into()))
                        .await
                        .map_err(Into::into)?;
                    self.id_map.insert(self.req_id, req.id);
                    self.req_id += 1;
                }
            }
            ReqData::Unsubscribe(streams) => {
                let mut native_params = Vec::new();
                for s in &streams {
                    match s.parse::<MdsStream>() {
                        Ok(MdsStream::AggDepth {
                            symbol,
                            decimal_places,
                            depth,
                        }) => {
                            self.unsubscribe_agg_depth(writer, &symbol, decimal_places, depth)
                                .await?;
                        }
                        _ => {
                            if let Ok(param) = inner::stream_to_theirs(s) {
                                native_params.push(param);
                            }
                        }
                    }
                }
                if !native_params.is_empty() {
                    let data = json!({
                        "method": "UNSUBSCRIBE",
                        "params": native_params,
                        "id": self.req_id,
                    });
                    let text = serde_json::to_string(&data).unwrap();
                    writer
                        .send(Message::Text(text.into()))
                        .await
                        .map_err(Into::into)?;
                    self.id_map.insert(self.req_id, req.id);
                    self.req_id += 1;
                }
            }
            ReqData::GetSymbolInfo(symbols) => {
                if self.symbol_infos.is_empty() {
                    self.get_symbol_infos().await?;
                }
                let infos = symbols
                    .iter()
                    .filter_map(|symbol| self.symbol_infos.get(symbol).cloned())
                    .collect();
                let rsp = Response::<RspData> {
                    timestamp: chrono::Utc::now().timestamp_millis(),
                    exchange: Exchange::BinanceFutures,
                    identifier: ResponseIdentifier::Id(req.id),
                    result: ResponseResult::Data(RspData::SymbolInfos(infos)),
                };
                let _ = self.tx.send(MdsEvent::Response(rsp)).await;
            }
        }
        Ok(())
    }

    async fn on_ws_msg(&mut self, text: &str) -> Result<()> {
        match serde_json::from_str::<inner::WsRsp>(text) {
            Ok(inner::WsRsp::Result(result)) => {
                if let Some(id) = self.id_map.remove(&result.id) {
                    if let Some(msg) = result.msg {
                        let rsp = Response::<RspData> {
                            timestamp: chrono::Utc::now().timestamp_millis(),
                            exchange: Exchange::BinanceFutures,
                            identifier: ResponseIdentifier::Id(id),
                            result: ResponseResult::Error(msg),
                        };
                        let _ = self.tx.send(MdsEvent::Response(rsp)).await;
                    }
                }
            }
            Ok(inner::WsRsp::Stream(inner::WsStream { stream, data })) => {
                let parts: Vec<&str> = stream.split('@').collect();
                if parts.get(1) == Some(&"depth") {
                    if let Ok(diff) = serde_json::from_value::<inner::DepthDiffEvent>(data) {
                        self.on_depth_diff(diff).await?;
                    }
                } else if let Ok(data) = serde_json::from_value::<inner::StreamData>(data)
                    && let Some(rsp_data) = data.into()
                {
                    let stream = inner::stream_to_ours(&stream)?;
                    let rsp = Response::<RspData> {
                        exchange: Exchange::BinanceFutures,
                        timestamp: chrono::Utc::now().timestamp_millis(),
                        identifier: ResponseIdentifier::Stream(stream),
                        result: ResponseResult::Data(rsp_data),
                    };
                    let _ = self.tx.send(MdsEvent::Response(rsp)).await;
                }
            }
            Err(_) => tracing::error!("Invalid message: {text}"),
        }
        Ok(())
    }

    // ─── AggDepth 相关方法 ───

    async fn subscribe_agg_depth<W>(
        &mut self,
        writer: &mut W,
        symbol: &str,
        decimal_places: u32,
        depth: usize,
    ) -> Result<()>
    where
        W: SinkExt<Message> + Unpin,
        W::Error: Into<crate::error::Error>,
    {
        if let Some(state) = self.orderbooks.get_mut(symbol) {
            state.subscriptions.insert((decimal_places, depth));
            return Ok(());
        }

        // 首次为该 symbol 创建 orderbook
        if self.symbol_infos.is_empty() {
            self.get_symbol_infos().await?;
        }
        let price_precision = self
            .symbol_infos
            .get(symbol)
            .map(|info| info.price_precision as u32)
            .unwrap_or(8);

        let mut state = OrderBookState {
            book: OrderBook::new(price_precision),
            last_update_id: 0,
            prev_final_update_id: 0,
            status: ObStatus::Buffering,
            buffered_events: Vec::new(),
            subscriptions: HashSet::new(),
        };
        state.subscriptions.insert((decimal_places, depth));
        self.orderbooks.insert(symbol.to_string(), state);

        // 订阅 diff depth stream
        let param = format!("{}@depth", symbol.to_lowercase());
        let data = json!({
            "method": "SUBSCRIBE",
            "params": [param],
            "id": self.req_id,
        });
        let text = serde_json::to_string(&data).unwrap();
        writer
            .send(Message::Text(text.into()))
            .await
            .map_err(Into::into)?;
        self.req_id += 1;

        self.spawn_snapshot_fetch(symbol);

        tracing::info!(
            "AggDepth subscribed: {}@AggDepth:{}:{}",
            symbol,
            decimal_places,
            depth
        );
        Ok(())
    }

    async fn unsubscribe_agg_depth<W>(
        &mut self,
        writer: &mut W,
        symbol: &str,
        decimal_places: u32,
        depth: usize,
    ) -> Result<()>
    where
        W: SinkExt<Message> + Unpin,
        W::Error: Into<crate::error::Error>,
    {
        let should_unsub_ws = if let Some(state) = self.orderbooks.get_mut(symbol) {
            state.subscriptions.remove(&(decimal_places, depth));
            state.subscriptions.is_empty()
        } else {
            false
        };

        if should_unsub_ws {
            self.orderbooks.remove(symbol);
            let param = format!("{}@depth", symbol.to_lowercase());
            let data = json!({
                "method": "UNSUBSCRIBE",
                "params": [param],
                "id": self.req_id,
            });
            let text = serde_json::to_string(&data).unwrap();
            writer
                .send(Message::Text(text.into()))
                .await
                .map_err(Into::into)?;
            self.req_id += 1;
            tracing::info!("AggDepth orderbook removed for {}", symbol);
        }
        Ok(())
    }

    async fn on_snapshot(
        &mut self,
        symbol: &str,
        snapshot: inner::DepthSnapshotRsp,
    ) -> Result<()> {
        let should_emit = {
            let state = match self.orderbooks.get_mut(symbol) {
                Some(s) => s,
                None => return Ok(()),
            };

            state.book.clear();
            let bids: Vec<(f64, f64)> = snapshot.bids.iter().map(|p| (p[0], p[1])).collect();
            let asks: Vec<(f64, f64)> = snapshot.asks.iter().map(|p| (p[0], p[1])).collect();
            state.book.update(&bids, &asks);
            state.last_update_id = snapshot.last_update_id;

            // 处理缓冲的事件
            let buffered = std::mem::take(&mut state.buffered_events);
            let mut found_first = false;
            for event in &buffered {
                if event.final_update_id < snapshot.last_update_id {
                    continue;
                }
                if !found_first {
                    if event.first_update_id <= snapshot.last_update_id
                        && event.final_update_id >= snapshot.last_update_id
                    {
                        found_first = true;
                    } else {
                        continue;
                    }
                }
                let bids: Vec<(f64, f64)> = event.bids.iter().map(|p| (p[0], p[1])).collect();
                let asks: Vec<(f64, f64)> = event.asks.iter().map(|p| (p[0], p[1])).collect();
                state.book.update(&bids, &asks);
                state.last_update_id = event.final_update_id;
                state.prev_final_update_id = event.final_update_id;
            }

            if found_first {
                state.status = ObStatus::Ready;
                tracing::info!(
                    "Orderbook ready for {}, lastUpdateId={}",
                    symbol,
                    state.last_update_id
                );
                true
            } else {
                state.status = ObStatus::Syncing;
                tracing::info!(
                    "Orderbook snapshot loaded for {}, lastUpdateId={}, waiting for bridge event...",
                    symbol,
                    state.last_update_id
                );
                false
            }
        };

        if should_emit {
            let state = self.orderbooks.get(symbol).unwrap();
            Self::emit_agg_depth(state, symbol, &self.tx).await;
        }
        Ok(())
    }

    async fn on_depth_diff(&mut self, diff: inner::DepthDiffEvent) -> Result<()> {
        let symbol = diff.symbol.clone();

        // 阶段 1：根据当前状态处理 diff，返回是否需要推送
        let should_emit = {
            let state = match self.orderbooks.get_mut(&symbol) {
                Some(s) => s,
                None => return Ok(()),
            };

            match state.status {
                ObStatus::Buffering => {
                    state.buffered_events.push(diff);
                    return Ok(());
                }
                ObStatus::Syncing => {
                    if diff.final_update_id < state.last_update_id {
                        return Ok(());
                    }
                    if diff.first_update_id <= state.last_update_id {
                        let bids: Vec<(f64, f64)> =
                            diff.bids.iter().map(|p| (p[0], p[1])).collect();
                        let asks: Vec<(f64, f64)> =
                            diff.asks.iter().map(|p| (p[0], p[1])).collect();
                        state.book.update(&bids, &asks);
                        state.last_update_id = diff.final_update_id;
                        state.prev_final_update_id = diff.final_update_id;
                        state.status = ObStatus::Ready;
                        tracing::info!(
                            "Orderbook synced for {}, lastUpdateId={}",
                            symbol,
                            state.last_update_id
                        );
                        true
                    } else {
                        tracing::warn!(
                            "Missed bridge event for {} (U={} > lastUpdateId={}), re-syncing...",
                            symbol,
                            diff.first_update_id,
                            state.last_update_id
                        );
                        state.book.clear();
                        state.status = ObStatus::Buffering;
                        state.buffered_events.clear();
                        state.buffered_events.push(diff);
                        false
                    }
                }
                ObStatus::Ready => {
                    if diff.prev_final_update_id != state.prev_final_update_id {
                        tracing::warn!(
                            "Orderbook continuity broken for {}: expected pu={}, got pu={}. Re-syncing...",
                            symbol,
                            state.prev_final_update_id,
                            diff.prev_final_update_id
                        );
                        state.book.clear();
                        state.status = ObStatus::Buffering;
                        state.buffered_events.clear();
                        state.buffered_events.push(diff);
                        false
                    } else {
                        let bids: Vec<(f64, f64)> =
                            diff.bids.iter().map(|p| (p[0], p[1])).collect();
                        let asks: Vec<(f64, f64)> =
                            diff.asks.iter().map(|p| (p[0], p[1])).collect();
                        state.book.update(&bids, &asks);
                        state.last_update_id = diff.final_update_id;
                        state.prev_final_update_id = diff.final_update_id;
                        true
                    }
                }
            }
        };

        // 阶段 2：副作用——推送或重新同步（mutable borrow 已释放）
        if should_emit {
            let state = self.orderbooks.get(&symbol).unwrap();
            Self::emit_agg_depth(state, &symbol, &self.tx).await;
        } else {
            self.spawn_snapshot_fetch(&symbol);
        }
        Ok(())
    }

    fn spawn_snapshot_fetch(&self, symbol: &str) {
        let http_client = self.http_client.clone();
        let snapshot_tx = self.snapshot_tx.clone();
        let sym = symbol.to_string();
        tokio::spawn(async move {
            loop {
                match BinanceFutures::fetch_depth_snapshot(&http_client, &sym).await {
                    Ok(snapshot) => {
                        let _ = snapshot_tx.send((sym, snapshot)).await;
                        break;
                    }
                    Err(e) => {
                        tracing::error!(
                            "Error fetching depth snapshot for {}: {}. Retrying...",
                            sym,
                            e
                        );
                        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                    }
                }
            }
        });
    }

    /// 向所有订阅了该 symbol 的聚合档位的客户端推送数据
    async fn emit_agg_depth(state: &OrderBookState, symbol: &str, tx: &Sender<MdsEvent>) {
        if !state.book.check_validity() {
            return;
        }
        let timestamp = chrono::Utc::now().timestamp_millis();
        for &(decimal_places, depth) in &state.subscriptions {
            let bids = state.book.best_bids(depth, decimal_places);
            let asks = state.book.best_asks(depth, decimal_places);
            let stream = MdsStream::AggDepth {
                symbol: symbol.to_string(),
                decimal_places,
                depth,
            };
            let rsp = Response::<RspData> {
                exchange: Exchange::BinanceFutures,
                timestamp,
                identifier: ResponseIdentifier::Stream(stream.to_string()),
                result: ResponseResult::Data(RspData::Depth(Depth {
                    symbol: symbol.to_string(),
                    timestamp,
                    bids,
                    asks,
                })),
            };
            let _ = tx.send(MdsEvent::Response(rsp)).await;
        }
    }

    // ─── HTTP 方法 ───

    async fn get_symbol_infos(&mut self) -> Result<()> {
        let rsp = self
            .http_client
            .get(format!("{}/fapi/v1/exchangeInfo", HTTP_URL))
            .send()
            .await?
            .json::<inner::ExchangeInfoRsp>()
            .await?;
        for symbol in rsp.symbols {
            self.symbol_infos
                .insert(symbol.symbol.clone(), symbol.into());
        }
        Ok(())
    }

    async fn fetch_depth_snapshot(
        http_client: &Client,
        symbol: &str,
    ) -> Result<inner::DepthSnapshotRsp> {
        let rsp = http_client
            .get(format!(
                "{}/fapi/v1/depth?symbol={}&limit=1000",
                HTTP_URL, symbol
            ))
            .send()
            .await?
            .json::<inner::DepthSnapshotRsp>()
            .await?;
        Ok(rsp)
    }
}

mod inner {
    use super::*;
    use crate::error::Error;
    use abraca_base::types::{
        ContractStatus, ContractType, OrderSide, OrderStatus, OrderType, TimeInForce,
    };
    use serde::{Deserialize, Serialize};
    use serde_json::Value;
    use serde_with::{DisplayFromStr, serde_as};
    use std::str::FromStr;

    pub(super) fn stream_to_theirs(stream: &str) -> Result<String> {
        let mds_stream = MdsStream::from_str(stream)?;
        match mds_stream {
            MdsStream::Kline(symbol) => Ok(format!("{}@kline_1m", symbol.to_lowercase())),
            MdsStream::Depth(symbol) => Ok(format!("{}@depth10@500ms", symbol.to_lowercase())),
            MdsStream::BestPrice(symbol) => Ok(format!("{}@bookTicker", symbol.to_lowercase())),
            MdsStream::MarkPrice(symbol) => Ok(format!("{}@markPrice@1s", symbol.to_lowercase())),
            MdsStream::ForceOrder(symbol) => Ok(format!("{}@forceOrder", symbol.to_lowercase())),
            MdsStream::AggDepth { .. } => {
                Err(Error::Mds("AggDepth is not a native stream".into()))
            }
        }
    }

    pub(super) fn stream_to_ours(stream: &str) -> Result<String> {
        let parts = stream.split('@').collect::<Vec<&str>>();
        let (symbol, data_type) = (parts[0], parts[1]);
        let symbol = symbol.to_string().to_uppercase();
        let mds_stream = match data_type {
            "kline_1m" => MdsStream::Kline(symbol),
            "depth10" => MdsStream::Depth(symbol),
            "bookTicker" => MdsStream::BestPrice(symbol),
            "markPrice" => MdsStream::MarkPrice(symbol),
            "forceOrder" => MdsStream::ForceOrder(symbol),
            _ => return Err(Error::Mds(format!("Invalid stream: {stream}"))),
        };
        Ok(mds_stream.to_string())
    }

    fn str_to_order_side(s: &str) -> OrderSide {
        match s {
            "BUY" => OrderSide::Buy,
            "SELL" => OrderSide::Sell,
            _ => OrderSide::Buy,
        }
    }

    fn str_to_order_type(s: &str) -> OrderType {
        match s {
            "LIMIT" => OrderType::Limit,
            "MARKET" => OrderType::Market,
            "STOP" => OrderType::Stop,
            "STOP_MARKET" => OrderType::StopMarket,
            _ => OrderType::Limit,
        }
    }

    fn str_to_time_in_force(s: &str) -> TimeInForce {
        match s {
            "GTC" => TimeInForce::GTC,
            "IOC" => TimeInForce::IOC,
            "FOK" => TimeInForce::FOK,
            "GTX" => TimeInForce::GTX,
            "GTD" => TimeInForce::GTD,
            _ => TimeInForce::GTC,
        }
    }

    fn str_to_order_status(s: &str) -> OrderStatus {
        match s {
            "NEW" => OrderStatus::New,
            "PARTIALLY_FILLED" => OrderStatus::PartiallyFilled,
            "FILLED" => OrderStatus::Filled,
            "CANCELED" => OrderStatus::Canceled,
            "REJECTED" => OrderStatus::Rejected,
            "EXPIRED" => OrderStatus::Expired,
            _ => OrderStatus::New,
        }
    }

    fn str_to_contract_type(s: &str) -> ContractType {
        match s {
            "PERPETUAL" => ContractType::Perpetual,
            "CURRENT_MONTH" => ContractType::CurrentMonth,
            "NEXT_MONTH" => ContractType::NextMonth,
            "CURRENT_QUARTER" => ContractType::CurrentQuarter,
            "NEXT_QUARTER" => ContractType::NextQuarter,
            "PERPETUAL_DELIVERING" => ContractType::PerpetualDelivering,
            _ => ContractType::Perpetual,
        }
    }

    fn str_to_contract_status(s: &str) -> ContractStatus {
        match s {
            "TRADING" => ContractStatus::Trading,
            "PRE_DELIVERING" => ContractStatus::PreDelivering,
            "DELIVERING" => ContractStatus::Delivering,
            "DELIVERED" => ContractStatus::Delivered,
            "PRE_SETTLE" => ContractStatus::PreSettle,
            "SETTLING" => ContractStatus::Settling,
            "CLOSE" => ContractStatus::Close,
            _ => ContractStatus::Trading,
        }
    }

    // ─── REST API 响应结构 ───

    #[derive(Debug, Deserialize)]
    pub struct ExchangeInfoRsp {
        pub symbols: Vec<Symbol>,
    }

    #[derive(Debug, Deserialize)]
    #[serde(rename_all = "camelCase")]
    pub struct Symbol {
        pub symbol: String,
        contract_type: String,
        delivery_date: i64,
        onboard_date: i64,
        status: String,
        base_asset: String,
        quote_asset: String,
        margin_asset: String,
        price_precision: u8,
        quantity_precision: u8,
        base_asset_precision: u8,
        quote_precision: u8,
        filters: Vec<Value>,
    }

    impl From<Symbol> for SymbolInfo {
        fn from(s: Symbol) -> Self {
            let mut info = Self {
                symbol: s.symbol,
                contract_type: str_to_contract_type(s.contract_type.as_str()),
                delivery_date: s.delivery_date,
                onboard_date: s.onboard_date,
                status: str_to_contract_status(s.status.as_str()),
                base_asset: s.base_asset,
                quote_asset: s.quote_asset,
                margin_asset: s.margin_asset,
                price_precision: s.price_precision,
                quantity_precision: s.quantity_precision,
                base_asset_precision: s.base_asset_precision,
                quote_precision: s.quote_precision,
                tick_size: 0.0,
                lot_size: 0.0,
                market_lot_size: 0.0,
                min_notional: 0.0,
            };
            for filter in s.filters {
                if filter.get("filterType").and_then(|v| v.as_str()) == Some("PRICE_FILTER") {
                    info.tick_size = filter
                        .get("tickSize")
                        .and_then(|v| v.as_str())
                        .and_then(|s| s.parse().ok())
                        .unwrap_or(0.0);
                }
                if filter.get("filterType").and_then(|v| v.as_str()) == Some("LOT_SIZE") {
                    info.lot_size = filter
                        .get("stepSize")
                        .and_then(|v| v.as_str())
                        .and_then(|s| s.parse().ok())
                        .unwrap_or(0.0);
                }
                if filter.get("filterType").and_then(|v| v.as_str()) == Some("MARKET_LOT_SIZE") {
                    info.market_lot_size = filter
                        .get("stepSize")
                        .and_then(|v| v.as_str())
                        .and_then(|s| s.parse().ok())
                        .unwrap_or(0.0);
                }
                if filter.get("filterType").and_then(|v| v.as_str()) == Some("MIN_NOTIONAL") {
                    info.min_notional = filter
                        .get("notional")
                        .and_then(|v| v.as_str())
                        .and_then(|s| s.parse().ok())
                        .unwrap_or(0.0);
                }
            }
            info
        }
    }

    /// 深度快照 REST 响应
    #[serde_as]
    #[derive(Debug, Deserialize)]
    pub struct DepthSnapshotRsp {
        #[serde(rename = "lastUpdateId")]
        pub last_update_id: i64,
        #[serde_as(as = "Vec<Vec<DisplayFromStr>>")]
        pub bids: Vec<Vec<f64>>,
        #[serde_as(as = "Vec<Vec<DisplayFromStr>>")]
        pub asks: Vec<Vec<f64>>,
    }

    /// diff depth WS 增量事件
    #[serde_as]
    #[derive(Debug, Deserialize)]
    pub struct DepthDiffEvent {
        #[serde(rename = "s")]
        pub symbol: String,
        #[serde(rename = "T")]
        pub transaction_time: i64,
        /// firstUpdateId
        #[serde(rename = "U")]
        pub first_update_id: i64,
        /// finalUpdateId
        #[serde(rename = "u")]
        pub final_update_id: i64,
        /// 前一事件的 finalUpdateId
        #[serde(rename = "pu")]
        pub prev_final_update_id: i64,
        #[serde(rename = "b")]
        #[serde_as(as = "Vec<Vec<DisplayFromStr>>")]
        pub bids: Vec<Vec<f64>>,
        #[serde(rename = "a")]
        #[serde_as(as = "Vec<Vec<DisplayFromStr>>")]
        pub asks: Vec<Vec<f64>>,
    }

    // ─── WebSocket 消息结构 ───

    #[derive(Debug, Serialize, Deserialize)]
    #[serde(untagged)]
    pub enum WsRsp {
        Result(WsResult),
        Stream(WsStream),
    }

    #[derive(Debug, Serialize, Deserialize)]
    pub struct WsResult {
        pub id: u32,
        pub result: Option<Value>,
        pub msg: Option<String>,
    }

    #[derive(Debug, Serialize, Deserialize)]
    pub struct WsStream {
        pub stream: String,
        pub data: Value,
    }

    #[derive(Debug, Serialize, Deserialize)]
    #[serde(untagged)]
    pub enum StreamData {
        Kline(KlineStream),
        Depth(DepthStream),
        BestPrice(BookTickerStream),
        MarkPrice(MarkPriceStream),
        ForceOrder(ForceOrderStream),
    }

    impl From<StreamData> for Option<RspData> {
        fn from(data: StreamData) -> Self {
            match data {
                inner::StreamData::Kline(ks) => Option::<Kline>::from(ks).map(RspData::Kline),
                inner::StreamData::Depth(d) => Some(RspData::Depth(d.into())),
                inner::StreamData::BestPrice(b) => Some(RspData::BestPrice(b.into())),
                inner::StreamData::MarkPrice(m) => Some(RspData::MarkPrice(m.into())),
                inner::StreamData::ForceOrder(f) => Some(RspData::ForceOrder(f.into())),
            }
        }
    }

    #[serde_as]
    #[derive(Debug, Serialize, Deserialize)]
    pub struct BookTickerStream {
        #[serde(rename = "s")]
        symbol: String,
        #[serde(rename = "T")]
        timestamp: i64,
        #[serde(rename = "b")]
        #[serde_as(as = "DisplayFromStr")]
        bid_price: f64,
        #[serde(rename = "B")]
        #[serde_as(as = "DisplayFromStr")]
        bid_volume: f64,
        #[serde(rename = "a")]
        #[serde_as(as = "DisplayFromStr")]
        ask_price: f64,
        #[serde(rename = "A")]
        #[serde_as(as = "DisplayFromStr")]
        ask_volume: f64,
    }

    impl From<BookTickerStream> for BestPrice {
        fn from(data: BookTickerStream) -> Self {
            Self {
                symbol: data.symbol,
                timestamp: data.timestamp,
                bid_price: data.bid_price,
                bid_volume: data.bid_volume,
                ask_price: data.ask_price,
                ask_volume: data.ask_volume,
            }
        }
    }

    #[derive(Debug, Serialize, Deserialize)]
    pub struct KlineStream {
        #[serde(rename = "s")]
        s: String,
        #[serde(rename = "k")]
        k: KlineStreamData,
    }

    #[serde_as]
    #[derive(Debug, Serialize, Deserialize)]
    pub struct KlineStreamData {
        #[serde(rename = "T")]
        timestamp: i64,
        #[serde(rename = "s")]
        symbol: String,
        #[serde(rename = "i")]
        interval: String,
        #[serde(rename = "o")]
        #[serde_as(as = "DisplayFromStr")]
        open: f64,
        #[serde(rename = "c")]
        #[serde_as(as = "DisplayFromStr")]
        close: f64,
        #[serde(rename = "h")]
        #[serde_as(as = "DisplayFromStr")]
        high: f64,
        #[serde(rename = "l")]
        #[serde_as(as = "DisplayFromStr")]
        low: f64,
        #[serde(rename = "v")]
        #[serde_as(as = "DisplayFromStr")]
        volume: f64,
        #[serde(rename = "n")]
        trades: i64,
        #[serde(rename = "x")]
        is_closed: bool,
        #[serde(rename = "q")]
        #[serde_as(as = "DisplayFromStr")]
        amount: f64,
    }

    impl From<KlineStream> for Option<Kline> {
        fn from(data: KlineStream) -> Self {
            let k = &data.k;
            if k.is_closed {
                Some(Kline {
                    symbol: k.symbol.clone(),
                    open: k.open,
                    high: k.high,
                    low: k.low,
                    close: k.close,
                    volume: k.volume,
                    quote_volume: k.amount,
                    timestamp: k.timestamp,
                })
            } else {
                None
            }
        }
    }

    #[serde_as]
    #[derive(Debug, Serialize, Deserialize)]
    pub struct DepthStream {
        #[serde(rename = "s")]
        s: String,
        #[serde(rename = "T")]
        timestamp: i64,
        #[serde(rename = "a")]
        #[serde_as(as = "Vec<Vec<DisplayFromStr>>")]
        a: Vec<Vec<f64>>,
        #[serde(rename = "b")]
        #[serde_as(as = "Vec<Vec<DisplayFromStr>>")]
        b: Vec<Vec<f64>>,
    }

    impl From<DepthStream> for Depth {
        fn from(data: DepthStream) -> Self {
            Self {
                symbol: data.s,
                asks: data.a.into_iter().map(|p| (p[0], p[1])).collect(),
                bids: data.b.into_iter().map(|p| (p[0], p[1])).collect(),
                timestamp: data.timestamp,
            }
        }
    }

    #[serde_as]
    #[derive(Debug, Serialize, Deserialize)]
    pub struct MarkPriceStream {
        #[serde(rename = "s")]
        symbol: String,
        #[serde(rename = "E")]
        timestamp: i64,
        #[serde_as(as = "DisplayFromStr")]
        #[serde(rename = "p")]
        mark_price: f64,
        #[serde(rename = "i")]
        #[serde_as(as = "DisplayFromStr")]
        index_price: f64,
        #[serde(rename = "P")]
        #[serde_as(as = "DisplayFromStr")]
        estimated_settle_price: f64,
        #[serde(rename = "r")]
        #[serde_as(as = "DisplayFromStr")]
        funding_rate: f64,
        #[serde(rename = "T")]
        next_funding_time: i64,
    }

    impl From<MarkPriceStream> for MarkPrice {
        fn from(data: MarkPriceStream) -> Self {
            Self {
                symbol: data.symbol,
                timestamp: data.timestamp,
                mark_price: data.mark_price,
                index_price: data.index_price,
                estimated_settle_price: data.estimated_settle_price,
                funding_rate: data.funding_rate,
                next_funding_time: data.next_funding_time,
            }
        }
    }

    #[derive(Debug, Serialize, Deserialize)]
    pub struct ForceOrderStream {
        #[serde(rename = "E")]
        timestamp: i64,
        #[serde(rename = "o")]
        order: ForceOrderStreamData,
    }

    #[serde_as]
    #[derive(Debug, Serialize, Deserialize)]
    struct ForceOrderStreamData {
        #[serde(rename = "s")]
        symbol: String,
        #[serde(rename = "S")]
        side: String,
        #[serde(rename = "o")]
        order_type: String,
        #[serde(rename = "f")]
        time_in_force: String,
        #[serde(rename = "q")]
        #[serde_as(as = "DisplayFromStr")]
        quantity: f64,
        #[serde(rename = "p")]
        #[serde_as(as = "DisplayFromStr")]
        price: f64,
        #[serde(rename = "ap")]
        #[serde_as(as = "DisplayFromStr")]
        average_price: f64,
        #[serde(rename = "X")]
        status: String,
        #[serde(rename = "l")]
        #[serde_as(as = "DisplayFromStr")]
        last_filled_quantity: f64,
        #[serde(rename = "z")]
        #[serde_as(as = "DisplayFromStr")]
        filled_quantity: f64,
        #[serde(rename = "T")]
        timestamp: i64,
    }

    impl From<ForceOrderStream> for ForceOrder {
        fn from(data: ForceOrderStream) -> Self {
            Self {
                symbol: data.order.symbol,
                side: str_to_order_side(data.order.side.as_str()),
                order_type: str_to_order_type(data.order.order_type.as_str()),
                time_in_force: str_to_time_in_force(data.order.time_in_force.as_str()),
                quantity: data.order.quantity,
                price: data.order.price,
                average_price: data.order.average_price,
                status: str_to_order_status(data.order.status.as_str()),
                last_filled_quantity: data.order.last_filled_quantity,
                filled_quantity: data.order.filled_quantity,
                timestamp: data.timestamp,
            }
        }
    }
}
