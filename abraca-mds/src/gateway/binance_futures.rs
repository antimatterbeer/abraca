use crate::{
    channel::{ReqData, ReqReceiver, RspData, RspSender},
    error::Result,
};
use abraca_base::prelude::*;
use futures::{SinkExt, StreamExt};
use reqwest::Client;
use serde_json::json;
use std::collections::HashMap;
use tokio_tungstenite::tungstenite::Message;

const BASE_URL: &str = "https://fapi.binance.com";

pub struct BinanceFutures {
    rx: ReqReceiver,
    tx: RspSender,
    http_client: Client,
    req_id: u32,                               // 请求ID
    id_map: HashMap<u32, u32>,                 // 请求ID映射
    symbol_infos: HashMap<String, SymbolInfo>, // 交易对信息
}

impl BinanceFutures {
    pub fn new(rx: ReqReceiver, tx: RspSender) -> Self {
        Self {
            rx,
            tx,
            http_client: Client::new(),
            req_id: 0,
            id_map: HashMap::new(),
            symbol_infos: HashMap::new(),
        }
    }

    pub async fn run(mut self) -> Result<()> {
        tracing::info!("Binance futures market started");
        self.get_symbol_infos().await?;
        let (ws_stream, _) =
            tokio_tungstenite::connect_async("wss://fstream.binance.com/stream").await?;
        tracing::info!("Connected to Binance Futures");
        let (mut writer, mut reader) = ws_stream.split();
        loop {
            tokio::select! {
                Some(req) = self.rx.recv() => {
                    self.handle_req(&mut writer, req).await?;
                }
                Some(Ok(msg)) = reader.next() => {
                    if let Message::Text(text) = msg {
                        self.handle_ws_msg(&text).await;
                    }
                }
            }
        }
    }

    /// 处理客户端请求
    async fn handle_req<W>(&mut self, writer: &mut W, req: Request<ReqData>) -> Result<()>
    where
        W: SinkExt<Message> + Unpin,
        W::Error: Into<crate::error::Error>,
    {
        match req.data {
            ReqData::Subscribe(topics) => {
                let params = topics
                    .iter()
                    .map(|topic| inner::topic_to_stream_name(topic))
                    .filter_map(Result::ok)
                    .collect::<Vec<String>>();
                let data = json!({
                    "method": "SUBSCRIBE",
                    "params": params,
                    "id": self.req_id,
                });
                writer
                    .send(Message::Text(data.to_string().into()))
                    .await
                    .map_err(Into::into)?;
                tracing::info!("Subscribed to topics: {:?}", topics);
                self.id_map.insert(self.req_id, req.id);
                self.req_id += 1;
            }
            ReqData::Unsubscribe(topics) => {
                let params = topics
                    .iter()
                    .map(|topic| inner::topic_to_stream_name(topic))
                    .filter_map(Result::ok)
                    .collect::<Vec<String>>();
                let data = json!({
                    "method": "UNSUBSCRIBE",
                    "params": params,
                    "id": self.req_id,
                });
                writer
                    .send(Message::Text(data.to_string().into()))
                    .await
                    .map_err(Into::into)?;
                tracing::info!("Unsubscribed from topics: {:?}", topics);
                self.id_map.insert(self.req_id, req.id);
                self.req_id += 1;
            }
            ReqData::GetSymbolInfo(symbols) => {
                let infos = self
                    .symbol_infos
                    .iter()
                    .filter_map(|(symbol, info)| {
                        if symbols.contains(symbol) {
                            Some(info.clone())
                        } else {
                            None
                        }
                    })
                    .collect::<Vec<SymbolInfo>>();
                let rsp = Response::<RspData> {
                    exchange: Exchange::BinanceFutures,
                    id: Some(req.id),
                    timestamp: chrono::Utc::now().timestamp_millis(),
                    data: RspData::SymbolInfos(infos),
                };
                let _ = self.tx.send(rsp).await;
            }
        }
        Ok(())
    }

    /// 处理 WebSocket 下行消息
    async fn handle_ws_msg(&mut self, text: &str) {
        match serde_json::from_str::<inner::WsRsp>(text) {
            Ok(inner::WsRsp::Result(result)) => {
                if let Some(id) = self.id_map.remove(&result.id) {
                    if let Some(msg) = result.msg {
                        let rsp = Response::<RspData> {
                            exchange: Exchange::BinanceFutures,
                            timestamp: chrono::Utc::now().timestamp_millis(),
                            id: Some(id),
                            data: RspData::Error(msg),
                        };
                        let _ = self.tx.send(rsp).await;
                    }
                } else {
                    tracing::error!("id not found: {}", result.id);
                }
            }
            Ok(inner::WsRsp::Stream(inner::WsStream { stream: _, data })) => {
                if let Ok(data) = serde_json::from_value::<inner::StreamData>(data)
                    && let Some(rsp_data) = data.into()
                {
                    let rsp = Response::<RspData> {
                        exchange: Exchange::BinanceFutures,
                        timestamp: chrono::Utc::now().timestamp_millis(),
                        id: None,
                        data: rsp_data,
                    };
                    let _ = self.tx.send(rsp).await;
                }
            }
            Err(_) => tracing::error!("Invalid message: {text}"),
        }
    }

    /// 获取交易对信息
    async fn get_symbol_infos(&mut self) -> Result<()> {
        let rsp = self
            .http_client
            .get(format!("{}/fapi/v1/exchangeInfo", BASE_URL))
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
}

mod inner {
    use super::*;
    use crate::error::Error;
    use serde::{Deserialize, Serialize};
    use serde_json::Value;
    use serde_with::{DisplayFromStr, serde_as};

    pub fn topic_to_stream_name(topic: &str) -> Result<String> {
        let parts = topic.split('@').collect::<Vec<&str>>();
        if parts.len() != 2 {
            return Err(Error::Mds(format!(
                "Invalid topic: {}. expected: <symbol>@<stream>",
                topic
            )));
        }
        let (symbol, data_type) = (parts[0], parts[1]);
        let symbol = symbol.to_lowercase();
        match data_type {
            "Depth" => Ok(format!("{symbol}@depth10@500ms")),
            "Kline" => Ok(format!("{symbol}@kline_1m")),
            "BestPrice" => Ok(format!("{symbol}@bookTicker")),
            "MarkPrice" => Ok(format!("{symbol}@markPrice@1s")),
            _ => Err(Error::Mds(format!(
                "Invalid topic: {topic}. Unsupported data type: {data_type}"
            ))),
        }
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
