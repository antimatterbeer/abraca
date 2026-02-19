use crate::prelude::*;
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
    async fn handle_req<W>(&mut self, writer: &mut W, req: MarketReq) -> Result<()>
    where
        W: SinkExt<Message> + Unpin,
        W::Error: Into<crate::error::Error>,
    {
        match req.data {
            MarketReqData::Subscribe(topics) => {
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
            MarketReqData::Unsubscribe(topics) => {
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
            MarketReqData::GetSymbolInfo(symbols) => {
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
                let rsp = MarketRsp {
                    exchange: Exchange::BinanceFutures,
                    id: Some(req.id),
                    timestamp: chrono::Utc::now().timestamp_millis(),
                    data: MarketRspData::SymbolInfos(infos),
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
                        let rsp = MarketRsp {
                            exchange: Exchange::BinanceFutures,
                            timestamp: chrono::Utc::now().timestamp_millis(),
                            id: Some(id),
                            data: MarketRspData::Error(msg),
                        };
                        let _ = self.tx.send(rsp).await;
                    }
                } else {
                    tracing::error!("id not found: {}", result.id);
                }
            }
            Ok(inner::WsRsp::Stream(inner::WsStream { stream: _, data })) => {
                if let Ok(data) = serde_json::from_value::<inner::StreamData>(data) {
                    match data {
                        inner::StreamData::Kline(ks) => {
                            if let Some(kline) = Option::<Kline>::from(ks) {
                                let rsp = MarketRsp {
                                    exchange: Exchange::BinanceFutures,
                                    timestamp: chrono::Utc::now().timestamp_millis(),
                                    id: None,
                                    data: MarketRspData::Kline(kline),
                                };
                                let _ = self.tx.send(rsp).await;
                            }
                        }
                        inner::StreamData::Depth(depth) => {
                            let rsp = MarketRsp {
                                exchange: Exchange::BinanceFutures,
                                timestamp: chrono::Utc::now().timestamp_millis(),
                                id: None,
                                data: MarketRspData::Depth(depth.into()),
                            };
                            let _ = self.tx.send(rsp).await;
                        }
                    }
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
    #![allow(unused)]

    use super::*;
    use serde::{Deserialize, Serialize};
    use serde_json::Value;

    pub fn topic_to_stream_name(topic: &str) -> Result<String> {
        let parts = topic.split('@').collect::<Vec<&str>>();
        if parts.len() != 2 {
            return Err(Error::Market(format!(
                "Invalid topic: {}. expected: <symbol>@<stream>",
                topic
            )));
        }
        let (symbol, data_type) = (parts[0], parts[1]);
        let symbol = symbol.to_lowercase();
        match data_type {
            "Depth" => Ok(format!("{symbol}@depth5@500ms")),
            "Kline" => Ok(format!("{symbol}@kline_1m")),
            _ => Err(Error::Market(format!(
                "Invalid stream: {}. expected: Depth or Kline",
                data_type
            ))),
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
                contract_type: match s.contract_type.as_str() {
                    "PERPETUAL" => ContractType::Perpetual,
                    "CURRENT_MONTH" => ContractType::CurrentMonth,
                    "NEXT_MONTH" => ContractType::NextMonth,
                    "CURRENT_QUARTER" => ContractType::CurrentQuarter,
                    "NEXT_QUARTER" => ContractType::NextQuarter,
                    "PERPETUAL_DELIVERING" => ContractType::PerpetualDelivering,
                    _ => ContractType::Perpetual,
                },
                delivery_date: s.delivery_date,
                onboard_date: s.onboard_date,
                status: match s.status.as_str() {
                    "TRADING" => ContractStatus::Trading,
                    "PRE_DELIVERING" => ContractStatus::PreDelivering,
                    "DELIVERING" => ContractStatus::Delivering,
                    "DELIVERED" => ContractStatus::Delivered,
                    "PRE_SETTLE" => ContractStatus::PreSettle,
                    "SETTLING" => ContractStatus::Settling,
                    "CLOSE" => ContractStatus::Close,
                    _ => ContractStatus::Trading,
                },
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
    }

    #[derive(Debug, Serialize, Deserialize)]
    pub struct KlineStream {
        #[serde(rename = "s")]
        s: String,
        #[serde(rename = "k")]
        k: KlineStreamData,
    }

    #[derive(Debug, Serialize, Deserialize)]
    pub struct KlineStreamData {
        #[serde(rename = "T")]
        timestamp: i64,
        #[serde(rename = "s")]
        symbol: String,
        #[serde(rename = "i")]
        interval: String,
        #[serde(rename = "o")]
        open: String,
        #[serde(rename = "c")]
        close: String,
        #[serde(rename = "h")]
        high: String,
        #[serde(rename = "l")]
        low: String,
        #[serde(rename = "v")]
        volume: String,
        #[serde(rename = "n")]
        trades: i64,
        #[serde(rename = "x")]
        is_closed: bool,
        #[serde(rename = "q")]
        amount: String,
    }

    impl From<KlineStream> for Option<Kline> {
        fn from(data: KlineStream) -> Self {
            let parse = |s: &str| s.parse().unwrap_or(0.0);
            let k = &data.k;
            if k.is_closed {
                Some(Kline {
                    symbol: k.symbol.clone(),
                    open: parse(&k.open),
                    high: parse(&k.high),
                    low: parse(&k.low),
                    close: parse(&k.close),
                    volume: parse(&k.volume),
                    quote_volume: parse(&k.amount),
                    timestamp: k.timestamp,
                })
            } else {
                None
            }
        }
    }

    #[derive(Debug, Serialize, Deserialize)]
    pub struct DepthStream {
        #[serde(rename = "s")]
        s: String,
        #[serde(rename = "T")]
        timestamp: i64,
        #[serde(rename = "a")]
        a: Vec<Vec<String>>,
        #[serde(rename = "b")]
        b: Vec<Vec<String>>,
    }

    impl From<DepthStream> for Depth {
        fn from(data: DepthStream) -> Self {
            let parse_pair = |p: Vec<String>| {
                let price: f64 = p[0].parse().unwrap_or(0.0);
                let qty: f64 = p[1].parse().unwrap_or(0.0);
                (price, qty)
            };
            Self {
                symbol: data.s,
                asks: data.a.into_iter().map(parse_pair).collect(),
                bids: data.b.into_iter().map(parse_pair).collect(),
                timestamp: data.timestamp,
            }
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use crate::prelude::*;

        #[test]
        fn test_deserialize_exchange_info_rsp() -> Result<()> {
            let json = include_str!("../../fixtures/binance_futures/exchangeInfo.json");
            let rsp: ExchangeInfoRsp = serde_json::from_str(json)?;
            let symbols: Vec<SymbolInfo> = rsp.symbols.into_iter().map(Symbol::into).collect();
            println!("{:?}", symbols[0]);
            Ok(())
        }

        #[test]
        fn test_deserialize_rsp() {
            let json = include_str!("../../fixtures/binance_futures/subscribe_result.json");
            let rsp: WsRsp = serde_json::from_str(json).unwrap();
            assert!(matches!(
                rsp,
                WsRsp::Result(WsResult {
                    id: 0,
                    result: None,
                    msg: None,
                })
            ));
        }

        #[test]
        fn test_deserialize_kline_stream() {
            let json = include_str!("../../fixtures/binance_futures/kline.json");
            let rsp: WsRsp = serde_json::from_str(json).unwrap();
            if let WsRsp::Stream(WsStream { stream, data }) = rsp {
                assert_eq!(stream, "btcusdt@kline_1m");
                if let Ok(data) = serde_json::from_value::<StreamData>(data) {
                    assert!(matches!(
                        data,
                        StreamData::Kline(KlineStream { s: _, k: _ })
                    ));
                }
            }
        }

        #[test]
        fn test_deserialize_depth_stream() {
            let json = include_str!("../../fixtures/binance_futures/depth.json");
            let rsp: WsRsp = serde_json::from_str(json).unwrap();
            if let WsRsp::Stream(WsStream { stream, data }) = rsp {
                assert_eq!(stream, "btcusdt@depth5@500ms");
                if let Ok(data) = serde_json::from_value::<StreamData>(data) {
                    assert!(matches!(
                        data,
                        StreamData::Depth(DepthStream {
                            s: _,
                            timestamp: _,
                            a: _,
                            b: _
                        })
                    ));
                }
            }
        }
    }
}
