use crate::{
    common::{
        Depth, Exchange, Kline, MgReqData, MgResult, MgRsp, MgUpdate, MgUpdateData, ReqReceiver,
        RspSender,
    },
    error::Result,
};
use futures::{SinkExt, StreamExt};
use serde_json::json;
use std::collections::HashMap;
use tokio_tungstenite::tungstenite::Message;

pub struct BinanceFutures {
    rx: ReqReceiver,
    tx: RspSender,
    // 请求ID
    req_id: u32,
    id_map: HashMap<u32, u32>,
}

impl BinanceFutures {
    pub fn new(rx: ReqReceiver, tx: RspSender) -> Self {
        Self {
            rx,
            tx,
            req_id: 0,
            id_map: HashMap::new(),
        }
    }

    pub async fn run(mut self) -> Result<()> {
        tracing::info!("Binance futures mg started");
        let (ws_stream, _) =
            tokio_tungstenite::connect_async("wss://fstream.binance.com/stream").await?;
        tracing::info!("Connected to Binance Futures");
        let (mut writer, mut reader) = ws_stream.split();
        loop {
            tokio::select! {
                Some(req) = self.rx.recv() => {
                    match req.data {
                        MgReqData::Subscribe(topics) => {
                            let data = json!({
                                "method": "SUBSCRIBE",
                                "params": topics,
                                "id": self.req_id,
                            });
                            writer.send(Message::Text(data.to_string().into())).await?;
                            tracing::info!("Subscribed to topics: {:?}", topics);
                        }
                        MgReqData::Unsubscribe(topics) => {
                            let data = json!({
                                "method": "UNSUBSCRIBE",
                                "params": topics,
                                "id": self.req_id,
                            });
                            writer.send(Message::Text(data.to_string().into())).await?;
                            tracing::info!("Unsubscribed from topics: {:?}", topics);
                        }
                    }
                    self.id_map.insert(self.req_id, req.id);
                    self.req_id += 1;
                }
                Some(Ok(msg)) = reader.next() => {
                    if let Message::Text(text) = msg {
                        match serde_json::from_str::<inner::Rsp>(&text){
                            Ok(inner::Rsp::Result(result)) => {
                                if let Some(id) = self.id_map.remove(&result.id) {
                                    let rsp = MgRsp::Result(MgResult {
                                        id,
                                        timestamp: chrono::Utc::now().timestamp_millis(),
                                        error: None,
                                        result: result.result.is_null(),
                                    });
                                    let _ = self.tx.send(rsp).await;
                                }else{
                                    tracing::error!("id not found: {}", result.id);
                                }
                            }
                            Ok(inner::Rsp::Stream(inner::Stream { stream, data })) => {
                                if let Ok(data) = serde_json::from_value::<inner::StreamData>(data){
                                    match data{
                                        inner::StreamData::Kline(kline) => {
                                            let rsp = MgRsp::Update(MgUpdate {
                                                exchange: Exchange::BinanceFutures,
                                                topic: stream,
                                                timestamp: chrono::Utc::now().timestamp_millis(),
                                                data: MgUpdateData::Kline(kline.into()),
                                            });
                                            let _ = self.tx.send(rsp).await;
                                        }
                                        inner::StreamData::Depth(depth) => {
                                            let rsp = MgRsp::Update(MgUpdate {
                                                exchange: Exchange::BinanceFutures,
                                                topic: stream,
                                                timestamp: chrono::Utc::now().timestamp_millis(),
                                                data: MgUpdateData::Depth(depth.into()),
                                            });
                                            let _ = self.tx.send(rsp).await;
                                        }
                                    }
                                }
                            }
                            _ => tracing::error!("Invalid message: {text}"),
                        }
                    }
                }
            }
        }
    }
}

mod inner {
    #![allow(unused)]

    use super::*;
    use serde::{Deserialize, Serialize};
    use serde_json::Value;

    #[derive(Debug, Serialize, Deserialize)]
    #[serde(untagged)]
    pub enum Rsp {
        Result(Result),
        Stream(Stream),
    }

    #[derive(Debug, Serialize, Deserialize)]
    pub struct Result {
        pub id: u32,
        pub result: Value,
    }

    #[derive(Debug, Serialize, Deserialize)]
    pub struct Stream {
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

    impl From<KlineStream> for Kline {
        fn from(data: KlineStream) -> Self {
            let parse = |s: &str| s.parse().unwrap_or(0.0);
            let k = &data.k;
            Self {
                symbol: k.symbol.clone(),
                open: parse(&k.open),
                high: parse(&k.high),
                low: parse(&k.low),
                close: parse(&k.close),
                volume: parse(&k.volume),
                amount: parse(&k.amount),
                timestamp: k.timestamp,
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

        #[test]
        fn test_deserialize_rsp() {
            let json = include_str!("../../fixtures/binance_futures/subscribe_result.json");
            let rsp: Rsp = serde_json::from_str(json).unwrap();
            assert!(matches!(
                rsp,
                Rsp::Result(Result {
                    id: 0,
                    result: Value::Null
                })
            ));
        }

        #[test]
        fn test_deserialize_kline_stream() {
            let json = include_str!("../../fixtures/binance_futures/kline.json");
            let rsp: Rsp = serde_json::from_str(json).unwrap();
            if let Rsp::Stream(Stream { stream, data }) = rsp {
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
            let rsp: Rsp = serde_json::from_str(json).unwrap();
            if let Rsp::Stream(Stream { stream, data }) = rsp {
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
