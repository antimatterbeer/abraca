use crate::{
    MdsEvent, ReqData,
    error::{Error, Result},
};
use abraca_base::{message::Request, types::Exchange};
use tokio::sync::mpsc::Sender;

mod binance_futures;

pub async fn start_gateway(
    exchange: Exchange,
    event_tx: Sender<MdsEvent>,
) -> Result<Sender<Request<ReqData>>> {
    let (req_tx, req_rx) = tokio::sync::mpsc::channel(1024);
    match exchange {
        Exchange::BinanceFutures => {
            let gw = binance_futures::BinanceFutures::new(req_rx, event_tx);
            tokio::spawn(async move {
                if let Err(e) = gw.run().await {
                    tracing::error!("Error running binance futures: {}", e);
                }
            });
            Ok(req_tx)
        }
        _ => Err(Error::Mds(format!("Unsupported exchange: {:?}", exchange))),
    }
}

#[derive(Debug, Hash, Eq, PartialEq)]
pub enum MdsStream {
    Kline(String),
    Depth(String),
    BestPrice(String),
    MarkPrice(String),
    ForceOrder(String),
    AggDepth {
        symbol: String,
        decimal_places: u32,
        depth: usize,
    },
}

impl std::fmt::Display for MdsStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Kline(symbol) => write!(f, "{}@Kline", symbol),
            Self::Depth(symbol) => write!(f, "{}@Depth", symbol),
            Self::BestPrice(symbol) => write!(f, "{}@BestPrice", symbol),
            Self::MarkPrice(symbol) => write!(f, "{}@MarkPrice", symbol),
            Self::ForceOrder(symbol) => write!(f, "{}@ForceOrder", symbol),
            Self::AggDepth {
                symbol,
                decimal_places,
                depth,
            } => write!(f, "{}@AggDepth:{}:{}", symbol, decimal_places, depth),
        }
    }
}

impl std::str::FromStr for MdsStream {
    type Err = Error;
    fn from_str(s: &str) -> Result<Self> {
        let parts = s.split('@').collect::<Vec<&str>>();
        if parts.len() < 2 {
            return Err(Error::Mds(format!("Invalid stream: {s}")));
        }
        let (symbol, data_type) = (parts[0], parts[1]);
        let symbol = symbol.to_string();
        if let Some(rest) = data_type.strip_prefix("AggDepth:") {
            let sub_parts: Vec<&str> = rest.split(':').collect();
            if sub_parts.len() != 2 {
                return Err(Error::Mds(format!("Invalid AggDepth format: {s}")));
            }
            let decimal_places: u32 = sub_parts[0]
                .parse()
                .map_err(|_| Error::Mds(format!("Invalid AggDepth decimal_places: {s}")))?;
            let depth: usize = sub_parts[1]
                .parse()
                .map_err(|_| Error::Mds(format!("Invalid AggDepth depth: {s}")))?;
            return Ok(Self::AggDepth {
                symbol,
                decimal_places,
                depth,
            });
        }
        match data_type {
            "Kline" => Ok(Self::Kline(symbol)),
            "Depth" => Ok(Self::Depth(symbol)),
            "BestPrice" => Ok(Self::BestPrice(symbol)),
            "MarkPrice" => Ok(Self::MarkPrice(symbol)),
            "ForceOrder" => Ok(Self::ForceOrder(symbol)),
            _ => Err(Error::Mds(format!("Invalid stream: {s}"))),
        }
    }
}
