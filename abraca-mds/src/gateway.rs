use crate::{
    channel::{ReqSender, RspSender, channel},
    error::{Error, Result},
};
use abraca_base::prelude::Exchange;

mod binance_futures;

pub async fn start(exchange: Exchange, rsp_tx: RspSender) -> Result<ReqSender> {
    let (req_tx, req_rx) = channel(1024);
    match exchange {
        Exchange::BinanceFutures => {
            tokio::spawn(async move {
                let mg = binance_futures::BinanceFutures::new(req_rx, rsp_tx);
                if let Err(e) = mg.run().await {
                    tracing::error!("Error running binance futures: {}", e);
                }
            });
            Ok(req_tx)
        }
        _ => Err(Error::Mds(format!("Unsupported exchange: {:?}", exchange))),
    }
}
