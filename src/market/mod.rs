use crate::prelude::*;

mod binance_futures;

#[allow(unused)]
pub async fn start_mg(exchange: Exchange, rsp_tx: RspSender) -> Result<ReqSender> {
    let (req_tx, req_rx) = tokio::sync::mpsc::channel(1024);
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
        _ => Err(Error::Market(format!("Unsupported exchange: {:?}", exchange))),
    }
}
