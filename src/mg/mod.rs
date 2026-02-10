use crate::{
    common::{Exchange, ReqReceiver, ReqSender, RspSender},
    error::{Error, Result},
};
use async_trait::async_trait;

mod binance_futures;

#[async_trait]
pub trait Mg {
    fn new(rx: ReqReceiver, tx: RspSender) -> Self;
    async fn run(&mut self) -> Result<()>;
}

#[allow(unused)]
pub async fn start_mg(exchange: Exchange, rsp_tx: RspSender) -> Result<ReqSender> {
    let (req_tx, req_rx) = tokio::sync::mpsc::channel(1024);
    match exchange {
        Exchange::BinanceFutures => {
            tokio::spawn(async move {
                let mg = binance_futures::BinanceFutures::new(req_rx, rsp_tx);
                if let Err(e) = mg.run().await {
                    log::error!("Error running binance futures: {}", e);
                }
            });
            Ok(req_tx)
        }
        _ => Err(Error::Mg(format!("Unsupported exchange: {:?}", exchange))),
    }
}
