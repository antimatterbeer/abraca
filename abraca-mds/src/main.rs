#[tokio::main]
async fn main() -> abraca_mds::Result<()> {
    abraca_base::utils::setup_logger(env!("CARGO_BIN_NAME"));
    let server = abraca_mds::Server::new();
    server.run(8080).await?;
    Ok(())
}
