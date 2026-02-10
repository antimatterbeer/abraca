#[tokio::main]
async fn main() -> abraca::error::Result<()> {
    env_logger::Builder::from_default_env()
        .filter_level(log::LevelFilter::Info)
        .init();
    let abraca = abraca::Abraca::new();
    abraca.run(8080).await?;
    Ok(())
}
