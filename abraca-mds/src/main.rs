use clap::Parser;

#[derive(Debug, Parser)]
struct Args {
    /// 监听端口
    #[clap(short, long, default_value = "8080", help = "监听端口")]
    port: u16,
}

#[tokio::main]
async fn main() -> abraca_mds::error::Result<()> {
    abraca_base::log::setup_logger(env!("CARGO_BIN_NAME"));
    let args = Args::parse();
    let abraca = abraca_mds::mds::Mds::new();
    abraca.run(args.port).await?;
    Ok(())
}
