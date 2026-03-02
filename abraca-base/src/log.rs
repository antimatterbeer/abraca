use tracing_subscriber::{
    EnvFilter, fmt::time::ChronoLocal, layer::SubscriberExt, util::SubscriberInitExt,
};

pub fn setup_logger(filename: &str) {
    let env_filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let log_date_format = "%Y-%m-%d %H:%M:%S";
    let timer = ChronoLocal::new(log_date_format.to_string());
    let file_layer = tracing_subscriber::fmt::layer()
        .compact()
        .with_ansi(false)
        .with_target(false)
        .with_timer(timer.clone())
        .with_writer(tracing_appender::rolling::daily("./logs", filename));
    let console_layer = tracing_subscriber::fmt::layer()
        .compact()
        .with_target(false)
        .with_timer(timer)
        .with_writer(std::io::stdout);

    tracing_subscriber::registry()
        .with(env_filter) // 统一使用 EnvFilter
        .with(file_layer)
        .with(console_layer)
        .init();
}
