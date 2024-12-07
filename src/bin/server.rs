//! mini-redis 服务器。
//!
//! 这个文件是该库中实现的服务器的入口点。
//! 它执行命令行解析，并将参数传递给 `mini_redis::server`。
//!
//! `clap` 库用于解析参数。use mini_redis::{server, DEFAULT_PORT};

use mini_redis::{server, DEFAULT_PORT};
use clap::Parser;
use tokio::net::TcpListener;
use tokio::signal;

#[cfg(feature = "otel")]
// 为了能够设置 XrayPropagator
use opentelemetry::global;
#[cfg(feature = "otel")]
// 用于配置某些选项，例如采样率
use opentelemetry::sdk::trace as sdktrace;
#[cfg(feature = "otel")]
// 用于跨服务传递相同的 XrayId
use opentelemetry_aws::trace::XrayPropagator;
#[cfg(feature = "otel")]
// `Ext` 特性用于使注册表接受 OpenTelemetry 特定类型（例如 `OpenTelemetryLayer`）
use tracing_subscriber::{
    fmt, layer::SubscriberExt, util::SubscriberInitExt, util::TryInitError, EnvFilter,
};

#[tokio::main]
pub async fn main() -> mini_redis::Result<()> {
    set_up_logging()?;

    let cli = Cli::parse();
    let port = cli.port.unwrap_or(DEFAULT_PORT);

    // Bind a TCP listener
    let listener = TcpListener::bind(&format!("127.0.0.1:{}", port)).await?;

    server::run(listener, signal::ctrl_c()).await;

    Ok(())
}

#[derive(Parser, Debug)]
#[command(name = "mini-redis-server", version, author, about = "A Redis server")]
struct Cli {
    #[arg(long)]
    port: Option<u16>,
}

#[cfg(not(feature = "otel"))]
fn set_up_logging() -> mini_redis::Result<()> {
    // See https://docs.rs/tracing for more info
    tracing_subscriber::fmt::try_init()
}

#[cfg(feature = "otel")]
fn set_up_logging() -> Result<(), TryInitError> {
    // 将全局传播器设置为 X-Ray 传播器
    // 注意：如果您需要在同一个追踪中跨服务传递 x-amzn-trace-id，
    // 您将需要这行代码。但是，这需要额外的代码，这里没有展示。
    // 有关使用 hyper 的完整示例，请参见：
    // https://github.com/open-telemetry/opentelemetry-rust/blob/v0.19.0/examples/aws-xray/src/server.rs#L14-L26
    global::set_text_map_propagator(XrayPropagator::default());

    let tracer = opentelemetry_otlp::new_pipeline()
        .tracing()
        .with_exporter(opentelemetry_otlp::new_exporter().tonic())
        .with_trace_config(
            sdktrace::config()
                .with_sampler(sdktrace::Sampler::AlwaysOn)
                // Needed in order to convert the trace IDs into an Xray-compatible format
                .with_id_generator(sdktrace::XrayIdGenerator::default()),
        )
        .install_simple()
        .expect("Unable to initialize OtlpPipeline");

    // 使用配置的追踪器创建一个跟踪层
    let opentelemetry = tracing_opentelemetry::layer().with_tracer(tracer);

    // 从 `RUST_LOG` 环境变量解析 `EnvFilter` 配置。
    let filter = EnvFilter::from_default_env();

    // 使用跟踪订阅者 `Registry`，或任何其他实现 `LookupSpan` 的订阅者
    tracing_subscriber::registry()
        .with(opentelemetry)
        .with(filter)
        .with(fmt::Layer::default())
        .try_init()
}
