use mini_redis::{clients::Client, DEFAULT_PORT};

use bytes::Bytes;
use clap::{Parser, Subcommand};
use std::num::ParseIntError;
use std::str;
use std::time::Duration;

#[derive(Parser, Debug)]
#[command(
    name = "mini-redis-cli",
    version,
    author,
    about = "Issue Redis commands"
)]
struct Cli {
    #[clap(subcommand)]
    command: Command,

    #[arg(id = "hostname", long, default_value = "127.0.0.1")]
    host: String,

    #[arg(long, default_value_t = DEFAULT_PORT)]
    port: u16,
}

#[derive(Subcommand, Debug)]
enum Command {
    Ping {
        /// 要 ping 的消息
        msg: Option<Bytes>,
    },
    /// 获取键的值。
    Get {
        /// 要获取的键的名称
        key: String,
    },
    /// 设置键以保存字符串值。
    Set {
        /// 要设置的键的名称
        key: String,

        /// 要设置的值。
        value: Bytes,

        /// 在指定时间后使该值过期
        #[arg(value_parser = duration_from_ms_str)]
        expires: Option<Duration>,
    },
    /// 发布者向特定频道发送消息。
    Publish {
        /// 频道的名称
        channel: String,

        /// 要发布的消息
        message: Bytes,
    },
    /// 订阅一个客户端到特定的频道或频道列表。
    Subscribe {
        /// 特定的频道或频道列表
        channels: Vec<String>,
    },
}

/// CLI 工具的入口点。
///
/// `[tokio::main]` 注解表示当函数被调用时应该启动 Tokio 运行时。函数体将在新创建的运行时中执行。
///
/// 这里使用 `flavor = "current_thread"` 是为了避免生成后台线程。CLI 工具用例更有利于轻量，而不是多线程。
#[tokio::main(flavor = "current_thread")]
async fn main() -> mini_redis::Result<()> {
    // 启用日志记录
    tracing_subscriber::fmt::try_init()?;

    // 解析命令行参数
    let cli = Cli::parse();

    // 获取要连接的远程地址
    let addr = format!("{}:{}", cli.host, cli.port);

    // 建立连接
    let mut client = Client::connect(&addr).await?;

    // 处理请求的命令
    match cli.command {
        Command::Ping { msg } => {
            let value = client.ping(msg).await?;
            if let Ok(string) = str::from_utf8(&value) {
                println!("\"{}\"", string);
            } else {
                println!("{:?}", value);
            }
        }
        Command::Get { key } => {
            if let Some(value) = client.get(&key).await? {
                if let Ok(string) = str::from_utf8(&value) {
                    println!("\"{}\"", string);
                } else {
                    println!("{:?}", value);
                }
            } else {
                println!("(nil)");
            }
        }
        Command::Set {
            key,
            value,
            expires: None,
        } => {
            client.set(&key, value).await?;
            println!("OK");
        }
        Command::Set {
            key,
            value,
            expires: Some(expires),
        } => {
            client.set_expires(&key, value, expires).await?;
            println!("OK");
        }
        Command::Publish { channel, message } => {
            client.publish(&channel, message).await?;
            println!("Publish OK");
        }
        Command::Subscribe { channels } => {
            if channels.is_empty() {
                return Err("channel(s) must be provided".into());
            }
            let mut subscriber = client.subscribe(channels).await?;

            // 等待频道上的消息
            while let Some(msg) = subscriber.next_message().await? {
                println!(
                    "从频道收到消息：{}; 消息 = {:?}",
                    msg.channel, msg.content
                );
            }
        }
    }

    Ok(())
}

fn duration_from_ms_str(src: &str) -> Result<Duration, ParseIntError> {
    let ms = src.parse::<u64>()?;
    Ok(Duration::from_millis(ms))
}
