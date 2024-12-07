//! 一个简单（即非常不完整）的 Redis 服务器和客户端实现。
//!
//! 本项目的目的是提供一个使用 Tokio 构建的异步 Rust 项目的较大示例。不要试图在生产环境中运行这个……真的。
//!
//! # 布局
//!
//! 这个库的结构设计是为了可以与指南一起使用。这里有一些模块是公共的，可能在“真实”的 Redis 客户端库中不会是公共的。
//!
//! 主要组件包括：
//!
//! * `server`：Redis 服务器实现。包含一个单一的 `run` 函数，该函数接受一个 `TcpListener` 并开始接受 Redis 客户端连接。
//!
//! * `clients/client`：一个异步的 Redis 客户端实现。演示如何使用 Tokio 构建客户端。
//!
//! * `cmd`：对支持的 Redis 命令的实现。
//!
//! * `frame`：表示一个 Redis 协议帧。帧作为“命令”和字节表示之间的中间表示。

pub mod clients;
pub use clients::{BlockingClient, BufferedClient, Client};

pub mod cmd;
pub use cmd::Command;

mod connection;
pub use connection::Connection;

pub mod frame;
pub use frame::Frame;

mod db;
use db::Db;
use db::DbDropGuard;

mod parse;
use parse::{Parse, ParseError};

pub mod server;

mod shutdown;
use shutdown::Shutdown;

/// Redis 服务器监听的默认端口。
///
/// 如果没有指定端口，则使用此端口。
pub const DEFAULT_PORT: u16 = 6379;

/// 大多数函数返回的错误。
///
/// 在编写真实应用程序时，可以考虑使用专门的错误处理库或将错误类型定义为原因的 `enum`。
/// 然而，对于我们的示例，使用一个装箱的 `std::error::Error` 就足够了。
///
/// 出于性能原因，在任何热点路径中都避免装箱。例如，在 `parse` 函数中，定义了一个自定义错误 `enum`。
/// 这是因为在正常执行期间，当在套接字上收到部分帧时，会遇到并处理此错误。
/// 实现了 `std::error::Error` 用于 `parse::Error`，这允许其转换为 `Box<dyn std::error::Error>`。
pub type Error = Box<dyn std::error::Error + Send + Sync>;

/// 一个用于 mini-redis 操作的专用 `Result` 类型。
///
/// 这是定义为一个便利类型……enience.
pub type Result<T> = std::result::Result<T, Error>;
