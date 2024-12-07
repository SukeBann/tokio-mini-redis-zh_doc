//! 最小化 Redis 服务器实现
//!
//! 提供一个异步 `run` 函数，监听传入的连接，
//! 每个连接生成一个任务。

use crate::{Command, Connection, Db, DbDropGuard, Shutdown};

use std::future::Future;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{broadcast, mpsc, Semaphore};
use tokio::time::{self, Duration};
use tracing::{debug, error, info, instrument};

/// 服务器侦听器状态。在 `run` 调用中创建。它包括一个执行 TCP 监听和初始化每个连接状态的 `run` 方法。
#[derive(Debug)]
struct Listener {
    /// 共享的数据库句柄。
    ///
    /// 包含键/值存储以及用于 pub/sub 的广播通道。
    ///
    /// 这里包含了一个 `Arc` 的包装器。内部的 `Db` 可以被检索并传入每个连接状态 (`Handler`)。
    db_holder: DbDropGuard,

    /// 由 `run` 调用者提供的 TCP 侦听器。
    listener: TcpListener,

    /// 限制最大连接数量。
    ///
    /// 使用 `Semaphore` 来限制最大连接数量。在尝试接受新连接之前，
    ///
    /// 当处理程序完成处理一个连接时，许可证会被返回到信号量。
    limit_connections: Arc<Semaphore>,

    /// 向所有活动连接广播关闭信号。
    ///
    /// 初始的 `shutdown` 触发器由 `run` 调用者提供。服务器负责优雅地关闭活动连接。
    /// 当一个连接任务被生成时，它会传递一个广播接收器句柄。
    /// 当启动优雅关闭时，会通过 broadcast::Sender 发送一个 `()` 值。
    /// 每个活动连接接收到信号后，达到一个安全的终端状态，并完成任务。
    notify_shutdown: broadcast::Sender<()>,

    /// 用作优雅关闭过程的一部分，等待客户端连接完成处理。
    ///
    /// 一旦所有 `Sender` 句柄超出范围，Tokio 通道就会关闭。
    /// 当通道关闭时，接收器会接收到 `None`。这用于检测所有连接处理程序是否完成。
    /// 当一个连接处理程序被初始化时，它被分配一个 `shutdown_complete_tx` 的克隆。
    /// 当侦听器关闭时，它会丢弃由此 `shutdown_complete_tx` 字段持有的发送器。
    /// 一旦所有处理程序任务完成，所有 `Sender` 的克隆也会被丢弃。
    /// 这导致 `shutdown_complete_rx.recv()` 以 `None` 完成。
    /// 此时，可以安全地退出服务器进程。
    shutdown_complete_tx: mpsc::Sender<()>,
}

/// 每个连接的处理程序。从 `connection` 读取请求并将指令应用于 `db`。
#[derive(Debug)]
struct Handler {
    /// 共享的数据库句柄。
    ///
    /// 当从 `connection` 接收到命令时，它将与 `db` 一起应用。
    /// 与 `db` 交互以完成工作。
    db: Db,

    /// 使用带缓冲的 `TcpStream` 实现的 redis 协议编码器/解码器装饰的 TCP 连接。
    /// 在"帧"级别上操作，并将字节级协议解析细节封装在 `Connection` 中。
    /// the byte level protocol parsing details encapsulated in `Connection`.
    connection: Connection,

    /// 监听关闭通知。
    ///
    /// 这是对于 `Listener` 中的发送器配对的 `broadcast::Receiver` 的包装。
    /// 连接处理程序处理来自连接的请求，直到对等方断开连接 **或** 从 `shutdown` 收到关闭通知。
    /// 在后一种情况下，任何正在进行的工作都会继续，直到到达一个安全状态，
    /// 此时连接才会终止。
    shutdown: Shutdown,

    /// 不直接使用。相反，当 `Handler` 被丢弃时...？
    _shutdown_complete: mpsc::Sender<()>,
}

/// Redis 服务器可接受的最大并发连接数。
///
/// 当达到此限制时，服务器将停止接受连接，直到有活动连接终止。
/// 是一样的)。
/// well).
const MAX_CONNECTIONS: usize = 250;

/// 运行 mini-redis 服务器。
///
/// 接受来自提供的侦听器的连接。对于每个传入的连接，
/// 生成一个任务来处理该连接。服务器一直运行到
/// `shutdown` future 完成，此时服务器将优雅地关闭。
///
/// 可以将 `tokio::signal::ctrl_c()` 用作 `shutdown` 参数。
/// 这将监听 SIGINT 信号。
pub async fn run(listener: TcpListener, shutdown: impl Future) {
    // 当提供的 `shutdown` future 完成时，我们必须向所有活动连接发送关闭消息。
    // 我们使用广播通道来实现这一目的。下面的调用忽略了广播对的接收器，当需要接收器时，
    // 使用发送器上的 subscribe() 方法来创建一个。
    let (notify_shutdown, _) = broadcast::channel(1);
    let (shutdown_complete_tx, mut shutdown_complete_rx) = mpsc::channel(1);

    // 初始化监听器状态
    let mut server = Listener {
        listener,
        db_holder: DbDropGuard::new(),
        limit_connections: Arc::new(Semaphore::new(MAX_CONNECTIONS)),
        notify_shutdown,
        shutdown_complete_tx,
    };

    // 并发运行服务器并监听 `shutdown` 信号。
    // 服务器任务运行到遇到错误为止，因此在正常情况下，
    // 此 `select!` 语句运行到接收到 `shutdown` 信号。
    //
    // `select!` 语句的编写格式为：
    //
    // ```
    // <异步操作的结果> = <异步操作> => <使用结果进行的步骤>
    // ```
    //
    // 所有 `<异步操作>` 语句异步执行。
    // 一旦 **第一个** 操作完成，它的关联 `<使用结果进行的步骤>` 会被执行。
    //
    // `select!` 宏是编写异步 Rust 的基础构建块。查看 API 文档了解更多详情：
    //
    // https://docs.rs/tokio/*/tokio/macro.select.html
    tokio::select! {
        res = server.run() => {
            // 如果在这里收到错误，则表示接受 TCP 侦听器的连接失败多次，服务器放弃并关闭。
            //
            // 在处理单个连接时遇到的错误不会浮出到此点。
            if let Err(err) = res {
                error!(cause = %err, "failed to accept");
            }
        }
        _ = shutdown => {
            // 已收到关闭信号。
            info!("shutting down");
        }
    }

    // 提取 `shutdown_complete` 接收器和传输器
    // 显式删除 `shutdown_transmitter`。这很重要，否则下面的 `.await` 将永远不会完成。
    let Listener {
        shutdown_complete_tx,
        notify_shutdown,
        ..
    } = server;

    // 当 `notify_shutdown` 被丢弃时，所有调用过 `subscribe` 的任务将会接收到关闭信号并退出
    drop(notify_shutdown);
    // Drop最后的 `Sender` 以便下面的 `Receiver` 可以完成
    drop(shutdown_complete_tx);

    // 等待所有活动连接完成处理。
    // 由于监听器持有的 `Sender` 已经在上面被删除，
    // 唯一剩下的 `Sender` 实例在连接处理器任务中持有。
    // 当这些任务完成时，`mpsc` 通道将关闭，`recv()` 将返回 `None`。
    let _ = shutdown_complete_rx.recv().await;
}

impl Listener {
    /// 运行服务器
    ///
    /// 监听传入的连接。对于每个传入的连接，生成一个任务来处理该连接。
    ///
    /// # 错误
    ///
    /// 如果接受连接时返回错误，则返回 `Err`。这可能由于各种原因而发生，这些原因可以随着时间的推移解决。
    /// 例如，如果底层操作系统达到了最大套接字数量的内部限制，accept 调用将会失败。
    ///
    /// 无法检测到短暂错误何时自行解决。一种处理这种情况的策略是实现退避策略，我们在这里实现这种策略。
    async fn run(&mut self) -> crate::Result<()> {
        info!("accepting inbound connections");

        loop {
            // 等待许可证可用
            //
            // `acquire_owned` 返回一个与信号量绑定的许可证。
            // 当许可证值被丢弃时，它会自动返回到信号量。
            //
            // 当信号量已关闭时，`acquire_owned()` 会返回 `Err`。
            // 我们从不关闭信号量，所以 `unwrap()` 是安全的。
            let permit = self
                .limit_connections
                .clone()
                .acquire_owned()
                .await
                .unwrap();

            // 接受一个新的套接字。这将尝试执行错误处理。
            // `accept` 方法在内部尝试恢复错误，因此此处的错误是不可恢复的。
            let socket = self.accept().await?;

            // 创建每个连接所需的处理状态。
            let mut handler = Handler {
                // 获取一个共享数据库的句柄。
                db: self.db_holder.db(),

                // 初始化连接状态。这将分配读/写缓冲区以执行 redis 协议帧解析。
                connection: Connection::new(socket),

                // 接收关闭通知。
                shutdown: Shutdown::new(self.notify_shutdown.subscribe()),

                // 一旦所有克隆被丢弃后通知接收方。
                _shutdown_complete: self.shutdown_complete_tx.clone(),
            };

            // 生成一个新任务来处理连接。Tokio 任务类似于异步绿线程，并发执行。
            tokio::spawn(async move {
                // 处理连接。如果遇到错误，记录错误。
                if let Err(err) = handler.run().await {
                    error!(cause = ?err, "connection error");
                }
                // 将许可证移入任务，并在完成后将其丢弃。这会将许可证返回到信号量。
                drop(permit);
            });
        }
    }
    /// 接受一个传入的连接。
    ///
    /// 通过退避重试来处理错误。使用指数退避策略。在第一次失败后，任务等待1秒。
    /// 第二次失败后，任务等待2秒。每次后续失败都会使等待时间加倍。
    /// 如果在等待64秒后第6次尝试接受失败，则此函数将返回一个错误。
    async fn accept(&mut self) -> crate::Result<TcpStream> {
        let mut backoff = 1;

        // 尝试接受几次
        loop {
            // 执行接受操作。如果成功接受了一个套接字，则返回它。否则，保存错误。
            match self.listener.accept().await {
                Ok((socket, _)) => return Ok(socket),
                Err(err) => {
                    if backoff > 64 {
                        // 接受操作失败太多次。返回错误。
                        return Err(err.into());
                    }
                }
            }

            // 暂停执行直到退避期结束。
            time::sleep(Duration::from_secs(backoff)).await;

            // 将退避时间加倍
            backoff *= 2;
        }
    }
}
impl Handler {
    /// 处理单个连接。
    ///
    /// 请求帧从套接字读取并处理。响应将写回到套接字。
    ///
    /// 目前，流水线尚未实现。流水线是指能够在每个连接上并发处理多个请求而不交错帧。
    /// 更多详情参见：https://redis.io/topics/pipelining
    ///
    /// 当接收到关闭信号时，连接会处理到达安全状态，之后进行终止。
    #[instrument(skip(self))]
    async fn run(&mut self) -> crate::Result<()> {
        // 只要没有收到关闭信号，就尝试读取一个新的请求帧。
        while !self.shutdown.is_shutdown() {
            // 在读取请求帧的同时也监听关闭信号。
            let maybe_frame = tokio::select! {
                res = self.connection.read_frame() => res?,
                _ = self.shutdown.recv() => {
                    // 如果收到关闭信号，从 `run` 返回。
                    // 这将导致任务终止。
                    return Ok(());
                }
            };

            // 如果 `read_frame()` 返回 `None`，则表示对等方关闭了套接字。
            // 没有进一步的工作要做，任务可以被终止。
            let frame = match maybe_frame {
                Some(frame) => frame,
                None => return Ok(()),
            };

            // 将 redis 帧转换为命令结构体。如果帧不是有效的 redis 命令或不支持的命令，则返回错误。
            let cmd = Command::from_frame(frame)?;

            // 记录 `cmd` 对象。这里的语法是由 `tracing` crate 提供的简写。
            // 它可以被认为类似于：
            //
            // ```
            // debug!(cmd = format!("{:?}", cmd));
            // ```
            //
            // `tracing` 提供结构化日志记录，因此信息以键值对的形式“记录”。
            debug!(?cmd);

            // 执行应用命令所需的工作。这可能会导致数据库状态的变化。
            //
            // 连接被传递到 apply 函数中，这允许命令直接将响应帧写入连接。
            // 在发布/订阅的情况下，可能会有多个帧发送回对等方。
            cmd.apply(&self.db, &mut self.connection, &mut self.shutdown)
                .await?;
        }

        Ok(())
    }
}
