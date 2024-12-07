use tokio::sync::broadcast;

/// 监听服务器关闭信号。
///
/// 使用 `broadcast::Receiver` 来发出关闭信号。该信号仅发送一个值。一旦通过广播通道发送了一个值，服务器应该关闭。
///
/// `Shutdown` 结构体监听信号并跟踪信号是否已接收。调用方可以查询关闭信号是否已接收到。
#[derive(Debug)]
pub(crate) struct Shutdown {
    /// 如果关闭信号已接收到，则为 `true`
    is_shutdown: bool,

    /// 用于监听关闭的通道接收部分。
    notify: broadcast::Receiver<()>,
}

impl Shutdown {
    /// 使用给定的 `broadcast::Receiver` 创建一个新的 `Shutdown`。
    pub(crate) fn new(notify: broadcast::Receiver<()>) -> Shutdown {
        Shutdown {
            is_shutdown: false,
            notify,
        }
    }

    /// 如果关闭信号已经被接收，则返回 `true`。
    pub(crate) fn is_shutdown(&self) -> bool {
        self.is_shutdown
    }

    /// 接收关闭通知，必要时会等待。
    pub(crate) async fn recv(&mut self) {
        // 如果关闭信号已接收，则立即返回。
        if self.is_shutdown {
            return;
        }

        // 不会收到“滞后错误”，因为只发送了一个值。
        let _ = self.notify.recv().await;

        // 记住信号已被接收。
        self.is_shutdown = true;
    }
}