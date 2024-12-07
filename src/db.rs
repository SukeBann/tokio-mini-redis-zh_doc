use tokio::sync::{broadcast, Notify};
use tokio::time::{self, Duration, Instant};

use bytes::Bytes;
use std::collections::{BTreeSet, HashMap};
use std::sync::{Arc, Mutex};
use tracing::debug;

/// `Db` 实例的包装器。此结构存在的目的是在该结构被丢弃时，通过通知后台清理任务关闭 `Db`，从而允许有序清理。
#[derive(Debug)]
pub(crate) struct DbDropGuard {
    /// 当此 `DbDropGuard` 结构被丢弃时将被关闭的 `Db` 实例。
    db: Db,
}

/// 在所有连接间共享的服务器状态。
///
/// `Db` 包含一个用于存储键/值数据的 `HashMap` 以及所有活动发布/订阅频道的 `broadcast::Sender` 值。
/// 一个 `Db` 实例是共享状态的句柄。克隆 `Db` 是浅拷贝，只会增加一个原子引用计数。
/// 当创建 `Db` 值时，会生成一个后台任务。此任务用于在请求的持续时间过去后过期值。
/// 该任务运行直到所有 `Db` 实例被丢弃，届时任务终止。
#[derive(Debug, Clone)]
pub(crate) struct Db {
    /// 用于共享状态的句柄。后台任务也将具有一个
    /// `Arc<Shared>`.
    shared: Arc<Shared>,
}

#[derive(Debug)]
struct Shared {
    /// 共享状态由一个互斥锁保护。这是一个 `std::sync::Mutex`，而不是 Tokio 互斥锁。
    /// 因为在持有互斥锁时没有执行异步操作。此外，临界区非常小。
    /// 
    /// Tokio 互斥锁主要用于需要跨 `.await` 让步点持有的锁。所有其他情况通常最好使用 std 互斥锁。
    /// 如果临界区不包含任何异步操作但很长（CPU 密集型或执行阻塞操作），
    /// 则整个操作包括等待互斥锁都被视为“阻塞”操作，应使用 `tokio::task::spawn_blocking`。
    state: Mutex<State>,

    /// 通知处理条目过期的后台任务。后台任务等待此通知，然后检查过期的值或关闭信号。
    background_task: Notify,
}

#[derive(Debug)]
struct State {
    /// 键值数据。我们不打算做任何复杂的事情，所以 `std::collections::HashMap` 就足够了。
    entries: HashMap<String, Entry>,

    /// 发布/订阅键空间。Redis 使用一个**独立**的键空间来分别处理键值和发布/订阅。`mini-redis` 通过使用一个独立的 `HashMap` 来处理这个问题。
    pub_sub: HashMap<String, broadcast::Sender<Bytes>>,

    /// 跟踪键的 TTL（生存时间）。
    ///
    /// 使用 `BTreeSet` 来按照过期时间排序维护过期时间。这使得后台任务可以迭代此映射以找到下一个到期的值。
    ///
    /// 尽管极不可能，但有可能在同一时刻创建多个过期。因此，`Instant` 对于键来说是不够的。使用唯一键（`String`）来打破这种僵局。
    expirations: BTreeSet<(Instant, String)>,

    /// 当 Db 实例关闭时为 true。当所有 `Db` 值被丢弃时, 会发生这种情况。将其设置为 `true` 通知后台任务退出。
    shutdown: bool,
}

/// 键值存储中的条目
#[derive(Debug)]
struct Entry {
    /// 存储的数据
    data: Bytes,

    /// 条目过期并应从数据库中移除的时刻。
    expires_at: Option<Instant>,

}

impl DbDropGuard {
    /// 创建一个新的 `DbDropGuard`，包装一个 `Db` 实例。当该实例被丢弃时，`Db` 的清理任务将被关闭。
    pub(crate) fn new() -> DbDropGuard {
        DbDropGuard { db: Db::new() }
    }

    /// 获取共享数据库。在内部，这是一个 `Arc`，因此克隆只会增加引用计数。
    pub(crate) fn db(&self) -> Db {
        self.db.clone()
    }
}

impl Drop for DbDropGuard {
    fn drop(&mut self) {
        // 通知 'Db' 实例关闭清理过期键的任务
        self.db.shutdown_purge_task();
    }
}

impl Db {
    /// 创建一个新的、空的 `Db` 实例。分配共享状态并启动一个后台任务来管理key的过期。
    pub(crate) fn new() -> Db {
        let shared = Arc::new(Shared {
            state: Mutex::new(State {
                entries: HashMap::new(),
                pub_sub: HashMap::new(),
                expirations: BTreeSet::new(),
                shutdown: false,
            }),
            background_task: Notify::new(),
        });

        // Start the background task.
        tokio::spawn(purge_expired_tasks(shared.clone()));

        Db { shared }
    }

    /// 获取与key相关联的值。
    ///
    /// 如果没有与key相关联的value，则返回 `None`。
    /// 这可能是因为从未给key分配过value，或先前分配的value已过期。
    pub(crate) fn get(&self, key: &str) -> Option<Bytes> {
        // 获取锁，获取条目并克隆值。
        //
        // 因为数据是使用 `Bytes` 存储的，所以此处的克隆是浅克隆。
        // 数据不会被复制。
        let state = self.shared.state.lock().unwrap();
        state.entries.get(key).map(|entry| entry.data.clone())
    }

    /// 设置与键相关联的值，并可选择指定一个过期时长。
    ///
    /// 如果已存在与该键相关联的值，则将其移除。
    pub(crate) fn set(&self, key: String, value: Bytes, expire: Option<Duration>) {
        let mut state = self.shared.state.lock().unwrap();

        // 如果这个 `set` 成为**下一个**过期的键，则需要通知后台任务，以便它可以更新其状态。
        //
        // 是否需要通知后台任务是在执行 `set` 操作期间计算的。
        let mut notify = false;

        let expires_at = expire.map(|duration| {
            // `Instant` at which the key expires.
            let when = Instant::now() + duration;

            // 仅当新插入的过期时间是下一个要驱逐的键时，才通知工作任务。
            // 在这种情况下，需要唤醒工作任务以更新其状态。
            notify = state
                .next_expiration()
                .map(|expiration| expiration > when)
                .unwrap_or(true);

            when
        });

        // 将条目插入到 `HashMap` 中。
        let prev = state.entries.insert(
            key.clone(),
            Entry {
                data: value,
                expires_at,
            },
        );

        // 如果先前已经存在与该键关联的值**并且**有一个过期时间，
        // 则必须从 `expirations` 映射中移除关联的条目。这可以避免数据泄漏。
        if let Some(prev) = prev {
            if let Some(when) = prev.expires_at {
                // clear expiration
                state.expirations.remove(&(when, key.clone()));
            }
        }

        // 跟踪过期时间。如果在移除之前插入，当当前 `(when, key)` 等于之前的 `(when, key)` 时会导致错误。
        // 先移除再插入可以避免这种情况。
        if let Some(when) = expires_at {
            state.expirations.insert((when, key));
        }

        // 在通知后台任务之前释放互斥锁。这有助于减少争用，
        // 避免后台任务被唤醒时由于此函数仍持有互斥锁而无法获取。
        drop(state);

        if notify {
            // 最后，仅在后台任务需要更新其状态以反映新的过期时间时才通知它。
            self.shared.background_task.notify_one();
        }
    }

    /// 返回请求的频道的 `Receiver`。
    ///
    /// 返回的 `Receiver` 用于接收由 `PUBLISH` 命令广播的值。
    pub(crate) fn subscribe(&self, key: String) -> broadcast::Receiver<Bytes> {
        use std::collections::hash_map::Entry;

        // Acquire the mutex
        let mut state = self.shared.state.lock().unwrap();

        // 如果请求的频道没有条目，则创建一个新的广播频道并将其与键关联。
        // 如果已经存在，则返回一个关联的接收器。
        match state.pub_sub.entry(key) {
            Entry::Occupied(e) => e.get().subscribe(),
            Entry::Vacant(e) => {
                // 目前没有广播频道，因此创建一个。
                //
                // 创建的频道容量为 `1024` 条消息。消息会存储在频道中，直到**所有**订阅者都已查看。
                // 这意味着缓慢的订阅者可能导致消息被无限期保留。
                //
                // 当频道容量达到上限时，发布操作会导致旧消息被丢弃。
                // 这可以防止缓慢的消费者阻塞整个系统。
                let (tx, rx) = broadcast::channel(1024);
                e.insert(tx);
                rx
            }
        }
    }

    /// 将消息发布到频道。返回正在监听该频道的订阅者数量。
    pub(crate) fn publish(&self, key: &str, value: Bytes) -> usize {
        let state = self.shared.state.lock().unwrap();

        state
            .pub_sub
            .get(key)
            // 在广播频道成功发送消息时，返回订阅者的数量。
            // 如果发生错误，表示没有接收器，此时应返回 `0`。
            .map(|tx| tx.send(value).unwrap_or(0))
            // 如果频道键没有条目，则表示没有订阅者。在这种情况下，返回 `0`。
            .unwrap_or(0)
    }

    /// 发出信号以关闭清理后台任务。这是由 `DbShutdown` 的 `Drop` 实现调用的。
    fn shutdown_purge_task(&self) {
        // 必须发出信号以关闭后台任务。这是通过将 `State::shutdown` 设为 `true` 并发出信号给任务来完成的。
        let mut state = self.shared.state.lock().unwrap();
        state.shutdown = true;

        // 在通知后台任务之前释放锁。这有助于减少锁争用，确保后台任务唤醒时不会因无法获取互斥锁而阻塞。
        drop(state);
        self.shared.background_task.notify_one();
    }
}

impl Shared {
    /// 清除所有已过期的键，并返回下一个键将过期的 `Instant`。后台任务将休眠至该时刻。
    fn purge_expired_keys(&self) -> Option<Instant> {
        let mut state = self.state.lock().unwrap();

        if state.shutdown {
            // 数据库正在关闭。所有共享状态的句柄已被释放。后台任务应退出。
            return None;
        }

        // 这是为了让借用检查器满意。简而言之，`lock()` 返回一个 `MutexGuard`，而不是 `&mut State`。
        // 借用检查器无法“透过”互斥锁守护体判断同时可变访问 `state.expirations` 和 `state.entries` 是安全的，
        // 因此我们在循环外部获取 `State` 的“真正”可变引用。
        let state = &mut *state;

        // 查找所有计划在当前时间之前过期的键。
        let now = Instant::now();

        while let Some(&(when, ref key)) = state.expirations.iter().next() {
            if when > now {
                // 清除完成，`when` 是下一个键过期的时刻。工作线程将等待至此时刻。
                return Some(when);
            }

            // 键已过期，移除它
            state.entries.remove(key);
            state.expirations.remove(&(when, key.clone()));
        }

        None
    }

    /// 如果数据库正在关闭则返回 `true`
    ///
    /// 当所有 `Db` 值都已被释放时，`shutdown` 标志被设置，这表明共享状态不再可访问。
    fn is_shutdown(&self) -> bool {
        self.state.lock().unwrap().shutdown
    }
}

impl State {
    fn next_expiration(&self) -> Option<Instant> {
        self.expirations
            .iter()
            .next()
            .map(|expiration| expiration.0)
    }
}

/// 后台任务执行的例程。
///
/// 等待通知。在收到通知时，从共享状态句柄中清除任何已过期的键。如果设置了 `shutdown`，则终止任务。
async fn purge_expired_tasks(shared: Arc<Shared>) {
    // 如果关闭标志被设置，则任务应退出。
    while !shared.is_shutdown() {
        // 清除所有已过期的键。该函数返回下一个键过期的时刻。
        // 工作线程应该等待直到该时刻过去，然后再次清除。
        if let Some(when) = shared.purge_expired_keys() {
            // 等待直到下一个键过期或直到收到后台任务的通知。
            // 如果任务收到通知，则必须重新加载其状态，因为新的键被设置为提前过期。这是通过循环完成的。
            tokio::select! {
                _ = time::sleep_until(when) => {}
                _ = shared.background_task.notified() => {}
            }
        } else {
            // 没有键在未来过期。等待任务通知。
            shared.background_task.notified().await;
        }
    }

    debug!("Purge background task shut down")
}