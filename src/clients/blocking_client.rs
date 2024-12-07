//! 最小阻塞 Redis 客户端实现
//!
//! 提供阻塞的连接和执行支持命令的方法。

use bytes::Bytes;
use std::time::Duration;
use tokio::net::ToSocketAddrs;
use tokio::runtime::Runtime;

pub use crate::clients::Message;

/// 与 Redis 服务器建立的连接。
///
/// 基于单个 `TcpStream`，`BlockingClient` 提供基本的网络客户端功能
/// （没有连接池、重试等）。使用 [`connect`](fn@connect) 函数建立连接。
///
/// 使用 `Client` 的各种方法发出请求。
pub struct BlockingClient {
    /// 异步 `Client`。
    inner: crate::clients::Client,

    /// 一个 `current_thread` 运行时，用于以阻塞方式在异步客户端上执行操作。
    rt: Runtime,
}

/// 进入发布/订阅模式的客户端。
///
/// 一旦客户端订阅了一个频道，它们可能只能执行与发布/订阅相关的命令。
/// `BlockingClient` 类型转换为 `BlockingSubscriber` 类型，
/// 以防止调用非发布/订阅的方法。
pub struct BlockingSubscriber {
    /// 异步 `Subscriber`。
    inner: crate::clients::Subscriber,

    /// 一个 `current_thread` 运行时，用于以阻塞方式在异步 `Subscriber` 上执行操作。
    rt: Runtime,
}

/// `Subscriber::into_iter` 返回的迭代器。
struct SubscriberIterator {
    /// 异步 `Subscriber`。
    inner: crate::clients::Subscriber,

    /// 一个 `current_thread` 运行时，用于以阻塞方式在异步 `Subscriber` 上执行操作。
    rt: Runtime,
}

impl BlockingClient {
    /// 与位于 `addr` 的 Redis 服务器建立连接。
    ///
    /// `addr` 可以是任何可以异步转换为 `SocketAddr` 的类型。这包括 `SocketAddr` 和字符串。
    /// `ToSocketAddrs` 是 Tokio 版本，而不是 `std` 版本。
    ///
    /// # 示例
    ///
    /// ```no_run
    /// use mini_redis::clients::BlockingClient;
    ///
    /// fn main() {
    ///     let client = match BlockingClient::connect("localhost:6379") {
    ///         Ok(client) => client,
    ///         Err(_) => panic!("failed to establish connection"),
    ///     };
    /// # drop(client);
    /// }
    /// ```
    pub fn connect<T: ToSocketAddrs>(addr: T) -> crate::Result<BlockingClient> {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;

        let inner = rt.block_on(crate::clients::Client::connect(addr))?;

        Ok(BlockingClient { inner, rt })
    }

    /// 获取键的值。
    ///
    /// 如果键不存在，则返回特殊值 `None`。
    ///
    /// # 示例
    ///
    /// 展示基本的用法。
    ///
    /// ```no_run
    /// use mini_redis::clients::BlockingClient;
    ///
    /// fn main() {
    ///     let mut client = BlockingClient::connect("localhost:6379").unwrap();
    ///
    ///     let val = client.get("foo").unwrap();
    ///     println!("Got = {:?}", val);
    /// }
    /// ```
    pub fn get(&mut self, key: &str) -> crate::Result<Option<Bytes>> {
        self.rt.block_on(self.inner.get(key))
    }
    /// 将给定的 `value` 设置为与 `key` 关联。
    ///
    /// `value` 与 `key` 关联，直到下一次调用 `set` 或它被删除时覆盖。
    ///
    /// 如果键已经有一个值，它将被覆盖。任何与该键相关联的先前的存活时间
    /// 在成功的 SET 操作时将被丢弃。
    ///
    /// # 示例
    ///
    /// 展示基本的用法。
    ///
    /// ```no_run
    /// use mini_redis::clients::BlockingClient;
    ///
    /// fn main() {
    ///     let mut client = BlockingClient::connect("localhost:6379").unwrap();
    ///
    ///     client.set("foo", "bar".into()).unwrap();
    ///
    ///     // 立即获取值是可行的
    ///     let val = client.get("foo").unwrap().unwrap();
    ///     assert_eq!(val, "bar");
    /// }
    /// ```
    pub fn set(&mut self, key: &str, value: Bytes) -> crate::Result<()> {
        self.rt.block_on(self.inner.set(key, value))
    }

    /// 将给定的 `value` 设置为与 `key` 关联，该值将在 `expiration` 后过期。
    ///
    /// `value` 与 `key` 关联，直到以下之一发生：
    /// - 它过期。
    /// - 它被下一次调用 `set` 覆盖。
    /// - 它被删除。
    ///
    /// 如果键已经有一个值，它将被覆盖。任何与该键相关联的先前的存活时间
    /// 在成功的 SET 操作时将被丢弃。
    ///
    /// # 示例
    ///
    /// 展示基本用法。这个例子不**保证**总是有效，因为它依赖于基于时间的逻辑，
    /// 并假设客户端和服务器在时间上保持相对同步。现实世界往往不那么有利。
    ///
    /// ```no_run
    /// use mini_redis::clients::BlockingClient;
    /// use std::thread;
    /// use std::time::Duration;
    ///
    /// fn main() {
    ///     let ttl = Duration::from_millis(500);
    ///     let mut client = BlockingClient::connect("localhost:6379").unwrap();
    ///
    ///     client.set_expires("foo", "bar".into(), ttl).unwrap();
    ///
    ///     // 立即获取值是可行的
    ///     let val = client.get("foo").unwrap().unwrap();
    ///     assert_eq!(val, "bar");
    ///
    ///     // 等待 TTL 到期
    ///     thread::sleep(ttl);
    ///
    ///     let val = client.get("foo").unwrap();
    ///     assert!(val.is_some());
    /// }
    /// ```
    pub fn set_expires(
        &mut self,
        key: &str,
        value: Bytes,
        expiration: Duration,
    ) -> crate::Result<()> {
        self.rt
            .block_on(self.inner.set_expires(key, value, expiration))
    }

    /// 发布 `message` 到指定的 `channel`。
    ///
    /// 返回当前在频道上监听的订阅者数量。不能保证这些订阅者会接收到消息，因为他们可能随时断开连接。
    ///
    /// # 示例
    ///
    /// 展示基本用法。
    ///
    /// ```no_run
    /// use mini_redis::clients::BlockingClient;
    ///
    /// fn main() {
    ///     let mut client = BlockingClient::connect("localhost:6379").unwrap();
    ///
    ///     let val = client.publish("foo", "bar".into()).unwrap();
    ///     println!("Got = {:?}", val);
    /// }
    /// ```
    pub fn publish(&mut self, channel: &str, message: Bytes) -> crate::Result<u64> {
        self.rt.block_on(self.inner.publish(channel, message))
    }

    /// 订阅客户端到指定的频道。
    ///
    /// 一旦客户端发出订阅命令，它不能再发出任何非发布/订阅命令。该函数消耗 `self` 并返回一个
    /// `BlockingSubscriber`。
    ///
    /// `BlockingSubscriber` 值用于接收消息，以及管理客户端所订阅的频道列表。
    pub fn subscribe(self, channels: Vec<String>) -> crate::Result<BlockingSubscriber> {
        let subscriber = self.rt.block_on(self.inner.subscribe(channels))?;
        Ok(BlockingSubscriber {
            inner: subscriber,
            rt: self.rt,
        })
    }
}

impl BlockingSubscriber {
    /// 返回当前订阅的频道集合。
    pub fn get_subscribed(&self) -> &[String] {
        self.inner.get_subscribed()
    }

    /// 接收订阅的频道上发布的下一条消息，如有必要，等待。
    ///
    /// `None` 表示订阅已终止。
    pub fn next_message(&mut self) -> crate::Result<Option<Message>> {
        self.rt.block_on(self.inner.next_message())
    }

    /// 将订阅者转换为一个 `Iterator`，提供在已订阅频道上发布的新消息。
    pub fn into_iter(self) -> impl Iterator<Item = crate::Result<Message>> {
        SubscriberIterator {
            inner: self.inner,
            rt: self.rt,
        }
    }

    /// 订阅一个新的频道列表
    pub fn subscribe(&mut self, channels: &[String]) -> crate::Result<()> {
        self.rt.block_on(self.inner.subscribe(channels))
    }

    /// 取消订阅一个新的频道列表
    pub fn unsubscribe(&mut self, channels: &[String]) -> crate::Result<()> {
        self.rt.block_on(self.inner.unsubscribe(channels))
    }
}

impl Iterator for SubscriberIterator {
    type Item = crate::Result<Message>;

    fn next(&mut self) -> Option<crate::Result<Message>> {
        self.rt.block_on(self.inner.next_message()).transpose()
    }
}
