//! 简单的 Redis 客户端实现
//!
//! 提供异步连接和发出支持的命令的方法。

use crate::cmd::{Get, Ping, Publish, Set, Subscribe, Unsubscribe};
use crate::{Connection, Frame};

use async_stream::try_stream;
use bytes::Bytes;
use std::io::{Error, ErrorKind};
use std::time::Duration;
use tokio::net::{TcpStream, ToSocketAddrs};
use tokio_stream::Stream;
use tracing::{debug, instrument};

/// 与 Redis 服务器建立的连接。
///
/// 基于单个 `TcpStream`，`Client` 提供基本的网络客户端功能（不包含池化、重试等）。可以使用 [`connect`](fn@connect) 函数建立连接。
///
/// 可以通过 `Client` 的各种方法发出请求。
pub struct Client {
    /// 增强了 Redis 协议编码器/解码器并使用缓冲的 `TcpStream` 实现的 TCP 连接。
    ///
    /// 当 `Listener` 接收到一个入站连接时，`TcpStream` 被传递给 `Connection::new`，它会初始化相关联的缓冲区。
    /// `Connection` 允许处理器在“帧”级别运行，并在 `Connection` 中将字节级别的协议解析细节封装起来。
    connection: Connection,
}

/// 处于发布/订阅模式的客户端。
///
/// 一旦客户端订阅了一个频道，它们就只能执行与发布/订阅相关的命令。
/// `Client` 类型会转换为 `Subscriber` 类型，以防止调用非发布/订阅的方法。
pub struct Subscriber {
    /// 已订阅的客户端。
    client: Client,

    /// `Subscriber` 当前订阅的频道集合。
    subscribed_channels: Vec<String>,
}

/// 在已订阅的频道上接收到的消息。
#[derive(Debug, Clone)]
pub struct Message {
    pub channel: String,
    pub content: Bytes,
}

impl Client {
    /// 与位于 `addr` 的 Redis 服务器建立连接。
    ///
    /// `addr` 可以是任何类型，只要它能够异步转换为 `SocketAddr`。这包括 `SocketAddr` 和字符串。
    /// `ToSocketAddrs` 特性是 Tokio 版本，而不是 `std` 版本。
    ///
    /// # 示例
    ///
    /// ```no_run
    /// use mini_redis::clients::Client;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let client = match Client::connect("localhost:6379").await {
    ///         Ok(client) => client,
    ///         Err(_) => panic!("无法建立连接"),
    ///     };
    /// # drop(client);
    /// }
    /// ```
    ///
    pub async fn connect<T: ToSocketAddrs>(addr: T) -> crate::Result<Client> {
        // `addr` 参数直接传递给 `TcpStream::connect`。这会执行任何异步 DNS 查找
        // 并尝试建立 TCP 连接。在任一步发生错误都会返回错误，
        // 该错误会被传递给 `mini_redis` connect 的调用者。
        let socket = TcpStream::connect(addr).await?;

        // 初始化连接状态。这会分配读/写缓冲区以执行 Redis 协议帧解析。
        let connection = Connection::new(socket);

        Ok(Client { connection })
    }

    /// 向服务器发送 Ping。
    ///
    /// 如果没有提供参数，则返回 PONG，否则返回参数的副本作为批量回复。
    ///
    /// 此命令通常用于测试连接是否仍然活跃，或者衡量延迟。
    ///
    /// # 示例
    ///
    /// 演示基本用法。
    /// ```no_run
    /// use mini_redis::clients::Client;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let mut client = Client::connect("localhost:6379").await.unwrap();
    ///
    ///     let pong = client.ping(None).await.unwrap();
    ///     assert_eq!(b"PONG", &pong[..]);
    /// }
    /// ```
    #[instrument(skip(self))]
    pub async fn ping(&mut self, msg: Option<Bytes>) -> crate::Result<Bytes> {
        let frame = Ping::new(msg).into_frame();
        debug!(request = ?frame);
        self.connection.write_frame(&frame).await?;

        match self.read_response().await? {
            Frame::Simple(value) => Ok(value.into()),
            Frame::Bulk(value) => Ok(value),
            frame => Err(frame.to_error()),
        }
    }

    /// 获取键的值。
    ///
    /// 如果键不存在，则返回特殊值 `None`。
    ///
    /// # 示例
    ///
    /// 演示基本用法。
    ///
    /// ```no_run
    /// use mini_redis::clients::Client;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let mut client = Client::connect("localhost:6379").await.unwrap();
    ///
    ///     let val = client.get("foo").await.unwrap();
    ///     println!("Got = {:?}", val);
    /// }
    /// ```
    #[instrument(skip(self))]
    pub async fn get(&mut self, key: &str) -> crate::Result<Option<Bytes>> {
        // 为 `key` 创建一个 `Get` 命令并将其转换为帧。
        let frame = Get::new(key).into_frame();

        debug!(request = ?frame);

        // 将帧写入套接字。这会将完整帧写入套接字，必要时会等待。
        self.connection.write_frame(&frame).await?;

        // 等待服务器的响应
        //
        // 接受 `Simple` 和 `Bulk` 帧。`Null` 表示键不存在，返回 `None`。
        match self.read_response().await? {
            Frame::Simple(value) => Ok(Some(value.into())),
            Frame::Bulk(value) => Ok(Some(value)),
            Frame::Null => Ok(None),
            frame => Err(frame.to_error()),
        }
    }

    /// 将 `key` 设为持有指定的 `value`。
    ///
    /// 该 `value` 与 `key` 关联，直到它被下一次对 `set` 的调用覆盖或被移除。
    ///
    /// 如果键已经持有一个值，它将被覆盖。任何与键关联的先前生存时间在成功的 SET 操作后将被丢弃。
    ///
    /// # 示例
    ///
    /// 演示基本用法。
    ///
    /// ```no_run
    /// use mini_redis::clients::Client;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let mut client = Client::connect("localhost:6379").await.unwrap();
    ///
    ///     client.set("foo", "bar".into()).await.unwrap();
    ///
    ///     // 立即获取值可以成功
    ///     let val = client.get("foo").await.unwrap().unwrap();
    ///     assert_eq!(val, "bar");
    /// }
    /// ```
    #[instrument(skip(self))]
    pub async fn set(&mut self, key: &str, value: Bytes) -> crate::Result<()> {
        // 创建一个 `Set` 命令并将其传递给 `set_cmd`。一个单独的方法用于设置带有过期时间的值。
        // 两个函数的共同部分由 `set_cmd` 实现。
        self.set_cmd(Set::new(key, value, None)).await
    }

    /// 将 `key` 设为持有指定的 `value`。该值在 `expiration` 后过期。
    ///
    /// 该 `value` 与 `key` 关联，直到以下之一发生：
    /// - 它过期。
    /// - 它被下一次对 `set` 的调用覆盖。
    /// - 它被移除。
    ///
    /// 如果键已经持有一个值，它将被覆盖。任何与键关联的先前生存时间在成功的 SET 操作后将被丢弃。
    ///
    /// # 示例
    ///
    /// 演示基本用法。此示例不 **保证** 始终有效，因为它依赖于基于时间的逻辑，并且假定客户端和服务器在时间上保持相对同步。现实世界往往不那么理想。
    ///
    /// ```no_run
    /// use mini_redis::clients::Client;
    /// use tokio::time;
    /// use std::time::Duration;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let ttl = Duration::from_millis(500);
    ///     let mut client = Client::connect("localhost:6379").await.unwrap();
    ///
    ///     client.set_expires("foo", "bar".into(), ttl).await.unwrap();
    ///
    ///     // 立即获取值可以成功
    ///     let val = client.get("foo").await.unwrap().unwrap();
    ///     assert_eq!(val, "bar");
    ///
    ///     // 等待TTL过期
    ///     time::sleep(ttl).await;
    ///
    ///     let val = client.get("foo").await.unwrap();
    ///     assert!(val.is_some());
    /// }
    /// ```
    #[instrument(skip(self))]
    pub async fn set_expires(
        &mut self,
        key: &str,
        value: Bytes,
        expiration: Duration,
    ) -> crate::Result<()> {
        // 创建一个 `Set` 命令并将其传递给 `set_cmd`。一个单独的方法用于设置带有过期时间的值。
        // 两个函数的共同部分由 `set_cmd` 实现。
        self.set_cmd(Set::new(key, value, Some(expiration))).await
    }

    /// 核心的 `SET` 逻辑，既被 `set` 使用，也被 `set_expires` 使用。
    async fn set_cmd(&mut self, cmd: Set) -> crate::Result<()> {
        // 将 `Set` 命令转换为帧
        let frame = cmd.into_frame();

        debug!(request = ?frame);

        // 将帧写入套接字。这会将完整帧写入套接字，必要时会等待。
        self.connection.write_frame(&frame).await?;

        // 等待服务器的响应。成功时，服务器仅以 `OK` 响应。任何其他响应表示错误。
        match self.read_response().await? {
            Frame::Simple(response) if response == "OK" => Ok(()),
            frame => Err(frame.to_error()),
        }
    }

    /// 将 `message` 发送到给定的 `channel`。
    ///
    /// 返回当前监听频道的订阅者数量。无法保证这些订阅者会收到消息，因为他们可能随时断开连接。
    ///
    /// # 示例
    ///
    /// 演示基本用法。
    ///
    /// ```no_run
    /// use mini_redis::clients::Client;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let mut client = Client::connect("localhost:6379").await.unwrap();
    ///
    ///     let val = client.publish("foo", "bar".into()).await.unwrap();
    ///     println!("Got = {:?}", val);
    /// }
    /// ```
    #[instrument(skip(self))]
    pub async fn publish(&mut self, channel: &str, message: Bytes) -> crate::Result<u64> {
        // 将 `Publish` 命令转换为帧
        let frame = Publish::new(channel, message).into_frame();

        debug!(request = ?frame);

        // 将帧写入套接字
        self.connection.write_frame(&frame).await?;

        // 读取响应
        match self.read_response().await? {
            Frame::Integer(response) => Ok(response),
            frame => Err(frame.to_error()),
        }
    }

    /// 订阅客户端到指定的频道。
    ///
    /// 一旦客户端发出订阅命令，它不再能发出任何非发布/订阅命令。该函数消耗 `self` 并返回一个 `Subscriber`。
    ///
    /// `Subscriber` 用于接收消息以及管理客户端订阅的频道列表。
    #[instrument(skip(self))]
    pub async fn subscribe(mut self, channels: Vec<String>) -> crate::Result<Subscriber> {
        // 向服务器发出订阅命令并等待确认。
        // 客户端随后将转换为“订阅者”状态，从那时起只能发出发布/订阅命令。
        self.subscribe_cmd(&channels).await?;

        // 返回 `Subscriber` 类型
        Ok(Subscriber {
            client: self,
            subscribed_channels: channels,
        })
    }

    /// 核心的 `SUBSCRIBE` 逻辑，由各种订阅函数使用
    async fn subscribe_cmd(&mut self, channels: &[String]) -> crate::Result<()> {
        // 将 `Subscribe` 命令转换为帧
        let frame = Subscribe::new(channels.to_vec()).into_frame();

        debug!(request = ?frame);

        // 将帧写入套接字
        self.connection.write_frame(&frame).await?;

        // 对于每个被订阅的频道，服务器会响应一个确认订阅该频道的消息。
        for channel in channels {
            // 读取响应
            let response = self.read_response().await?;

            // 验证它是订阅确认。
            match response {
                Frame::Array(ref frame) => match frame.as_slice() {
                    // 服务器以如下形式的数组帧响应：
                    //
                    // ```
                    // [ "subscribe", channel, num-subscribed ]
                    // ```
                    //
                    // 其中 channel 是频道的名称，
                    // num-subscribed 是客户端当前订阅的频道数量。
                    [subscribe, schannel, ..]
                        if *subscribe == "subscribe" && *schannel == channel => {}
                    _ => return Err(response.to_error()),
                },
                frame => return Err(frame.to_error()),
            };
        }

        Ok(())
    }

    /// 从套接字读取响应帧。
    ///
    /// 如果接收到 `Error` 帧，则将其转换为 `Err`。
    async fn read_response(&mut self) -> crate::Result<Frame> {
        let response = self.connection.read_frame().await?;

        debug!(?response);

        match response {
            // 将错误帧转换为 `Err`
            Some(Frame::Error(msg)) => Err(msg.into()),
            Some(frame) => Ok(frame),
            None => {
                // 接收到 `None` 表示服务器已关闭连接而未发送帧。这是意外的，被表示为“连接被对端重置”错误。
                let err = Error::new(ErrorKind::ConnectionReset, "connection reset by server");

                Err(err.into())
            }
        }
    }
}

impl Subscriber {
    /// 返回当前订阅的频道集合。
    pub fn get_subscribed(&self) -> &[String] {
        &self.subscribed_channels
    }

    /// 接收在订阅频道上发布的下一条消息，必要时等待。
    ///
    /// `None` 表示订阅已被终止。
    pub async fn next_message(&mut self) -> crate::Result<Option<Message>> {
        match self.client.connection.read_frame().await? {
            Some(mframe) => {
                debug!(?mframe);

                match mframe {
                    Frame::Array(ref frame) => match frame.as_slice() {
                        [message, channel, content] if *message == "message" => Ok(Some(Message {
                            channel: channel.to_string(),
                            content: Bytes::from(content.to_string()),
                        })),
                        _ => Err(mframe.to_error()),
                    },
                    frame => Err(frame.to_error()),
                }
            }
            None => Ok(None),
        }
    }

    /// 将订阅者转换为一个 `Stream`，生成在订阅频道上发布的新消息。
    ///
    /// `Subscriber` 本身并不实现流，因为使用安全代码实现这一点并不简单。使用 async/await 需要手动实现 `unsafe` 的流代码。
    /// 因此，提供了一个转换函数，并结合 `async-stream` crate 来实现返回的流。
    pub fn into_stream(mut self) -> impl Stream<Item = crate::Result<Message>> {
        // 使用 `async-stream` crate 中的 `try_stream` 宏。在 Rust 中生成器尚不稳定。
        // 该 crate 使用宏来模拟基于 async/await 的生成器。存在一些限制，因此请阅读相关文档。
        try_stream! {
            while let Some(message) = self.next_message().await? {
                yield message;
            }
        }
    }

    /// 订阅新的频道列表
    #[instrument(skip(self))]
    pub async fn subscribe(&mut self, channels: &[String]) -> crate::Result<()> {
        // 发出订阅命令
        self.client.subscribe_cmd(channels).await?;

        // 更新已订阅的频道集合。
        self.subscribed_channels
            .extend(channels.iter().map(Clone::clone));

        Ok(())
    }

    /// 取消订阅指定的频道列表
    #[instrument(skip(self))]
    pub async fn unsubscribe(&mut self, channels: &[String]) -> crate::Result<()> {
        let frame = Unsubscribe::new(channels).into_frame();

        debug!(request = ?frame);

        // 将帧写入套接字
        self.client.connection.write_frame(&frame).await?;

        // 如果输入的频道列表为空，服务器会确认取消订阅所有已订阅的频道，
        // 因此我们断言接收到的取消订阅列表与客户端订阅的列表相匹配
        let num = if channels.is_empty() {
            self.subscribed_channels.len()
        } else {
            channels.len()
        };

        // 读取响应
        for _ in 0..num {
            let response = self.client.read_response().await?;

            match response {
                Frame::Array(ref frame) => match frame.as_slice() {
                    [unsubscribe, channel, ..] if *unsubscribe == "unsubscribe" => {
                        let len = self.subscribed_channels.len();

                        if len == 0 {
                            // 必须至少有一个频道
                            return Err(response.to_error());
                        }

                        // 已取消订阅的频道现在应该存在于订阅列表中
                        self.subscribed_channels.retain(|c| *channel != &c[..]);

                        // 只应从订阅频道列表中删除一个频道。
                        if self.subscribed_channels.len() != len - 1 {
                            return Err(response.to_error());
                        }
                    }
                    _ => return Err(response.to_error()),
                },
                frame => return Err(frame.to_error()),
            };
        }

        Ok(())
    }
}
