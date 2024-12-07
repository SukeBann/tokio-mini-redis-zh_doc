use crate::cmd::{Parse, ParseError, Unknown};
use crate::{Command, Connection, Db, Frame, Shutdown};

use bytes::Bytes;
use std::pin::Pin;
use tokio::select;
use tokio::sync::broadcast;
use tokio_stream::{Stream, StreamExt, StreamMap};

/// 订阅客户端至一个或多个频道。
///
/// 一旦客户端进入订阅状态, 除了额外的 SUBSCRIBE、PSUBSCRIBE、UNSUBSCRIBE、
/// PUNSUBSCRIBE、PING 和 QUIT 命令外，不应发布其他命令。
#[derive(Debug)]
pub struct Subscribe {
    channels: Vec<String>,
}

/// 从一个或多个频道中取消客户端的订阅。
///
/// 当未指定频道时，客户端会从所有之前订阅的频道中取消订阅。
#[derive(Clone, Debug)]
pub struct Unsubscribe {
    channels: Vec<String>,
}

/// 消息流。该流从 `broadcast::Receiver` 接收消息。我们使用 `stream!` 来创建一个
/// 消费消息的 `Stream`。因为 `stream!` 的值不能命名，我们使用 trait 对象对流进行装箱。
type Messages = Pin<Box<dyn Stream<Item = Bytes> + Send>>;

impl Subscribe {
    /// 创建一个新的 `Subscribe` 命令以监听指定的频道。
    pub(crate) fn new(channels: Vec<String>) -> Subscribe {
        Subscribe { channels }
    }

    /// 从接收到的帧中解析一个 `Subscribe` 实例。
    ///
    /// `Parse` 参数提供了一个类似光标的 API 来从 `Frame` 中读取字段。
    /// 此时，整个帧已经从套接字中接收到。
    ///
    /// `SUBSCRIBE` 字符串已经被解析消耗。
    ///
    /// # 返回
    ///
    /// 成功时，返回 `Subscribe` 值。如果帧格式错误，返回 `Err`。
    ///
    /// # 格式
    ///
    /// 期望一个包含两个或更多条目的数组帧。
    ///
    /// ```text
    /// SUBSCRIBE channel [channel ...]
    /// ```
    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Subscribe> {
        use ParseError::EndOfStream;

        // `SUBSCRIBE` 字符串已经被解析消耗。
        // 此时，在 `parse` 中还剩下一个或多个字符串。
        // 这些字符串代表要订阅的频道。
        //
        // 提取第一个字符串。如果没有，帧格式错误并向上传递错误。
        let mut channels = vec![parse.next_string()?];

        // 现在，消耗帧的其余部分。
        // 每个值必须是字符串，否则帧格式错误。
        // 一旦帧中的所有值都被消耗，命令就被完全解析。
        loop {
            match parse.next_string() {
                // 从 `parse` 中消耗了一个字符串，将其推入要订阅的频道列表。
                Ok(s) => channels.push(s),
                // `EndOfStream` 错误表示没有更多数据可解析。
                Err(EndOfStream) => break,
                // 所有其他错误都被向上传递，导致连接终止。
                Err(err) => return Err(err.into()),
            }
        }

        Ok(Subscribe { channels })
    }

    /// 将 `Subscribe` 命令应用于指定的 `Db` 实例。
    ///
    /// 这个函数是入口点，包含了要订阅的初始频道列表。
    /// 客户端可能会接收到额外的 `subscribe` 和 `unsubscribe` 命令，
    /// 并据此更新订阅列表。
    ///
    /// [这里]: https://redis.io/topics/pubsub
    pub(crate) async fn apply(
        mut self,
        db: &Db,
        dst: &mut Connection,
        shutdown: &mut Shutdown,
    ) -> crate::Result<()> {
        // 每个单独的频道订阅是使用 `sync::broadcast` 频道处理的。
        // 然后，消息被扩展到当前订阅这些频道的所有客户端。
        //
        // 一个单独的客户端可以订阅多个频道，并可以动态地添加和移除其订阅集中的频道。
        // 为了处理这一点，使用 `StreamMap` 来跟踪活动订阅。
        // `StreamMap` 将接收到的来自各个广播频道的消息合并。
        let mut subscriptions = StreamMap::new();

        loop {
            // `self.channels` 用于跟踪要额外订阅的频道。在执行 `apply` 的过程中
            // 收到新的 `SUBSCRIBE` 命令时，新频道被推入这个 vec。
            for channel_name in self.channels.drain(..) {
                subscribe_to_channel(channel_name, &mut subscriptions, db, dst).await?;
            }

            // 等待以下事件之一发生：
            //
            // - 从已订阅频道之一接收到信息。
            // - 从客户端接收到订阅或取消订阅命令。
            // - 服务器关闭信号。
            select! {
                // 从已订阅的频道接收消息
                Some((channel_name, msg)) = subscriptions.next() => {
                    dst.write_frame(&make_message_frame(channel_name, msg)).await?;
                }
                res = dst.read_frame() => {
                    let frame = match res? {
                        Some(frame) => frame,
                        // 这种情况发生在远程客户端已断开连接时。
                        None => return Ok(())
                    };

                    handle_command(
                        frame,
                        &mut self.channels,
                        &mut subscriptions,
                        dst,
                    ).await?;
                }
                _ = shutdown.recv() => {
                    return Ok(());
                }
            };
        }
    }
    /// 将命令转换为等效的 `Frame`。
    ///
    /// 当客户端编码一个要发送到服务器的 `Subscribe` 命令时调用此方法。
    pub(crate) fn into_frame(self) -> Frame {
        let mut frame = Frame::array();
        frame.push_bulk(Bytes::from("subscribe".as_bytes()));
        for channel in self.channels {
            frame.push_bulk(Bytes::from(channel.into_bytes()));
        }
        frame
    }
}

async fn subscribe_to_channel(
    channel_name: String,
    subscriptions: &mut StreamMap<String, Messages>,
    db: &Db,
    dst: &mut Connection,
) -> crate::Result<()> {
    let mut rx = db.subscribe(channel_name.clone());

    // 订阅频道。
    let rx = Box::pin(async_stream::stream! {
        loop {
            match rx.recv().await {
                Ok(msg) => yield msg,
                // 如果我们在消费消息时落后了，只需继续。
                Err(broadcast::error::RecvError::Lagged(_)) => {}
                Err(_) => break,
            }
        }
    });

    // 在此客户端的订阅集中跟踪订阅。
    subscriptions.insert(channel_name.clone(), rx);

    // 响应成功订阅
    let response = make_subscribe_frame(channel_name, subscriptions.len());
    dst.write_frame(&response).await?;

    Ok(())
}

/// 处理在 `Subscribe::apply` 中接收到的命令。在此上下文中只允许订阅和取消订阅命令。
///
/// 任何新的订阅都会被追加到 `subscribe_to` 中，而不是修改 `subscriptions`。
async fn handle_command(
    frame: Frame,
    subscribe_to: &mut Vec<String>,
    subscriptions: &mut StreamMap<String, Messages>,
    dst: &mut Connection,
) -> crate::Result<()> {
    // 从客户端接收到一个命令。
    //
    // 在此上下文中只允许 `SUBSCRIBE` 和 `UNSUBSCRIBE` 命令。
    match Command::from_frame(frame)? {
        Command::Subscribe(subscribe) => {
            // `apply` 方法会订阅我们添加到此向量中的频道。
            subscribe_to.extend(subscribe.channels.into_iter());
        }
        Command::Unsubscribe(mut unsubscribe) => {
            // 如果没有指定频道，这将请求取消订阅**所有**频道。
            // 要实现这一点，将 `unsubscribe.channels` vec 填充为当前已订阅的频道列表。
            if unsubscribe.channels.is_empty() {
                unsubscribe.channels = subscriptions
                    .keys()
                    .map(|channel_name| channel_name.to_string())
                    .collect();
            }

            for channel_name in unsubscribe.channels {
                subscriptions.remove(&channel_name);

                let response = make_unsubscribe_frame(channel_name, subscriptions.len());
                dst.write_frame(&response).await?;
            }
        }
        command => {
            let cmd = Unknown::new(command.get_name());
            cmd.apply(dst).await?;
        }
    }
    Ok(())
}

/// 创建对订阅请求的响应。
///
/// 所有这些函数都将 `channel_name` 作为 `String` 而不是 `&str`，因为 `Bytes::from` 可以重用 `String` 中的分配，
/// 而使用 `&str` 则需要复制数据。这允许调用者决定是否克隆频道名称。
fn make_subscribe_frame(channel_name: String, num_subs: usize) -> Frame {
    let mut response = Frame::array();
    response.push_bulk(Bytes::from_static(b"subscribe"));
    response.push_bulk(Bytes::from(channel_name));
    response.push_int(num_subs as u64);
    response
}

/// 创建对取消订阅请求的响应。
fn make_unsubscribe_frame(channel_name: String, num_subs: usize) -> Frame {
    let mut response = Frame::array();
    response.push_bulk(Bytes::from_static(b"unsubscribe"));
    response.push_bulk(Bytes::from(channel_name));
    response.push_int(num_subs as u64);
    response
}

/// 创建一个消息，用于通知客户端有关频道上的新消息，该频道是客户端订阅的频道。
fn make_message_frame(channel_name: String, msg: Bytes) -> Frame {
    let mut response = Frame::array();
    response.push_bulk(Bytes::from_static(b"message"));
    response.push_bulk(Bytes::from(channel_name));
    response.push_bulk(msg);
    response
}

impl Unsubscribe {
    /// 使用给定的 `channels` 创建一个新的 `Unsubscribe` 命令。
    pub(crate) fn new(channels: &[String]) -> Unsubscribe {
        Unsubscribe {
            channels: channels.to_vec(),
        }
    }

    /// 从接收到的帧中解析一个 `Unsubscribe` 实例。
    ///
    /// `Parse` 参数提供了一个类似光标的 API，用于从 `Frame` 中读取字段。
    /// 此时，整个帧已经从套接字接收。
    ///
    /// `UNSUBSCRIBE` 字符串已经被解析消耗。
    ///
    /// # 返回
    ///
    /// 成功时，返回 `Unsubscribe` 值。如果帧格式错误，则返回 `Err`。
    ///
    /// # 格式
    ///
    /// 期望一个至少包含一个条目的数组帧。
    ///
    /// ```text
    /// UNSUBSCRIBE [channel [channel ...]]
    /// ```
    pub(crate) fn parse_frames(parse: &mut Parse) -> Result<Unsubscribe, ParseError> {
        use ParseError::EndOfStream;

        // 可能没有列出频道，因此从一个空的 vec 开始。
        let mut channels = vec![];

        // 帧中的每个条目必须是字符串，否则帧格式错误。
        // 一旦帧中的所有值都被消耗完，命令就被完全解析。
        loop {
            match parse.next_string() {
                // 从 `parse` 中消耗了一个字符串，将其推入要取消订阅的频道列表。
                Ok(s) => channels.push(s),
                // `EndOfStream` 错误表示没有进一步的数据可以解析。
                Err(EndOfStream) => break,
                // 所有其他错误都被向上传递，导致连接终止。
                Err(err) => return Err(err),
            }
        }

        Ok(Unsubscribe { channels })
    }

    /// 将命令转换为等效的 `Frame`。
    ///
    /// 当客户端编码要发送到服务器的 `Unsubscribe` 命令时调用此方法。
    pub(crate) fn into_frame(self) -> Frame {
        let mut frame = Frame::array();
        frame.push_bulk(Bytes::from("unsubscribe".as_bytes()));

        for channel in self.channels {
            frame.push_bulk(Bytes::from(channel.into_bytes()));
        }

        frame
    }
}
