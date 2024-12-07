use crate::{Connection, Frame, Parse, ParseError};
use bytes::Bytes;
use tracing::{debug, instrument};

/// 如果没有提供参数，则返回 PONG，否则返回参数的一个副本作为批量回应。
///
/// 此命令通常用于测试连接是否仍然存活，或测量延迟。
#[derive(Debug, Default)]
pub struct Ping {
    /// 可选的要返回的消息
    msg: Option<Bytes>,
}

impl Ping {
    /// 创建一个带有可选 `msg` 的新 `Ping` 命令。
    pub fn new(msg: Option<Bytes>) -> Ping {
        Ping { msg }
    }

    /// 从接收到的帧中解析一个 `Ping` 实例。
    ///
    /// `Parse` 参数提供了一个类似光标的 API，用于从 `Frame` 中读取字段。
    /// 此时，整个帧已经从套接字接收到。
    ///
    /// `PING` 字符串已经被解析消耗。
    ///
    /// # 返回
    ///
    /// 成功时返回 `Ping` 值。如果帧格式错误，则返回 `Err`。
    ///
    /// # 格式
    ///
    /// 期望一个包含 `PING` 和可选消息的数组帧。
    ///
    /// ```text
    /// PING [message]
    /// ```
    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Ping> {
        match parse.next_bytes() {
            Ok(msg) => Ok(Ping::new(Some(msg))),
            Err(ParseError::EndOfStream) => Ok(Ping::default()),
            Err(e) => Err(e.into()),
        }
    }

    /// 应用 `Ping` 命令并返回消息。
    ///
    /// 响应写入到 `dst`。服务器调用此函数以执行接收到的命令。
    #[instrument(skip(self, dst))]
    pub(crate) async fn apply(self, dst: &mut Connection) -> crate::Result<()> {
        let response = match self.msg {
            None => Frame::Simple("PONG".to_string()),
            Some(msg) => Frame::Bulk(msg),
        };

        debug!(?response);

        // 将响应写回客户端
        dst.write_frame(&response).await?;

        Ok(())
    }

    /// 将命令转换为等效的 `Frame`。
    ///
    /// 客户端在编码一个 `Ping` 命令以发送到服务器时调用此函数。
    pub(crate) fn into_frame(self) -> Frame {
        let mut frame = Frame::array();
        frame.push_bulk(Bytes::from("ping".as_bytes()));
        if let Some(msg) = self.msg {
            frame.push_bulk(msg);
        }
        frame
    }
}
