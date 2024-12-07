use crate::cmd::{Parse, ParseError};
use crate::{Connection, Db, Frame};

use bytes::Bytes;
use std::time::Duration;
use tracing::{debug, instrument};

/// 设置 `key` 以保存字符串 `value`。
///
/// 如果 `key` 已经持有一个值，则不管其类型如何，它都会被覆盖。
/// 任何与该键关联的先前生存时间在成功的 SET 操作时都会被丢弃。
///
/// # 选项
///
/// 当前支持以下选项：
///
/// * EX `seconds` -- 设置过期时间，以秒为单位。
/// * PX `milliseconds` -- 设置过期时间，以毫秒为单位。
#[derive(Debug)]
pub struct Set {
    /// 查找键
    key: String,

    /// 要存储的值
    value: Bytes,

    /// 键何时过期
    expire: Option<Duration>,
}

impl Set {
    /// 创建一个新的 `Set` 命令，将 `key` 设置为 `value`。
    ///
    /// 如果 `expire` 是 `Some`，值应在指定的持续时间后过期。
    pub fn new(key: impl ToString, value: Bytes, expire: Option<Duration>) -> Set {
        Set {
            key: key.to_string(),
            value,
            expire,
        }
    }

    /// 获取键
    pub fn key(&self) -> &str {
        &self.key
    }

    /// 获取值
    pub fn value(&self) -> &Bytes {
        &self.value
    }

    /// 获取过期时间
    pub fn expire(&self) -> Option<Duration> {
        self.expire
    }

    /// 从接收到的帧中解析一个 `Set` 实例。
    ///
    /// `Parse` 参数提供了一个类似光标的 API，用于从 `Frame` 中读取字段。
    /// 此时，整个帧已经从套接字接收到。
    ///
    /// `SET` 字符串已经被解析消耗。
    ///
    /// # 返回
    ///
    /// 成功时返回 `Set` 值。如果帧格式错误，则返回 `Err`。
    ///
    /// # 格式
    ///
    /// 期望一个至少包含三个条目的数组帧。
    ///
    /// ```text
    /// SET key value [EX seconds|PX milliseconds]
    /// ```
    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Set> {
        use ParseError::EndOfStream;

        // 读取要设置的键。这是一个必填字段。
        let key = parse.next_string()?;

        // 读取要设置的值。这是一个必填字段。
        let value = parse.next_bytes()?;

        // 过期时间是可选的。如果没有其他内容跟随，则为 `None`。
        let mut expire = None;

        // 尝试解析另一个字符串。
        match parse.next_string() {
            Ok(s) if s.to_uppercase() == "EX" => {
                // 以秒为单位指定的过期时间。下一个值是一个整数。
                let secs = parse.next_int()?;
                expire = Some(Duration::from_secs(secs));
            }
            Ok(s) if s.to_uppercase() == "PX" => {
                // 以毫秒为单位指定的过期时间。下一个值是一个整数。
                let ms = parse.next_int()?;
                expire = Some(Duration::from_millis(ms));
            }
            // 目前，mini-redis 不支持任何其他的 SET 选项。此处的错误将导致连接被终止。
            // 其他连接将继续正常运行。
            Ok(_) => return Err("目前 `SET` 仅支持过期选项".into()),
            // `EndOfStream` 错误表示没有更多数据可解析。在这种情况下，这是一个正常的运行情况，
            // 表示没有指定 `SET` 选项。
            Err(EndOfStream) => {}
            // 所有其他错误都会冒泡，导致连接被终止。
            Err(err) => return Err(err.into()),
        }

        Ok(Set { key, value, expire })
    }

    /// 将 `Set` 命令应用到指定的 `Db` 实例。
    ///
    /// 响应写入到 `dst`。服务器调用此函数以执行接收到的命令。
    #[instrument(skip(self, db, dst))]
    pub(crate) async fn apply(self, db: &Db, dst: &mut Connection) -> crate::Result<()> {
        // 在共享的数据库状态中设置值。
        db.set(self.key, self.value, self.expire);

        // 创建一个成功响应并将其写入 `dst`。
        let response = Frame::Simple("OK".to_string());
        debug!(?response);
        dst.write_frame(&response).await?;

        Ok(())
    }

    /// 将命令转换为等效的 `Frame`。
    ///
    /// 客户端在编码一个 `Set` 命令以发送到服务器时调用此函数。
    pub(crate) fn into_frame(self) -> Frame {
        let mut frame = Frame::array();
        frame.push_bulk(Bytes::from("set".as_bytes()));
        frame.push_bulk(Bytes::from(self.key.into_bytes()));
        frame.push_bulk(self.value);
        if let Some(ms) = self.expire {
            // Redis 协议中的过期时间可以通过两种方式指定：
            // 1. SET key value EX seconds
            // 2. SET key value PX milliseconds
            // 采用第二种方式，因为它允许更高的精度，
            // 并且 src/bin/cli.rs 将过期参数解析为持续时间的毫秒数
            frame.push_bulk(Bytes::from("px".as_bytes()));
            frame.push_int(ms.as_millis() as u64);
        }
        frame
    }
}
