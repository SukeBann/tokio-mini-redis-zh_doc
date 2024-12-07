use crate::{Connection, Db, Frame, Parse};

use bytes::Bytes;
use tracing::{debug, instrument};

/// 获取键的值。
///
/// 如果键不存在，则返回特殊值 nil。如果存储在键处的值不是字符串，则返回错误，因为 GET 只处理字符串值。
#[derive(Debug)]
pub struct Get {
    /// 要获取的键名
    key: String,
}

impl Get {
    /// 创建一个新的 `Get` 命令以获取 `key`。
    pub fn new(key: impl ToString) -> Get {
        Get {
            key: key.to_string(),
        }
    }

    /// 获取键名
    pub fn key(&self) -> &str {
        &self.key
    }

    /// 从接收到的帧中解析一个 `Get` 实例。
    ///
    /// `Parse` 参数提供了一个类似光标的 API，用于从 `Frame` 中读取字段。
    /// 此时，整个帧已经从套接字接收到。
    ///
    /// `GET` 字符串已经被解析消耗。
    ///
    /// # 返回
    ///
    /// 成功时返回 `Get` 值。如果帧格式错误，则返回 `Err`。
    ///
    /// # 格式
    ///
    /// 期望一个包含两个条目的数组帧。
    ///
    /// ```text
    /// GET key
    /// ```
    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Get> {
        // `GET` 字符串已经被解析消耗。下一个值是要获取的键名。
        // 如果下一个值不是字符串或输入已完全消耗，则返回错误。
        let key = parse.next_string()?;

        Ok(Get { key })
    }

    /// 将 `Get` 命令应用到指定的 `Db` 实例。
    ///
    /// 响应写入到 `dst`。服务器调用此函数以执行接收到的命令。
    #[instrument(skip(self, db, dst))]
    pub(crate) async fn apply(self, db: &Db, dst: &mut Connection) -> crate::Result<()> {
        // 从共享数据库状态获取值
        let response = if let Some(value) = db.get(&self.key) {
            // 如果存在值，以 "bulk" 格式写入客户端。
            Frame::Bulk(value)
        } else {
            // 如果没有值，写入 `Null`。
            Frame::Null
        };

        debug!(?response);

        // 将响应写回客户端
        dst.write_frame(&response).await?;

        Ok(())
    }

    /// 将命令转换为等效的 `Frame`。
    ///
    /// 客户端在编码一个 `Get` 命令以发送到服务器时调用此函数。
    pub(crate) fn into_frame(self) -> Frame {
        let mut frame = Frame::array();
        frame.push_bulk(Bytes::from("get".as_bytes()));
        frame.push_bulk(Bytes::from(self.key.into_bytes()));
        frame
    }
}
