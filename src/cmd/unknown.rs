use crate::{Connection, Frame};

use tracing::{debug, instrument};

/// 表示一个“未知”的命令。 这不是一个真正的 `Redis` 命令。
#[derive(Debug)]
pub struct Unknown {
    command_name: String,
}

impl Unknown {
    /// 创建一个新的 `Unknown` 命令，用于响应客户端发出的未知命令。
    pub(crate) fn new(key: impl ToString) -> Unknown {
        Unknown {
            command_name: key.to_string(),
        }
    }

    /// 返回命令名称
    pub(crate) fn get_name(&self) -> &str {
        &self.command_name
    }

    /// 响应客户端，表明该命令不被识别。
    ///
    /// 这通常意味着该命令尚未被 `mini-redis` 实现。
    #[instrument(skip(self, dst))]
    pub(crate) async fn apply(self, dst: &mut Connection) -> crate::Result<()> {
        let response = Frame::Error(format!("ERR unknown command '{}'", self.command_name));

        debug!(?response);

        dst.write_frame(&response).await?;
        Ok(())
    }
}
