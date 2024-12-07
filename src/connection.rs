use crate::frame::{self, Frame};

use bytes::{Buf, BytesMut};
use std::io::{self, Cursor};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufWriter};
use tokio::net::TcpStream;

/// 从远程对等方发送和接收 `Frame` 值。
///
/// 在实现网络协议时，该协议上的消息通常由几个较小的消息组成，称为帧。
/// `Connection` 的目的是在底层的 `TcpStream` 上读写帧。
///
/// 为了读取帧，`Connection` 使用一个内部缓冲区，该缓冲区被填充直到有足够的字节来创建一个完整的帧。
/// 一旦发生这种情况，`Connection` 将创建帧并将其返回给调用者。
///
/// 在发送帧时，帧首先被编码到写缓冲区中。写缓冲区的内容然后被写入套接字。
#[derive(Debug)]
pub struct Connection {
    // `TcpStream`。它装饰有 `BufWriter`，提供写级别的缓冲。
    // Tokio 提供的 `BufWriter` 实现足以满足我们的需求。
    stream: BufWriter<TcpStream>,

    // 用于读取帧的缓冲区。
    buffer: BytesMut,
}

impl Connection {
    /// 创建一个新的 `Connection`，由 `socket` 支持。读写缓冲区被初始化。
    pub fn new(socket: TcpStream) -> Connection {
        Connection {
            stream: BufWriter::new(socket),
            // 默认使用4KB的读缓冲区。对于 mini redis 的用例来说，这足够了。
            // 然而，实际应用程序将希望根据其具体用例调整此值。
            // 更大的读缓冲区可能表现更好。
            buffer: BytesMut::with_capacity(4 * 1024),
        }
    }

    /// 从底层流中读取一个 `Frame` 值。
    ///
    /// 该函数等待直到检索到足够的数据以解析一个帧。
    /// 在解析帧之后，读缓冲区中剩余的任何数据都会保留到下一次调用 `read_frame` 时。
    ///
    /// # 返回值
    ///
    /// 成功时，返回接收到的帧。如果 `TcpStream` 以不会中断帧的方式关闭，则返回 `None`。
    /// 否则，返回错误。
    pub async fn read_frame(&mut self) -> crate::Result<Option<Frame>> {
        loop {
            // 尝试从缓冲的数据中解析帧。如果已有足够的数据缓存在缓冲区中，则返回该帧。
            if let Some(frame) = self.parse_frame()? {
                return Ok(Some(frame));
            }

            // 没有足够的缓冲数据来读取帧。尝试从 socket 中读取更多数据。
            //
            // 成功时，返回字节数。`0` 表示“流结束”。
            if 0 == self.stream.read_buf(&mut self.buffer).await? {
                // 远程关闭了连接。若是正常关闭，读缓冲区中不应有数据。
                // 如果有，这表明对等方在发送帧时关闭了套接字。
                if self.buffer.is_empty() {
                    return Ok(None);
                } else {
                    return Err("连接被对等方重置".into());
                }
            }
        }
    }

    /// 尝试从缓冲区中解析一个帧。如果缓冲区包含足够的数据，则返回帧并从缓冲区中移除数据。
    /// 如果缓冲区中的数据不足，则返回 `Ok(None)`。如果缓冲的数据不是有效的帧，则返回 `Err`。
    fn parse_frame(&mut self) -> crate::Result<Option<Frame>> {
        use frame::Error::Incomplete;

        // 光标用于跟踪缓冲区中的“当前位置”。光标还实现了 `bytes` crate 中的 `Buf` 接口，
        // 该接口提供了处理字节的许多有用工具。
        let mut buf = Cursor::new(&self.buffer[..]);

        // 第一步是检查缓冲的数据是否足够解析一个完整的帧。
        // 这一步通常比完整解析帧要快得多，并允许我们在知道完整帧已收到之前，跳过分配用于保存帧数据的数据结构。
        match Frame::check(&mut buf) {
            Ok(_) => {
                // `check` 函数会将光标推进到帧的末尾。
                // 由于在调用 `Frame::check` 之前光标的位置设置为零，我们通过检查光标位置来获得帧的长度。
                let len = buf.position() as usize;

                // 在传递光标给 `Frame::parse` 之前将位置重置为零。
                buf.set_position(0);

                // 从缓冲区解析帧。这会分配必要的结构来表示帧并返回帧值。
                //
                // 如果编码的帧表示无效，则返回错误。
                // 这应该终止**当前**连接，但不应影响任何其他连接的客户端。
                let frame = Frame::parse(&mut buf)?;

                // 从读取缓冲区中丢弃已解析的数据。
                //
                // 当在读取缓冲区上调用 `advance` 时，所有的数据直到 `len` 都会被丢弃。
                // 这部分细节如何实现留给 `BytesMut`。
                // 这通常通过移动内部光标来完成，但也可能通过重新分配和复制数据来完成。
                self.buffer.advance(len);

                // 返回解析的帧给调用者。
                Ok(Some(frame))
            }
            // 读取缓冲区中没有足够的数据来解析单个帧。
            // 我们必须等待从套接字接收到更多的数据。
            // 从套接字中读取将在此 `match` 语句后的语句中进行。
            //
            // 我们不希望从这里返回 `Err`，因为这个“错误”是一个预期的运行时条件。
            Err(Incomplete) => Ok(None),
            // 解析帧时遇到错误。
            // 连接现在处于无效状态。从这里返回 `Err` 将导致连接被关闭。
            Err(e) => Err(e.into()),
        }
    }

    /// 将单个 `Frame` 值写入底层流。
    ///
    /// 使用 `AsyncWrite` 提供的各种 `write_*` 函数将 `Frame` 值写入套接字。
    /// 不建议直接在 `TcpStream` 上调用这些函数，因为这会导致大量的系统调用。
    /// 但是，在*缓冲*写流上调用这些函数是可以的。数据将被写入缓冲区。
    /// 一旦缓冲区满了，它将被刷新到底层套接字。
    pub async fn write_frame(&mut self, frame: &Frame) -> io::Result<()> {
        // 将数组通过编码每个条目进行编码。其他所有的帧类型都被视为文字。
        // 目前，mini-Redis 不能编码递归的帧结构。详情参见下文。
        match frame {
            Frame::Array(val) => {
                // 编码帧类型前缀。对于数组，它是 `*`。
                self.stream.write_u8(b'*').await?;

                // 编码数组的长度。
                self.write_decimal(val.len() as u64).await?;

                // 迭代并编码数组中的每个条目。
                for entry in &**val {
                    self.write_value(entry).await?;
                }
            }
            // 帧类型是一个文字。直接编码该值。
            _ => self.write_value(frame).await?,
        }

        // 确保编码后的帧被写入套接字。上述调用是对缓冲流和写入的。
        // 调用 `flush` 将缓冲区的剩余内容写入套接字。
        self.stream.flush().await
    }

    /// 将一个帧字面量写入流中。
    async fn write_value(&mut self, frame: &Frame) -> io::Result<()> {
        match frame {
            Frame::Simple(val) => {
                self.stream.write_u8(b'+').await?;
                self.stream.write_all(val.as_bytes()).await?;
                self.stream.write_all(b"\r\n").await?;
            }
            Frame::Error(val) => {
                self.stream.write_u8(b'-').await?;
                self.stream.write_all(val.as_bytes()).await?;
                self.stream.write_all(b"\r\n").await?;
            }
            Frame::Integer(val) => {
                self.stream.write_u8(b':').await?;
                self.write_decimal(*val).await?;
            }
            Frame::Null => {
                self.stream.write_all(b"$-1\r\n").await?;
            }
            Frame::Bulk(val) => {
                let len = val.len();

                self.stream.write_u8(b'$').await?;
                self.write_decimal(len as u64).await?;
                self.stream.write_all(val).await?;
                self.stream.write_all(b"\r\n").await?;
            }
            // 从值中编码一个 `Array` 不能使用递归策略。
            // 通常，异步函数不支持递归。
            // Mini-redis 尚不需要编码嵌套的数组，因此目前跳过。
            Frame::Array(_val) => unreachable!(),
        }

        Ok(())
    }

    /// 将一个十进制帧写入流中。
    async fn write_decimal(&mut self, val: u64) -> io::Result<()> {
        use std::io::Write;

        // Convert the value to a string
        let mut buf = [0u8; 20];
        let mut buf = Cursor::new(&mut buf[..]);
        write!(&mut buf, "{}", val)?;

        let pos = buf.position() as usize;
        self.stream.write_all(&buf.get_ref()[..pos]).await?;
        self.stream.write_all(b"\r\n").await?;

        Ok(())
    }
}
