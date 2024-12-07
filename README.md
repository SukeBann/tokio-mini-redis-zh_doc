# mini-redis

`mini-redis` 是一个不完整且符合惯用法的 [Redis](https://redis.io) 客户端和服务器实现，基于 [Tokio](https://tokio.rs) 构建。

此项目的目的是提供一个编写 Tokio 应用程序的较大示例。

**免责声明** 请不要在生产环境中使用 mini-redis。该项目旨在作为学习资源，因此省略了 Redis 协议的某些部分，因为它们的实现无法引入任何新的概念。我们不会因为您项目的需要而添加新功能——请使用功能齐全的替代方案。

## 为什么选择 Redis

该项目的主要目标是教授 Tokio。实现这一目标需要一个功能广泛、实现简单的项目。Redis 作为一个内存数据库，功能广泛且使用简单的传输协议。广泛的功能允许在“真实世界”环境中展示许多 Tokio 模式。

Redis 传输协议的文档可以在[这里](https://redis.io/topics/protocol)找到。

Redis 提供的命令集可以在[这里](https://redis.io/commands)找到。

## 运行

该仓库提供了一个服务器、客户端库和一些用于与服务器交互的客户端可执行文件。

启动服务器：

```bash
RUST_LOG=debug cargo run --bin mini-redis-server
```

[`tracing`](https://github.com/tokio-rs/tracing) crate 用于提供结构化日志。您可以将 `debug` 替换为所需的[日志级别][level]。

[level]: https://docs.rs/tracing-subscriber/latest/tracing_subscriber/filter/struct.EnvFilter.html#directives

然后，在另一个终端窗口中，可以执行各种客户端[示例](examples)。例如：

```bash
cargo run --example hello_world
```

此外，还提供了一个命令行客户端用于从终端运行任意命令。在服务器运行的情况下，可以执行以下命令：

```bash
cargo run --bin mini-redis-cli set foo bar

cargo run --bin mini-redis-cli get foo
```

## OpenTelemetry

如果您正在运行多个应用程序实例（例如，您在开发云服务时通常会遇到这种情况），则需要一种方法将所有跟踪数据从主机导出到集中位置。这里有很多选项，比如 Prometheus、Jaeger、DataDog、Honeycomb、AWS X-Ray 等。

我们选择使用 OpenTelemetry，因为它是一个开放标准，允许为上述所有选项（及更多）使用统一的数据格式。这消除了供应商锁定的风险，因为如果需要，您可以在提供商之间切换。

### AWS X-Ray 示例

要启用发送跟踪数据到 X-Ray，请使用 `otel` 功能：
```bash
RUST_LOG=debug cargo run --bin mini-redis-server --features otel
```

这将把 `tracing` 切换为使用 `tracing-opentelemetry`。您需要在同一主机上运行一个 AWSOtelCollector 实例。

出于演示目的，您可以按照以下链接中记录的设置进行操作：
https://github.com/aws-observability/aws-otel-collector/blob/main/docs/developers/docker-demo.md#run-a-single-aws-otel-collector-instance-in-docker

## 支持的命令

`mini-redis` 当前支持以下命令：

* [PING](https://redis.io/commands/ping)
* [GET](https://redis.io/commands/get)
* [SET](https://redis.io/commands/set)
* [PUBLISH](https://redis.io/commands/publish)
* [SUBSCRIBE](https://redis.io/commands/subscribe)

Redis 传输协议规范可以在[这里](https://redis.io/topics/protocol)找到。

目前尚不支持持久化。

## Tokio 模式

该项目展示了许多有用的模式，包括：

### TCP 服务器

[`server.rs`](src/server.rs) 启动一个接受连接的 TCP 服务器，并为每个连接生成一个新任务。它优雅地处理 `accept` 错误。

### 客户端库

[`client.rs`](src/clients/client.rs) 展示了如何建模异步客户端。各种功能以 `async` 方法形式公开。

### 跨套接字的状态共享

服务器维护一个 [`Db`] 实例，该实例可以从所有连接的连接中访问。[`Db`] 实例管理键值状态以及发布/订阅功能。

[`Db`]: src/db.rs

### 帧处理

[`connection.rs`](src/connection.rs) 和 [`frame.rs`](src/frame.rs) 展示了如何符合惯用法地实现传输协议。协议使用中间表示法 `Frame` 结构建模。`Connection` 接受一个 `TcpStream` 并公开一个发送和接收 `Frame` 值的 API。

### 优雅关闭

服务器实现优雅的关闭。[`tokio::signal`] 用于监听 SIGINT。一旦接收到信号，关闭过程就开始了。服务器停止接受新的连接。现有连接被通知以优雅地关闭。正在进行的工作完成后，连接将关闭。

[`tokio::signal`]: https://docs.rs/tokio/*/tokio/signal/

### 并发连接限制

服务器使用一个 [`Semaphore`] 限制最大并发连接数。一旦达到限制，服务器就会停止接受新的连接，直到现有的连接终止。

[`Semaphore`]: https://docs.rs/tokio/*/tokio/sync/struct.Semaphore.html

### 发布/订阅

服务器实现了非平凡的发布/订阅功能。客户端可以订阅多个通道并随时更新其订阅。服务器使用每个通道一个[广播频道][broadcast]和每个连接一个[`StreamMap`]实现这一点。客户端可以向服务器发送订阅命令以更新活动订阅。

[broadcast]: https://docs.rs/tokio/*/tokio/sync/broadcast/index.html
[`StreamMap`]: https://docs.rs/tokio-stream/*/tokio_stream/struct.StreamMap.html

### 在异步应用程序中使用 `std::sync::Mutex`

服务器使用 `std::sync::Mutex` 而**不是** Tokio 互斥锁来同步访问共享状态。有关详细信息，请参阅 [`db.rs`](src/db.rs)。

### 测试依赖时间的异步代码

在 [`tests/server.rs`](tests/server.rs) 中，有一些关于键过期的测试。这些测试依赖于时间流逝。为了使测试具有确定性，时间使用 Tokio 的测试工具进行了模拟。

## 贡献

欢迎对 `mini-redis` 的贡献。请记住，该项目的目标**不是**与真正的 Redis 达到功能一致，而是展示 Tokio 下的异步 Rust 模式。

仅在添加命令或其他功能能用于展示新模式时才应进行。

贡献应该附带针对新 Tokio 用户的详细注释。

只专注于澄清和改进注释的贡献是非常受欢迎的。

## 许可证

此项目根据 [MIT 许可证](LICENSE) 授权。

### 贡献

除非您明确声明，否则您故意提交的 `mini-redis` 包含的任何贡献，均根据 MIT 授权，而无任何额外条款和条件。