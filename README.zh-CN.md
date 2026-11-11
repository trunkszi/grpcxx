# grpcxx (中文文档)

[English](README.md)

`grpcxx` 是面向 Linux 的 C++26 gRPC 框架，覆盖线协议组帧、同步与 Proactor 客户端/服务端、流式 RPC、编解码、压缩、C++26 P2996 反射与代码生成。

全部库目标启用 `-fno-exceptions` 与 C++26 P2996 反射 / P3394 注解编译，可失败接口统一按值返回 `std::expected<T, grpcxx::Status>`。

## 双执行模式

`grpcxx` 在同一套 `nexora::Http2Connection` 与 gRPC 帧/压缩/元数据核心之上提供两种一等公民执行模式：

### 1. Proactor Run-to-Completion 异步模式

- `ClientChannelService` 与 `GrpcServerService` 直接驱动调用方持有的 `uring::Proactor&`、`nexora::CompletionRoutes<>&`、`nexora::IoUringSocket` / `nexora::TcpListener`、`nexora::Http2Connection` 以及可选的 `nexora::TlsClientConfig` / `nexora::TlsServerConfig`，稳态 RPC 帧分发零堆分配。
- `ChannelCoordinatorService` 协同 `ChannelConnectivityFsm` 状态机、`cares::Endpoint` 列表与 `nexora::EndpointFailover`。
- `ServiceRouter` 与 `ClientStub` 绑定 C++26 反射服务契约；一元处理器可立即返回 `std::expected<grpcxx::Response<T>, grpcxx::Status>`，也可持有仅移动的 `UnaryResponder<T>` 择机异步完成。
- `StreamMultiplexer` 为服务端流、客户端流与双向流 RPC 提供有界队列与带代数校验的流句柄。

### 2. 调用线程同步与流式模式

- `SynchronousChannel` 支持通过 `nexora::TcpTransport`（经由 `cares::Resolver` 解析端点）、`nexora::UnixSocket`、外部传入文件描述符或 `nexora::MemoryPipe` 建立连接，可选启用 `nexora::TlsClientConfig`（强制 `AlpnProtocol::kHttp2`）及连接/读写停滞超时截止期。
- 默认配置 256 KiB I/O 缓冲区、64 KiB 头部块上限、非聚合流式消息投递与 16 MiB HTTP/2 流控窗口，原生支持 4 MiB gRPC / Arrow Flight 大消息批次与 64 KiB 元数据块流式收发。
- 提供 `Unary`、`ClientStreamCall`、`ServerStreamCall`、`BidiStreamCall`、`Open`、`PollOnce`、`SetProgressHook`、`ActiveStreams` 与 `NativeHandle`，配合 `CallOptions` 与 `RpcResponse` 使用。
- `RpcStream` 提供 `Send`、`HalfClose`、`Read`（`RecvNext`）、`TryRecvNext`、`Finish`、`Cancel`（未完成流析构时自动发送 `RST_STREAM(kCancel)`）、`Id`、`Done`、`InitialMetadata`、`TrailingMetadata` 与 `FinalStatus`。
- `SynchronousServer` 与 `InMemoryChannelPair` 在 `nexora::MemoryPipe`、`TcpTransport` 或 `UnixSocket` 之上提供确定性单线程 HTTP/2 gRPC 服务端对端（`SetInitialMetadata`、`Send`、`Finish`、`PollOnce`、`CancelledStreams`、`ActiveStreams`、`WindowUpdatesReceived`），覆盖一元与全部三种流式 RPC 形态的确定性测试。

## 编解码、压缩与契约代码生成

- **编解码器**：支持 Protobuf（`grpcxx::codec_protobuf`）、Cap'n Proto（`grpcxx::codec_capnp`，支持 8 字节对齐零拷贝借用读取）与 Apache Fory（`grpcxx::codec_fory`）。
- **消息压缩（`grpcxx::compression`）**：支持 Identity、Gzip、Deflate、Zstd、LZ4 与 Snappy，内置最大解压体积上限与默认 256 字节免压缩阈值。
- **契约生成工具**：`protoc-gen-grpcxx`、`grpcxx-codegen` 与 CMake 辅助函数 `grpcxx_generate_contracts()` 可直接从 `.proto`、`.capnp` 与 `.fdl` 生成 C++26 反射服务契约头文件。

## 编译期反射一元服务示例

```cpp
#include <grpcxx/reflection/service_router.h>

#include <cstdint>
#include <expected>
#include <optional>
#include <utility>

struct EchoMessage {
  std::uint32_t sequence{};
};

struct [[= grpcxx::service{.name = "example.Echo"}]] EchoService {
  [[= grpcxx::rpc{.name = "Immediate", .kind = grpcxx::RpcKind::kUnary}]]
  EchoMessage Immediate(EchoMessage request);
  [[= grpcxx::rpc{.name = "Deferred", .kind = grpcxx::RpcKind::kUnary}]]
  EchoMessage Deferred(EchoMessage request);
};

struct EchoHandlers {
  std::optional<grpcxx::UnaryResponder<EchoMessage>> pending;
  EchoMessage deferred{};

  auto Immediate(grpcxx::ServerContext&, const EchoMessage& request) noexcept
      -> std::expected<grpcxx::Response<EchoMessage>, grpcxx::Status> {
    return grpcxx::Response<EchoMessage>{.message = {.sequence = request.sequence + 1}};
  }

  void Deferred(grpcxx::ServerContext&, const EchoMessage& request,
                grpcxx::UnaryResponder<EchoMessage> responder) noexcept {
    deferred.sequence = request.sequence + 2;
    pending.emplace(std::move(responder));
  }
};
```

完整示例参见 [`examples/cpp26_reflection_unary/main.cc`](examples/cpp26_reflection_unary/main.cc)。

## 构建与验证

需要 CMake >= 4.0、Ninja、支持 `io_uring` 的 Linux `x86_64` 环境，以及支持 P2996 反射与 P3394 注解的 C++26 编译器（GCC 16 `-freflection` 或 Clang P2996 `-freflection-latest`）。依赖通过 vcpkg Manifest 模式解析，overlay 端口目录为 `VCPKG_OVERLAY_PORTS=$HOME/.local/vcpkg`。

```sh
./build.py --help
./build.py setup --help
./build.py build
./build.py test
./build.py bench
./build.py format --check src
```

| CMake Preset | 说明 |
| --- | --- |
| `gcc-glibc` | GCC 16 反射工具链（默认） |
| `gcc-asan` | GCC 16 + AddressSanitizer 与 UndefinedBehaviorSanitizer |
| `clang-glibc` | Clang P2996 反射工具链（`libc++`） |
| `clang-asan` | Clang P2996 + AddressSanitizer 与 UndefinedBehaviorSanitizer |
| `gcc-musl` | GCC 16 / musl 反射工具链 |
| `clang-musl` | Clang P2996 / musl 反射工具链 |

## CMake / vcpkg 集成

```cmake
find_package(grpcxx CONFIG REQUIRED)
target_link_libraries(app PRIVATE grpcxx::grpcxx)
```

| 导出目标 | 职责 |
| --- | --- |
| `grpcxx::grpcxx` | 聚合接口目标，链接核心服务、流式多路复用、通道协调、压缩与已启用的编解码后端 |
| `grpcxx::core`、`grpcxx::framing` | 状态码、元数据、超时截止期与 8 字节对齐长度前缀 gRPC 帧 |
| `grpcxx::h2`、`grpcxx::reflection` | 基于 `nexora::Http2Connection` 的 gRPC HTTP/2 会话、`SynchronousChannel`/`RpcStream`/`SynchronousServer` 及 C++26 反射契约路由 |
| `grpcxx::client`、`grpcxx::server`、`grpcxx::channel` | Proactor 模式的 `ClientChannelService`、`GrpcServerService` 与 `ChannelCoordinatorService` |
| `grpcxx::streaming`、`grpcxx::interceptor`、`grpcxx::services` | 有界流式多路复用、调用拦截器、健康检查与反射注册表 |
| `grpcxx::compression` | Identity、Gzip、Deflate、Zstd、LZ4 与 Snappy 消息压缩 |
| `grpcxx::codec_protobuf`、`grpcxx::codec_capnp`、`grpcxx::codec_fory` | IDL 编解码器后端 |

## 示例与基准测试

| 示例目录 | 可执行文件 | 说明 |
| --- | --- | --- |
| `examples/cpp26_reflection_unary` | `grpcxx-cpp26-reflection-unary` | 基于 `uring::Proactor` 的立即返回与延迟完成反射一元 RPC |
| `examples/multi_codec_showcase` | `multi_codec_showcase` | Protobuf、Cap'n Proto 与 Apache Fory 三编解码器记录存储契约互调 |
| `examples/streaming_showcase` | `streaming_showcase` | 调用方驱动流式 RPC 与有界流控队列 |
| `examples/tls_and_compression` | `tls_and_compression` | TLS 1.3（`h2` ALPN）与多算法消息压缩 |
| `examples/task_service` | `task_service` | 端到端延迟一元调用、流式推送、取消与优雅排空 |

基准测试目标（`./build.py bench -t <target>`）：`bench_aligned_framing`、`bench_service_router`、`bench_codecs`（`bench_multi_codec`）、`bench_compression` 与 `bench_rpc_pipeline`。

更多接口说明参见 [docs/README.md](docs/README.md)、[docs/api.md](docs/api.md) 与 [docs/protoc-gen-grpcxx.md](docs/protoc-gen-grpcxx.md)。
