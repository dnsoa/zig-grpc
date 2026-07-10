# zig-grpc API 设计

日期:2026-07-10
状态:已评审通过(client 详细 + server 骨架)

## 目标

zig-grpc 是构建在 [zig-http2](../../../../zig-http2) 之上的 gRPC 实现。早期重点是 **client**:与 golang 的 grpc-go server 完成 unary RPC 及 client/server/bidirectional streaming 互通。server 端本阶段只定义顶层形态,保证共享类型两端通用。

## 关键决策(已确认)

1. **核心字节化 + 可插拔 Codec**:核心只做 gRPC 协议(framing/status/metadata),消息以 `[]const u8` 进出;protobuf 编解码通过 comptime 契约接入(zig-protobuf 或手写),codegen 留待后续。项目自身保持 dependency-free(仅依赖 zig-http2)。
2. **双层传输**:核心 `Channel.init` 接受任意 `*Io.Reader`/`*Io.Writer`(与 zig-http2 一致);另提供 `Channel.connectTcp()` 便捷层直连 h2c 明文(Go 端 insecure server 即可互通)。TLS 由调用方自行包装 reader/writer,后续可加 `connectTls`。
3. **Client 详细 + Server 骨架**:client API 设计到可实现粒度;server 只定顶层形态。
4. **单一 Call + comptime 类型化封装**:底层一个字节级 `RawCall` 覆盖四种 RPC 模式(grpc-go 内部同构),上层 `Call(M)` 用 comptime 方法定义绑定类型安全调用;codegen 未来只需生成 `Method` 常量。

## 工作约定:zig-http2 缺口处理

实现过程中发现 zig-http2 能力不满足时,**必须显式上报并在 zig-http2 侧修复**(带自己的测试),不得在 zig-grpc 里绕过或复制传输层逻辑。已知缺口清单见文末。

## 模块划分

```
zig-grpc/src/
├── lib.zig        // 公开出口: grpc.Channel, grpc.Method, grpc.Status, ...
├── status.zig     // Code(0-16) + Status,HTTP/2 错误码 → gRPC status 映射
├── metadata.zig   // Metadata:ascii 校验、-bin base64、grpc-timeout 编码
├── frame.zig      // 5 字节消息帧编解码(跨 DATA 事件重组、max_message_size)
├── codec.zig      // 编解码契约(comptime duck-typing + 可覆盖)
├── channel.zig    // Channel:包装 h2.Client + connectTcp 便捷层
├── call.zig       // RawCall(字节级) + Call(M)(类型化封装)
└── server.zig     // Server 骨架(注册表 + handler 签名,细节后置)
```

分层:`Call(M)`(comptime 类型化)→ `RawCall`(字节 + gRPC 状态机)→ `h2.Stream`(帧传输)。frame/status/metadata 为纯函数模块,可独立测试。

## 核心共享类型(client/server 通用)

```zig
pub const Code = enum(u8) {
    ok = 0, cancelled, unknown, invalid_argument, deadline_exceeded,
    not_found, already_exists, permission_denied, resource_exhausted,
    failed_precondition, aborted, out_of_range, unimplemented,
    internal, unavailable, data_loss, unauthenticated,
};

pub const Status = struct {
    code: Code,
    message: []const u8 = "",   // grpc-message,百分号解码后
    pub const ok: Status = .{ .code = .ok };
};

pub const Metadata = struct {
    entries: []const Entry,
    pub const Entry = struct { name: []const u8, value: []const u8 };
    pub fn get(self, name) ?[]const u8;
    pub fn getBin(self, arena, name) !?[]const u8; // "-bin" 后缀,base64 解码
};
```

规范细节:`grpc-message` 百分号编解码;`-bin` 元数据 base64(无 padding);RST/GOAWAY 的 HTTP/2 错误码按 gRPC 规范映射表转 status(`REFUSED_STREAM→unavailable`、`CANCEL→cancelled` 等);`grpc-timeout` 头编码(值 + 单位 H/M/S/m/u/n)。

## 编解码契约(Codec)

默认 codec 用 comptime duck-typing:消息类型需具备 `encode`/`decode` 方法(zig-protobuf 生成类型天然满足,手写 struct 只需实现这两个方法);可按方法覆盖为自定义函数,零运行时开销:

```zig
pub fn Method(comptime Request: type, comptime Response: type) type {
    return struct {
        path: []const u8,                    // "/pkg.Service/Method"
        pub const Req = Request;
        pub const Res = Response;
        // 默认:Req.encode(self, allocator) ![]u8 / Res.decode(arena, bytes) !Res
        encode_req: fn (std.mem.Allocator, Request) anyerror![]u8 = defaultEncode(Request),
        decode_res: fn (std.mem.Allocator, []const u8) anyerror!Response = defaultDecode(Response),
    };
}
```

注意:duck-typing 契约在接入 zig-protobuf 时需验证方法签名是否吻合;不吻合则依赖可覆盖的函数字段兜底,契约本身不变。

压缩:v1 只支持 identity。发送不压缩;收到 compressed flag=1 以 `internal` status 拒绝。`grpc-encoding` 协商留待后续。

## Channel(连接层)

```zig
pub const Channel = struct {
    pub const Options = struct {
        authority: []const u8,            // :authority,connectTcp 自动填 host:port
        scheme: []const u8 = "http",      // h2c 明文默认
        user_agent: []const u8 = "grpc-zig/0.1",
        max_recv_message_size: u32 = 4 << 20,  // 与 grpc-go 默认一致
        max_send_message_size: u32 = std.math.maxInt(u32),
    };

    // 核心:传输无关,调用方持有 reader/writer 生命周期
    pub fn init(self: *Channel, io: Io, gpa: Allocator, r: *Io.Reader, w: *Io.Writer, opts: Options) !void;

    // 便捷层:拨 TCP、h2c 直连;Channel 拥有 socket
    pub fn connectTcp(io: Io, gpa: Allocator, host: []const u8, port: u16, opts: Options) !*Channel;

    pub fn deinit(self: *Channel) void;

    pub fn unary(self, comptime M, arena: Allocator, req: M.Req, opts: CallOptions) !M.Res;
    pub fn start(self, comptime M, opts: CallOptions) !Call(M);
    pub fn startRaw(self, path: []const u8, opts: CallOptions) !RawCall;
};
```

一个 Channel = 一条 HTTP/2 连接,多路复用并发 call(zig-http2 的 MAX_CONCURRENT_STREAMS 准入控制兜底)。**明确不做**:连接池、重连、负载均衡——调用方或未来层的事。

请求头映射:`:method POST`、`:scheme`、`:path = method.path`、`:authority`、`te: trailers`、`content-type: application/grpc`、`user-agent`、`grpc-timeout`(如设置)、用户 metadata。

## Call(调用层)

```zig
pub const CallOptions = struct {
    metadata: []const Metadata.Entry = &.{},
    timeout_ns: ?u64 = null,       // 编码为 grpc-timeout 头,服务端执行
    status_out: ?*Status = null,   // unary 便捷入口用:非 OK 时带出 status
};

pub const RawCall = struct {
    pub fn sendMessage(self, bytes: []const u8) !void;   // 加 5 字节前缀,流控发送
    pub fn closeSend(self) !void;                        // 半关(空 DATA + END_STREAM)
    pub fn recvMessage(self, arena: Allocator) !?[]u8;   // null = 消息流结束(已收到 trailers)
    pub fn header(self, arena: Allocator) !Metadata;     // 阻塞至响应 HEADERS 到达
    pub fn finish(self) !Status;      // recvMessage 返回 null 后取最终状态
    pub fn trailers(self) Metadata;   // finish 之后可用
    pub fn cancel(self) void;         // 本地打断阻塞的 recv/send + 向对端发 RST_STREAM(CANCEL)(见线程模型)
    pub fn deinit(self) void;
};

pub fn Call(comptime M: anytype) type {
    return struct {
        raw: RawCall,
        pub fn send(self, msg: M.Req) !void;              // encode + sendMessage
        pub fn recv(self, arena: Allocator) !?M.Res;      // recvMessage + decode
        pub fn closeSend(self) !void;
        pub fn finish(self) !Status;
        pub fn cancel(self) void;
        pub fn deinit(self) void;
    };
}
```

### 四种模式用法

```zig
// unary:channel.unary 内部 = start → send → closeSend → recv → finish(校验 OK)
const reply = try channel.unary(SayHello, arena, .{ .name = "zig" }, .{});

// server streaming
var call = try channel.start(ListFeatures, .{});
try call.send(rect); try call.closeSend();
while (try call.recv(arena)) |feature| { ... }
_ = try call.finish();

// client streaming:多次 send → closeSend → recv 一条 → finish
// bidi:发送线程 send/closeSend,接收线程 recv/finish(与 h2.Stream 线程模型一致)
```

### 状态机

RawCall 消费 `h2.Stream.readEvent`:

- 首个 `.headers`(非 end_stream)→ 响应头:校验 `:status == 200`(非 200 按规范映射 status)、`content-type` 前缀 `application/grpc`、`grpc-encoding` 必须 identity。
- 首个 `.headers` 且 end_stream → **Trailers-Only**:直接解析 `grpc-status`/`grpc-message`,无消息。
- `.data` → 喂给 frame 解码器(跨 DATA 边界重组,超 `max_recv_message_size` 报错)。
- 后续 `.headers`(end_stream)→ trailers,解析最终 status。
- `.rst` / `.goaway` / 传输错误 → 映射为 gRPC status(`unavailable`/`cancelled` 等)存入 call。

### 错误模型

非 OK 的 gRPC 状态**不是** Zig error:`recv` 正常返回 null,`finish()` 返回 `Status` 由调用方检查。传输层故障(连接断、RST、超限)才是 Zig error,此时 call 内仍存有映射后的 status 可查。唯一例外:`channel.unary` 非 OK 返回 `error.RpcFailed`,status 经 `opts.status_out` 带出。

### 线程模型与限制(v1 明确接受)

- 每个 call 最多一个发送线程 + 一个接收线程(继承 h2.Stream 约束);`deinit` 不得与 send/recv 并发。
- `cancel()` 线程安全,且**会打断另一线程正阻塞的 `recv`**:zig-http2 的 `Stream.cancel()` 现在先本地唤醒(置 `cancelled`/`aborted` 并广播 `recv_cond`/`send_cond`),再向对端发 RST_STREAM。阻塞中的 `readEvent` 因此返回 `error.StreamCancelled`,阻塞中的 `send` 返回 `error.StreamReset`。zig-grpc 的接收状态机应把 `error.StreamCancelled` 映射为 `cancelled` 状态。
- **client 本地 deadline**:v1 仍默认交服务端执行(发 `grpc-timeout` 头),但底层已具备打断阻塞接收的能力——如需真正的本地 deadline,可在 gRPC 层起一个看门狗线程,到点调 `call.cancel()`。这不再受 zig-http2 限制(此前的缺口已修复,见文末清单)。

## Server 骨架(仅定形态)

```zig
pub const ServerCall = struct {   // handler 视角的一次 RPC
    pub fn recvMessage(self, arena) !?[]u8;
    pub fn sendMessage(self, bytes: []const u8) !void;
    pub fn sendHeader(self, md: Metadata) !void;
    request_metadata: Metadata,
};
// handler 返回值即最终 Status,框架负责写 trailers
pub const HandlerFn = fn (call: *ServerCall) anyerror!Status;

pub const Server = struct {
    pub fn init(io: Io, gpa: Allocator, opts: Options) Server;
    pub fn register(self, path: []const u8, handler: HandlerFn) !void;
    pub fn serveConn(self, r: *Io.Reader, w: *Io.Writer) !void; // 桥接 h2.serveConn
};
```

共享模块(status/metadata/frame/codec)必须两端通用;server 细节(comptime 服务注册、streaming handler 形态)留到 server 阶段设计。

## 测试策略

- **单元**:frame 编解码(跨 DATA 边界、超限拒绝)、status 映射表、grpc-timeout 编码、`-bin` base64、grpc-message 百分号编解码——全为纯函数。
- **回环集成**:内存管道上以 zig-http2 server 模拟 gRPC 对端,覆盖对抗场景(Trailers-Only、非 200、中途 RST、GOAWAY)。
- **互通 E2E(验收标准)**:`testdata/go-server/` 内置 grpc-go 服务(insecure h2c,echo unary + 三种 streaming);`zig build test-interop` 打通四种模式,需本机 Go 环境,CI 可选。

## zig-http2 依赖能力清单

设计所需能力及现状(核对于 zig-http2@48b7501):

| 能力 | 现状 |
|---|---|
| 自定义请求头(te/content-type/metadata) | ✅ `RequestHead.headers` |
| 流式请求体 + 半关 | ✅ `Stream.send(data, end_stream)`,空 DATA 半关 |
| 响应 trailers(区分首/尾 HEADERS) | ✅ `Event.headers` + `end_stream` 标志 |
| 双向并发读写 | ✅ 一发送线程 + 一接收线程 |
| 取消:通知对端 + 本地打断阻塞接收 | ✅ `Stream.cancel()`(本地唤醒 `readEvent`→`error.StreamCancelled` / `send`→`error.StreamReset`,再发 RST + 释放并发槽) |
| 接收侧流控背压 | ✅ 消费时才补窗 |
| GOAWAY 优雅处理 / 可重试判定 | ✅ `Event.goaway` + refused 语义 |
| MAX_CONCURRENT_STREAMS 准入 | ✅ `openStream` 阻塞准入 |
| **可本地打断阻塞接收** | ✅ 已修复(2026-07-10)。`Stream.cancel()` 现在先本地 abort/wake(置 `cancelled`/`aborted`、广播 `recv_cond`/`send_cond`)再发 RST;`readEvent` 在循环顶部检查 `cancelled` 并返回 `error.StreamCancelled`,阻塞 `send` 返回 `error.StreamReset`;`close()` 亦把 `cancelled` 视为已结束以免重复发 RST。两条并发测试覆盖(唤醒阻塞 reader / 释放阻塞 sender)。据此可在 gRPC 层用看门狗线程 + `cancel()` 构建本地 deadline。 |

实现中新发现的缺口按"工作约定"一节处理:上报 → zig-http2 修复 → 继续。
