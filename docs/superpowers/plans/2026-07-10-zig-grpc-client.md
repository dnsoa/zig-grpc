# zig-grpc Client Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 在 zig-http2 之上实现 gRPC client(unary + server/client/bidi streaming),与 grpc-go server 互通。

**Architecture:** 分层:`Call(M)`(comptime 类型化)→ `RawCall`(字节级 gRPC 状态机)→ `h2.Stream`(帧传输)。核心字节化,消息编解码经可覆盖的 comptime 契约接入。Channel = 一条 HTTP/2 连接。

**Tech Stack:** Zig 0.16.0(`std.Io` 模型),唯一依赖 zig-http2(path 依赖 `../zig-http2`)。互通验收用 Go + google.golang.org/grpc。

**Spec:** `docs/superpowers/specs/2026-07-10-zig-grpc-api-design.md`(本计划的需求来源;冲突时以 spec 为准)

## Global Constraints

- 工作目录:`/Users/millken/github.com/dnsoa/zig-grpc`(git 仓库已存在,直接在 main 上提交)
- Zig 版本:0.16.0;`minimum_zig_version = "0.16.0"`
- 依赖:仅 `zig_http2`(`.path = "../zig-http2"`);zig-grpc 自身 dependency-free
- 模块名 `zig_grpc`;消费者 `@import("zig_grpc")`
- v1 明确不做:压缩(仅 identity)、client 本地 deadline、连接池/重连/负载均衡、TLS(调用方自行包装)
- **zig-http2 缺口约定**:实现中发现 zig-http2 能力不足,停下来显式上报,在 zig-http2 侧修复(带测试)后继续;不得在 zig-grpc 里绕过。已知缺口「无法本地打断阻塞中的 `readEvent`」**已在 zig-http2 修复**:`Stream.cancel()` 现在会唤醒阻塞的 `readEvent`(返回 `error.StreamCancelled`)与阻塞的 `send`(返回 `error.StreamReset`)。因此 Task 7 的 `step()` readEvent 错误处理需增加一条 `error.StreamCancelled => { if (self.stat == null) self.stat = .{ .code = .cancelled, .message = "cancelled" }; self.state = .done; }` 分支(放在 `error.ConnectionClosed` 之后、`else` 之前)
- 每个任务结束运行 `zig build test`(全绿)再 commit
- gRPC wire 规范:HEADERS 映射、5 字节消息帧、`grpc-status`/`grpc-message`(percent 编码)、`grpc-timeout`、`-bin` metadata(base64 无 padding)
- zig-http2 API 核对基线:commit `48b7501`。计划中引用的 h2 API:`h2.Client.init(io,gpa,r,w)` / `openStream(RequestHead,end_stream)!*Stream` / `Stream.send(data,end_stream)` / `Stream.readEvent(arena)!Event` / `Stream.cancel()` / `Stream.close()` / `h2.Server{.io,.gpa,.handler,.userdata}` + `h2.serveConn(srv,r,w,client_ip,scheme)`(返回 void)/ `h2.Context`(`.req.get(name)`、`.body_reader.?.read(buf)`、`.res.status/header/write/trailer/finish`、`.arena`、`.userdata`)/ `h2.proto`(`preface`、`FrameType`、`flag_*`、`putHeader`、`parseHeader`、`ErrorCode`)

---

### Task 1: 项目脚手架

**Files:**
- Create: `build.zig.zon`
- Create: `build.zig`
- Create: `src/lib.zig`

**Interfaces:**
- Produces: `zig build test` 可运行;模块 `zig_grpc` 内可 `@import("zig_http2")`;后续任务向 `src/lib.zig` 的导出区和 `test` 块追加行

- [ ] **Step 1: 写 build.zig.zon**

```zig
.{
    .name = .zig_grpc,
    .version = "0.1.0",
    .fingerprint = 0x0,
    .minimum_zig_version = "0.16.0",
    .dependencies = .{
        .zig_http2 = .{ .path = "../zig-http2" },
    },
    .paths = .{
        "build.zig",
        "build.zig.zon",
        "src",
    },
}
```

- [ ] **Step 2: 写 build.zig**(镜像 zig-http2 的结构;test 模块也要 addImport)

```zig
const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const h2_dep = b.dependency("zig_http2", .{ .target = target, .optimize = optimize });
    const h2_mod = h2_dep.module("zig_http2");

    // The public library module. Consumers do
    //   b.dependency("zig_grpc", .{}).module("zig_grpc")
    // and then `@import("zig_grpc")`.
    const mod = b.addModule("zig_grpc", .{
        .root_source_file = b.path("src/lib.zig"),
        .target = target,
        .optimize = optimize,
    });
    mod.addImport("zig_http2", h2_mod);

    // ---- `zig build test` ----
    const test_step = b.step("test", "Run unit tests");
    const test_mod = b.createModule(.{
        .root_source_file = b.path("src/lib.zig"),
        .target = target,
        .optimize = optimize,
    });
    test_mod.addImport("zig_http2", h2_mod);
    const run_tests = b.addRunArtifact(b.addTest(.{ .root_module = test_mod }));
    test_step.dependOn(&run_tests.step);
}
```

- [ ] **Step 3: 写最小 src/lib.zig**

```zig
//! zig-grpc — gRPC client/server on top of zig-http2.
//! Core is bytes-in/bytes-out; message codecs plug in via comptime contracts.

const std = @import("std");

test "scaffold compiles" {
    const h2 = @import("zig_http2");
    _ = h2;
    try std.testing.expect(true);
}
```

- [ ] **Step 4: 运行 `zig build test`,修 fingerprint**

Run: `zig build test`
Expected: 首次报错 `invalid fingerprint: 0x0; declare it as .fingerprint = 0x…`(数值随机)。把报错给出的值抄进 build.zig.zon,再跑一次 → 无输出(全绿)。

- [ ] **Step 5: Commit**

```bash
git add build.zig build.zig.zon src/lib.zig
git commit -m "build: scaffold zig-grpc module depending on zig-http2"
```

---

### Task 2: status.zig — 状态码与映射

**Files:**
- Create: `src/status.zig`
- Modify: `src/lib.zig`

**Interfaces:**
- Produces: `Code`(enum(u8) ok=0..unauthenticated=16)、`Status{code, message, .ok, isOk()}`、`codeFromGrpcStatus([]const u8) Code`、`codeFromHttpStatus(u16) Code`、`codeFromH2Error(u32) Code`、`percentDecode(arena, s) ![]u8`

- [ ] **Step 1: 写失败测试**(src/status.zig 末尾)

```zig
const testing = std.testing;

test "codeFromGrpcStatus parses valid, falls back to unknown" {
    try testing.expectEqual(Code.ok, codeFromGrpcStatus("0"));
    try testing.expectEqual(Code.not_found, codeFromGrpcStatus("5"));
    try testing.expectEqual(Code.unauthenticated, codeFromGrpcStatus("16"));
    try testing.expectEqual(Code.unknown, codeFromGrpcStatus("17"));
    try testing.expectEqual(Code.unknown, codeFromGrpcStatus("abc"));
    try testing.expectEqual(Code.unknown, codeFromGrpcStatus(""));
}

test "codeFromHttpStatus follows the spec table" {
    try testing.expectEqual(Code.internal, codeFromHttpStatus(400));
    try testing.expectEqual(Code.unauthenticated, codeFromHttpStatus(401));
    try testing.expectEqual(Code.permission_denied, codeFromHttpStatus(403));
    try testing.expectEqual(Code.unimplemented, codeFromHttpStatus(404));
    try testing.expectEqual(Code.unavailable, codeFromHttpStatus(429));
    try testing.expectEqual(Code.unavailable, codeFromHttpStatus(502));
    try testing.expectEqual(Code.unavailable, codeFromHttpStatus(503));
    try testing.expectEqual(Code.unavailable, codeFromHttpStatus(504));
    try testing.expectEqual(Code.unknown, codeFromHttpStatus(418));
}

test "codeFromH2Error follows the spec table" {
    try testing.expectEqual(Code.unavailable, codeFromH2Error(0x7)); // REFUSED_STREAM
    try testing.expectEqual(Code.cancelled, codeFromH2Error(0x8)); // CANCEL
    try testing.expectEqual(Code.resource_exhausted, codeFromH2Error(0xb)); // ENHANCE_YOUR_CALM
    try testing.expectEqual(Code.permission_denied, codeFromH2Error(0xc)); // INADEQUATE_SECURITY
    try testing.expectEqual(Code.internal, codeFromH2Error(0x0));
    try testing.expectEqual(Code.internal, codeFromH2Error(0x2));
    try testing.expectEqual(Code.internal, codeFromH2Error(0xff));
}

test "percentDecode handles valid, invalid, and plain strings" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();
    try testing.expectEqualStrings("hello world", try percentDecode(arena, "hello%20world"));
    try testing.expectEqualStrings("plain", try percentDecode(arena, "plain"));
    // 非法转义原样透传(规范:decode 不得失败)
    try testing.expectEqualStrings("bad%zz", try percentDecode(arena, "bad%zz"));
    try testing.expectEqualStrings("tail%2", try percentDecode(arena, "tail%2"));
    try testing.expectEqualStrings("\xe4\xb8\xad", try percentDecode(arena, "%E4%B8%AD"));
}
```

- [ ] **Step 2: 在 lib.zig 挂上模块并跑测试确认失败**

lib.zig 加:

```zig
const status_mod = @import("status.zig");
pub const Code = status_mod.Code;
pub const Status = status_mod.Status;

test {
    _ = status_mod;
}
```

Run: `zig build test`
Expected: FAIL(`Code`/`codeFromGrpcStatus` 等未定义的编译错误)

- [ ] **Step 3: 实现 src/status.zig**

```zig
//! gRPC status codes (spec: 0–16) and the mappings from HTTP semantics.

const std = @import("std");

pub const Code = enum(u8) {
    ok = 0,
    cancelled = 1,
    unknown = 2,
    invalid_argument = 3,
    deadline_exceeded = 4,
    not_found = 5,
    already_exists = 6,
    permission_denied = 7,
    resource_exhausted = 8,
    failed_precondition = 9,
    aborted = 10,
    out_of_range = 11,
    unimplemented = 12,
    internal = 13,
    unavailable = 14,
    data_loss = 15,
    unauthenticated = 16,
};

pub const Status = struct {
    code: Code,
    message: []const u8 = "",

    pub const ok: Status = .{ .code = .ok };

    pub fn isOk(self: Status) bool {
        return self.code == .ok;
    }
};

/// Parses a `grpc-status` trailer value. Unparseable or out-of-range values
/// become `unknown` — the caller cannot do better.
pub fn codeFromGrpcStatus(value: []const u8) Code {
    const n = std.fmt.parseInt(u8, value, 10) catch return .unknown;
    if (n > 16) return .unknown;
    return @enumFromInt(n);
}

/// gRPC spec mapping for a non-200 `:status` when the response carries no
/// `grpc-status` (e.g. a proxy answered instead of a gRPC server).
pub fn codeFromHttpStatus(http_status: u16) Code {
    return switch (http_status) {
        400 => .internal,
        401 => .unauthenticated,
        403 => .permission_denied,
        404 => .unimplemented,
        429, 502, 503, 504 => .unavailable,
        else => .unknown,
    };
}

/// gRPC spec mapping for an HTTP/2 RST_STREAM error code. Codes not in the
/// spec table map to `internal`.
pub fn codeFromH2Error(code: u32) Code {
    return switch (code) {
        0x7 => .unavailable, // REFUSED_STREAM: not processed, safe to retry
        0x8 => .cancelled, // CANCEL
        0xb => .resource_exhausted, // ENHANCE_YOUR_CALM
        0xc => .permission_denied, // INADEQUATE_SECURITY
        else => .internal,
    };
}

/// Percent-decodes a `grpc-message` value. Invalid escapes pass through
/// verbatim — the spec forbids failing on them.
pub fn percentDecode(arena: std.mem.Allocator, s: []const u8) ![]u8 {
    var out: std.ArrayList(u8) = .empty;
    try out.ensureTotalCapacity(arena, s.len);
    var i: usize = 0;
    while (i < s.len) {
        if (s[i] == '%' and i + 2 < s.len) {
            const hi = std.fmt.charToDigit(s[i + 1], 16) catch {
                try out.append(arena, s[i]);
                i += 1;
                continue;
            };
            const lo = std.fmt.charToDigit(s[i + 2], 16) catch {
                try out.append(arena, s[i]);
                i += 1;
                continue;
            };
            try out.append(arena, @as(u8, @intCast(hi * 16 + lo)));
            i += 3;
        } else {
            try out.append(arena, s[i]);
            i += 1;
        }
    }
    return out.items;
}
```

- [ ] **Step 4: 跑测试通过**

Run: `zig build test`
Expected: PASS(无输出)

- [ ] **Step 5: Commit**

```bash
git add src/status.zig src/lib.zig
git commit -m "feat: gRPC status codes, HTTP/h2 mappings, grpc-message decoding"
```

---

### Task 3: metadata.zig — 元数据与 grpc-timeout

**Files:**
- Create: `src/metadata.zig`
- Modify: `src/lib.zig`

**Interfaces:**
- Produces: `Metadata{entries: []const Entry}`、`Metadata.Entry{name,value}`、`Metadata.get(name) ?[]const u8`、`Metadata.getBin(arena,name) !?[]u8`、`isReservedName([]const u8) bool`、`encodeTimeout(ns: u64, buf: *[9]u8) []const u8`

- [ ] **Step 1: 写失败测试**(src/metadata.zig 末尾)

```zig
const testing = std.testing;

test "get is case-insensitive, first match wins" {
    const md: Metadata = .{ .entries = &.{
        .{ .name = "x-trace-id", .value = "abc" },
        .{ .name = "x-trace-id", .value = "second" },
    } };
    try testing.expectEqualStrings("abc", md.get("X-Trace-Id").?);
    try testing.expect(md.get("missing") == null);
}

test "getBin decodes padded and unpadded base64" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();
    // "hi" -> aGk= (padded) / aGk (unpadded)
    const padded: Metadata = .{ .entries = &.{.{ .name = "k-bin", .value = "aGk=" }} };
    const unpadded: Metadata = .{ .entries = &.{.{ .name = "k-bin", .value = "aGk" }} };
    try testing.expectEqualStrings("hi", (try padded.getBin(arena, "k-bin")).?);
    try testing.expectEqualStrings("hi", (try unpadded.getBin(arena, "k-bin")).?);
    try testing.expect((try padded.getBin(arena, "other-bin")) == null);
}

test "isReservedName blocks pseudo, grpc-*, and transport headers" {
    try testing.expect(isReservedName(":path"));
    try testing.expect(isReservedName("grpc-timeout"));
    try testing.expect(isReservedName("content-type"));
    try testing.expect(isReservedName("te"));
    try testing.expect(isReservedName("user-agent"));
    try testing.expect(isReservedName(""));
    try testing.expect(!isReservedName("x-custom"));
    try testing.expect(!isReservedName("authorization"));
}

test "encodeTimeout picks the smallest unit that fits, rounding up" {
    var buf: [9]u8 = undefined;
    try testing.expectEqualStrings("100n", encodeTimeout(100, &buf));
    try testing.expectEqualStrings("1000000u", encodeTimeout(std.time.ns_per_s, &buf));
    try testing.expectEqualStrings("300000m", encodeTimeout(5 * std.time.ns_per_min, &buf));
    // 向上取整:1500ns 不能编成 1u(会缩短 deadline)
    try testing.expectEqualStrings("2u", encodeTimeout(1_500, &buf));
    try testing.expectEqualStrings("100000m", encodeTimeout(100_000_000_000, &buf));
}
```

- [ ] **Step 2: lib.zig 挂模块,跑测试确认失败**

lib.zig 加:

```zig
const metadata_mod = @import("metadata.zig");
pub const Metadata = metadata_mod.Metadata;
```

test 块加 `_ = metadata_mod;`

Run: `zig build test`
Expected: FAIL(编译错误)

- [ ] **Step 3: 实现 src/metadata.zig**

```zig
//! gRPC metadata: user headers on requests, response headers/trailers.
//! Handles the `-bin` base64 convention and `grpc-timeout` encoding.

const std = @import("std");

pub const Metadata = struct {
    entries: []const Entry = &.{},

    pub const Entry = struct { name: []const u8, value: []const u8 };

    /// Case-insensitive lookup; returns the first match. Headers arrive
    /// lowercased from HPACK but callers may query in any case.
    pub fn get(self: Metadata, name: []const u8) ?[]const u8 {
        for (self.entries) |e| {
            if (std.ascii.eqlIgnoreCase(e.name, name)) return e.value;
        }
        return null;
    }

    /// Looks up a binary (`-bin`) entry and base64-decodes it into `arena`.
    /// Accepts padded and unpadded values — the spec requires tolerating both.
    pub fn getBin(self: Metadata, arena: std.mem.Allocator, name: []const u8) !?[]u8 {
        std.debug.assert(std.mem.endsWith(u8, name, "-bin"));
        const v = self.get(name) orelse return null;
        const trimmed = std.mem.trimRight(u8, v, "=");
        const dec = std.base64.standard_no_pad.Decoder;
        const n = dec.calcSizeForSlice(trimmed) catch return error.InvalidBase64;
        const out = try arena.alloc(u8, n);
        dec.decode(out, trimmed) catch return error.InvalidBase64;
        return out;
    }
};

/// Header names the client manages itself; user metadata may not use them.
pub fn isReservedName(name: []const u8) bool {
    if (name.len == 0) return true;
    if (name[0] == ':') return true;
    if (std.ascii.startsWithIgnoreCase(name, "grpc-")) return true;
    return std.ascii.eqlIgnoreCase(name, "content-type") or
        std.ascii.eqlIgnoreCase(name, "te") or
        std.ascii.eqlIgnoreCase(name, "user-agent");
}

/// Encodes a deadline as a `grpc-timeout` value: at most 8 digits plus a
/// unit, smallest unit that fits, rounding up so a deadline never shortens.
pub fn encodeTimeout(ns: u64, buf: *[9]u8) []const u8 {
    const max: u64 = 99_999_999;
    const units = [_]struct { div: u64, unit: u8 }{
        .{ .div = 1, .unit = 'n' },
        .{ .div = std.time.ns_per_us, .unit = 'u' },
        .{ .div = std.time.ns_per_ms, .unit = 'm' },
        .{ .div = std.time.ns_per_s, .unit = 'S' },
        .{ .div = std.time.ns_per_min, .unit = 'M' },
        .{ .div = std.time.ns_per_hour, .unit = 'H' },
    };
    for (units) |u| {
        const v = std.math.divCeil(u64, ns, u.div) catch unreachable;
        if (v <= max) return std.fmt.bufPrint(buf, "{d}{c}", .{ v, u.unit }) catch unreachable;
    }
    // Over 99999999 hours (~11 millennia): clamp.
    @memcpy(buf, "99999999H");
    return buf;
}
```

- [ ] **Step 4: 跑测试通过**

Run: `zig build test`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/metadata.zig src/lib.zig
git commit -m "feat: metadata with -bin base64 and grpc-timeout encoding"
```

---

### Task 4: frame.zig — 消息帧编解码

**Files:**
- Create: `src/frame.zig`
- Modify: `src/lib.zig`

**Interfaces:**
- Produces: `prefix_len = 5`、`encodePrefix(msg_len: u32) [5]u8`、`Assembler.init(gpa, max_message_size)`、`Assembler.feed(bytes) !void`、`Assembler.next(arena) !?[]u8`(错误:`MessageTooLarge`/`CompressedUnsupported`/`MalformedFrame`)、`Assembler.hasPartial() bool`、`Assembler.deinit()`

- [ ] **Step 1: 写失败测试**(src/frame.zig 末尾)

```zig
const testing = std.testing;

fn feedMsg(a: *Assembler, payload: []const u8) !void {
    const p = encodePrefix(@intCast(payload.len));
    try a.feed(&p);
    try a.feed(payload);
}

test "single message round-trips" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    var a = Assembler.init(testing.allocator, 1024);
    defer a.deinit();
    try feedMsg(&a, "hello");
    try testing.expectEqualStrings("hello", (try a.next(arena_state.allocator())).?);
    try testing.expect((try a.next(arena_state.allocator())) == null);
    try testing.expect(!a.hasPartial());
}

test "message split byte-by-byte across feeds reassembles" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    var a = Assembler.init(testing.allocator, 1024);
    defer a.deinit();
    const p = encodePrefix(3);
    const wire = p ++ "abc".*;
    for (wire) |b| {
        try a.feed(&.{b});
    }
    try testing.expectEqualStrings("abc", (try a.next(arena_state.allocator())).?);
}

test "two messages in one feed, empty message allowed" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    var a = Assembler.init(testing.allocator, 1024);
    defer a.deinit();
    try feedMsg(&a, "");
    try feedMsg(&a, "x");
    try testing.expectEqualStrings("", (try a.next(arena_state.allocator())).?);
    try testing.expectEqualStrings("x", (try a.next(arena_state.allocator())).?);
    try testing.expect((try a.next(arena_state.allocator())) == null);
}

test "oversized message rejected before payload arrives" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    var a = Assembler.init(testing.allocator, 4);
    defer a.deinit();
    const p = encodePrefix(5);
    try a.feed(&p);
    try testing.expectError(error.MessageTooLarge, a.next(arena_state.allocator()));
}

test "compressed flag and garbage flag rejected" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    var a = Assembler.init(testing.allocator, 1024);
    defer a.deinit();
    try a.feed(&.{ 1, 0, 0, 0, 0 });
    try testing.expectError(error.CompressedUnsupported, a.next(arena_state.allocator()));
    var b = Assembler.init(testing.allocator, 1024);
    defer b.deinit();
    try b.feed(&.{ 9, 0, 0, 0, 0 });
    try testing.expectError(error.MalformedFrame, b.next(arena_state.allocator()));
}

test "hasPartial reports a truncated message" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    var a = Assembler.init(testing.allocator, 1024);
    defer a.deinit();
    const p = encodePrefix(4);
    try a.feed(&p);
    try a.feed("ab"); // 4 声明,只到 2
    try testing.expect((try a.next(arena_state.allocator())) == null);
    try testing.expect(a.hasPartial());
}
```

- [ ] **Step 2: lib.zig 挂模块,跑测试确认失败**

lib.zig 加 `pub const frame = @import("frame.zig");`,test 块加 `_ = frame;`

Run: `zig build test`
Expected: FAIL

- [ ] **Step 3: 实现 src/frame.zig**

```zig
//! gRPC length-prefixed message framing: 1-byte compressed flag + 4-byte
//! big-endian length + payload, reassembled across HTTP/2 DATA boundaries.

const std = @import("std");

pub const prefix_len = 5;

pub const Error = error{ MessageTooLarge, CompressedUnsupported, MalformedFrame };

pub fn encodePrefix(msg_len: u32) [prefix_len]u8 {
    var p: [prefix_len]u8 = undefined;
    p[0] = 0; // v1 never compresses
    std.mem.writeInt(u32, p[1..prefix_len], msg_len, .big);
    return p;
}

/// Accumulates inbound DATA payloads and yields complete messages. Consumed
/// bytes are tracked by a read cursor; the buffer is reclaimed whenever it
/// fully drains (mirrors zig-http2's queue pattern), so it stays bounded.
pub const Assembler = struct {
    gpa: std.mem.Allocator,
    max_message_size: u32,
    buf: std.ArrayList(u8) = .empty,
    head: usize = 0,

    pub fn init(gpa: std.mem.Allocator, max_message_size: u32) Assembler {
        return .{ .gpa = gpa, .max_message_size = max_message_size };
    }

    pub fn deinit(self: *Assembler) void {
        self.buf.deinit(self.gpa);
    }

    pub fn feed(self: *Assembler, bytes: []const u8) !void {
        try self.buf.appendSlice(self.gpa, bytes);
    }

    /// Returns the next complete message (copied into `arena`), or null if
    /// more bytes are needed.
    pub fn next(self: *Assembler, arena: std.mem.Allocator) (Error || std.mem.Allocator.Error)!?[]u8 {
        const avail = self.buf.items[self.head..];
        if (avail.len < prefix_len) return null;
        const flag = avail[0];
        if (flag > 1) return error.MalformedFrame;
        if (flag == 1) return error.CompressedUnsupported;
        const len = std.mem.readInt(u32, avail[1..prefix_len], .big);
        if (len > self.max_message_size) return error.MessageTooLarge;
        if (avail.len < prefix_len + len) return null;
        const msg = try arena.dupe(u8, avail[prefix_len .. prefix_len + len]);
        self.head += prefix_len + len;
        if (self.head == self.buf.items.len) {
            self.buf.clearRetainingCapacity();
            self.head = 0;
        }
        return msg;
    }

    /// True when leftover bytes do not form a complete message — a protocol
    /// error if the stream has already ended.
    pub fn hasPartial(self: *const Assembler) bool {
        return self.buf.items.len > self.head;
    }
};
```

- [ ] **Step 4: 跑测试通过**

Run: `zig build test`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/frame.zig src/lib.zig
git commit -m "feat: gRPC message framing with cross-DATA reassembly"
```

---

### Task 5: codec.zig — Method 定义与默认编解码

**Files:**
- Create: `src/codec.zig`
- Modify: `src/lib.zig`

**Interfaces:**
- Produces: `Method(comptime Req, comptime Res) type`,实例字段 `path: []const u8`、`encode_req: *const fn (Allocator, Req) anyerror![]u8`(默认 duck-type 到 `Req.encode(self, gpa) ![]u8`)、`decode_res: *const fn (Allocator, []const u8) anyerror!Res`(默认 duck-type 到 `Res.decode(arena, bytes) !Res`);decls `Req`/`Res`
- 消费方式:`const M = grpc.Method(A, B){ .path = "/pkg.Svc/M" };` 作为 `comptime M: anytype` 传参;`@TypeOf(M).Req` 取类型

- [ ] **Step 1: 写失败测试**(src/codec.zig 末尾)

```zig
const testing = std.testing;

const TestMsg = struct {
    text: []const u8 = "",

    pub fn encode(self: TestMsg, gpa: std.mem.Allocator) ![]u8 {
        return gpa.dupe(u8, self.text);
    }

    pub fn decode(arena: std.mem.Allocator, bytes: []const u8) !TestMsg {
        return .{ .text = try arena.dupe(u8, bytes) };
    }
};

test "default codec duck-types onto encode/decode" {
    const M = Method(TestMsg, TestMsg){ .path = "/test.Svc/Echo" };
    const bytes = try M.encode_req(testing.allocator, .{ .text = "ping" });
    defer testing.allocator.free(bytes);
    try testing.expectEqualStrings("ping", bytes);

    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const msg = try M.decode_res(arena_state.allocator(), "pong");
    try testing.expectEqualStrings("pong", msg.text);
    try testing.expectEqualStrings("/test.Svc/Echo", M.path);
}

test "codec functions are overridable per method" {
    const upper = struct {
        fn enc(gpa: std.mem.Allocator, msg: TestMsg) anyerror![]u8 {
            const out = try gpa.dupe(u8, msg.text);
            for (out) |*c| c.* = std.ascii.toUpper(c.*);
            return out;
        }
    }.enc;
    const M = Method(TestMsg, TestMsg){ .path = "/test.Svc/Echo", .encode_req = &upper };
    const bytes = try M.encode_req(testing.allocator, .{ .text = "up" });
    defer testing.allocator.free(bytes);
    try testing.expectEqualStrings("UP", bytes);
}
```

- [ ] **Step 2: lib.zig 挂模块,跑测试确认失败**

lib.zig 加:

```zig
const codec_mod = @import("codec.zig");
pub const Method = codec_mod.Method;
```

test 块加 `_ = codec_mod;`

Run: `zig build test`
Expected: FAIL

- [ ] **Step 3: 实现 src/codec.zig**

```zig
//! Message encode/decode contract. The default codec duck-types onto the
//! message type: `encode(self, gpa) ![]u8` and `decode(arena, bytes) !T`
//! (hand-written structs and zig-protobuf messages both fit; anything else
//! overrides the function fields — zero runtime cost either way).

const std = @import("std");

pub fn Method(comptime RequestT: type, comptime ResponseT: type) type {
    return struct {
        path: []const u8, // "/pkg.Service/Method"
        encode_req: *const fn (std.mem.Allocator, RequestT) anyerror![]u8 = defaultEncode(RequestT),
        decode_res: *const fn (std.mem.Allocator, []const u8) anyerror!ResponseT = defaultDecode(ResponseT),

        pub const Req = RequestT;
        pub const Res = ResponseT;
    };
}

fn defaultEncode(comptime T: type) *const fn (std.mem.Allocator, T) anyerror![]u8 {
    return &struct {
        fn enc(gpa: std.mem.Allocator, msg: T) anyerror![]u8 {
            return msg.encode(gpa);
        }
    }.enc;
}

fn defaultDecode(comptime T: type) *const fn (std.mem.Allocator, []const u8) anyerror!T {
    return &struct {
        fn dec(arena: std.mem.Allocator, bytes: []const u8) anyerror!T {
            return T.decode(arena, bytes);
        }
    }.dec;
}
```

- [ ] **Step 4: 跑测试通过**

Run: `zig build test`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/codec.zig src/lib.zig
git commit -m "feat: comptime Method definition with duck-typed default codec"
```

---

### Task 6: Channel + startRaw + RawCall 发送路径(含回环测试设施)

**Files:**
- Create: `src/channel.zig`
- Create: `src/call.zig`(本任务只有发送路径;接收状态机在 Task 7)
- Create: `src/testutil.zig`
- Modify: `src/lib.zig`

**Interfaces:**
- Consumes: `metadata.isReservedName`、`metadata.encodeTimeout`、`frame.encodePrefix`/`frame.Assembler`、h2 API(见 Global Constraints)
- Produces:
  - `Channel.Options{authority, scheme="http", user_agent="grpc-zig/0.1", max_recv_message_size=4<<20, max_send_message_size=maxInt(u32)}`
  - `Channel.init(*Channel, io, gpa, r: *Io.Reader, w: *Io.Writer, opts) !void`(Channel init 后地址不得移动)
  - `Channel.deinit()`
  - `Channel.startRaw(path, CallOptions) !RawCall`
  - `CallOptions{metadata: []const Metadata.Entry = &.{}, timeout_ns: ?u64 = null, status_out: ?*Status = null}`
  - `RawCall.sendMessage([]const u8) !void`、`RawCall.closeSend() !void`、`RawCall.cancel() void`、`RawCall.deinit() void`
  - `testutil.Loopback`:`start(*Loopback, io, gpa, handler: h2.Handler, userdata: ?*anyopaque, chan_opts) !void` → 就绪的 `.chan`;`stop()` 收尾
- 注意:`h2.Client.openStream` 是否同步消费 `RequestHead.headers`(栈上 slice 是否安全)——实现前打开 `../zig-http2/src/client.zig:464` 确认 HEADERS 在 openStream 返回前已编码写出;若不是,改为在 startRaw 里 gpa 分配并在写出后释放,**并在完成报告中注明**

- [ ] **Step 1: 写 src/testutil.zig(回环设施,无测试)**

```zig
//! Test-only helper: an in-process zig-http2 server the client talks to over
//! a real TCP loopback socket (the same pattern zig-http2's own tests use).
//! Not referenced outside `test` blocks.

const std = @import("std");
const h2 = @import("zig_http2");
const channel_mod = @import("channel.zig");
const Io = std.Io;

pub const Loopback = struct {
    io: Io,
    gpa: std.mem.Allocator,
    listener: Io.net.Server,
    cstream: Io.net.Stream,
    crbuf: [8192]u8 = undefined,
    cwbuf: [8192]u8 = undefined,
    csr: Io.net.Stream.Reader = undefined,
    csw: Io.net.Stream.Writer = undefined,
    srv: h2.Server,
    thread: ?std.Thread = null,
    chan: channel_mod.Channel = undefined,

    /// Starts an h2 server thread running `handler`, connects a Channel to it.
    /// `self` must be at a stable address until `stop`.
    pub fn start(
        self: *Loopback,
        io: Io,
        gpa: std.mem.Allocator,
        handler: h2.Handler,
        userdata: ?*anyopaque,
        chan_opts: channel_mod.Channel.Options,
    ) !void {
        self.io = io;
        self.gpa = gpa;
        const addr0 = try Io.net.IpAddress.parse("127.0.0.1", 0);
        self.listener = try addr0.listen(io, .{ .mode = .stream, .reuse_address = true });
        errdefer self.listener.deinit(io);
        const port = self.listener.socket.address.ip4.port;
        const caddr = try Io.net.IpAddress.parse("127.0.0.1", port);
        self.cstream = try caddr.connect(io, .{ .mode = .stream });
        errdefer self.cstream.close(io);
        self.srv = .{ .io = io, .gpa = gpa, .handler = handler, .userdata = userdata };
        self.thread = try std.Thread.spawn(.{}, serve, .{self});
        self.csr = self.cstream.reader(io, &self.crbuf);
        self.csw = self.cstream.writer(io, &self.cwbuf);
        var opts = chan_opts;
        if (opts.authority.len == 0) opts.authority = "loopback";
        try self.chan.init(io, gpa, &self.csr.interface, &self.csw.interface, opts);
    }

    fn serve(self: *Loopback) void {
        var accepted = self.listener.accept(self.io) catch return;
        defer accepted.close(self.io);
        var rbuf: [8192]u8 = undefined;
        var wbuf: [8192]u8 = undefined;
        var sr = accepted.reader(self.io, &rbuf);
        var sw = accepted.writer(self.io, &wbuf);
        h2.serveConn(&self.srv, &sr.interface, &sw.interface, null, "http");
    }

    /// Tears down in LIFO order: channel (client GOAWAY/close) → client socket
    /// (server read loop sees EOF and exits) → join → listener.
    pub fn stop(self: *Loopback) void {
        self.chan.deinit();
        self.cstream.close(self.io);
        if (self.thread) |t| t.join();
        self.listener.deinit(self.io);
    }
};
```

注:若 `h2.Server` 没有 `userdata` 字段(核对 `../zig-http2/src/server.zig` 的 `Server` 定义),按实际字段名调整;`h2.Context.userdata` 的文档写明"set via Server.userdata"。

- [ ] **Step 2: 写失败测试**(src/channel.zig 末尾;先测发送路径:服务端捕获请求头 + 请求体)

```zig
const testing = std.testing;
const testutil = @import("testutil.zig");

const Captured = struct {
    ok: std.atomic.Value(bool) = .init(false),
    saw_te_trailers: bool = false,
    saw_content_type: bool = false,
    saw_user_agent: bool = false,
    saw_custom_md: bool = false,
    saw_timeout: bool = false,
    body: [64]u8 = undefined,
    body_len: usize = 0,
};

fn captureHandler(ctx: *h2.Context) anyerror!void {
    const cap: *Captured = @ptrCast(@alignCast(ctx.userdata.?));
    cap.saw_te_trailers = if (ctx.req.get("te")) |v| std.mem.eql(u8, v, "trailers") else false;
    cap.saw_content_type = if (ctx.req.get("content-type")) |v| std.mem.eql(u8, v, "application/grpc") else false;
    cap.saw_user_agent = if (ctx.req.get("user-agent")) |v| std.mem.startsWith(u8, v, "grpc-zig/") else false;
    cap.saw_custom_md = if (ctx.req.get("x-trace-id")) |v| std.mem.eql(u8, v, "t1") else false;
    cap.saw_timeout = ctx.req.get("grpc-timeout") != null;
    if (ctx.body_reader) |br| {
        while (true) {
            const n = try br.read(cap.body[cap.body_len..]);
            if (n == 0) break;
            cap.body_len += n;
        }
    }
    ctx.res.status(200);
    try ctx.res.header("content-type", "application/grpc");
    try ctx.res.trailer("grpc-status", "0");
    try ctx.res.finish();
    cap.ok.store(true, .release);
}

test "startRaw sends gRPC request headers and framed messages" {
    var cap: Captured = .{};
    var lb: testutil.Loopback = undefined;
    try lb.start(testing.io, testing.allocator, captureHandler, &cap, .{});

    var call = try lb.chan.startRaw("/test.Svc/Echo", .{
        .metadata = &.{.{ .name = "x-trace-id", .value = "t1" }},
        .timeout_ns = 3 * std.time.ns_per_s,
    });
    try call.sendMessage("hi");
    try call.closeSend();
    // 等 handler 跑完再断言(发送路径无接收 API,Task 7 才有)
    while (!cap.ok.load(.acquire)) {
        std.Io.sleep(testing.io, .{ .nanoseconds = 5 * std.time.ns_per_ms }, .awake) catch {};
    }
    call.deinit();
    lb.stop();

    try testing.expect(cap.saw_te_trailers);
    try testing.expect(cap.saw_content_type);
    try testing.expect(cap.saw_user_agent);
    try testing.expect(cap.saw_custom_md);
    try testing.expect(cap.saw_timeout);
    // 5 字节前缀(flag 0, len 2)+ "hi"
    try testing.expectEqual(@as(usize, 7), cap.body_len);
    try testing.expectEqualSlices(u8, &.{ 0, 0, 0, 0, 2 }, cap.body[0..5]);
    try testing.expectEqualStrings("hi", cap.body[5..7]);
}

test "reserved metadata names are rejected" {
    var cap: Captured = .{};
    var lb: testutil.Loopback = undefined;
    try lb.start(testing.io, testing.allocator, captureHandler, &cap, .{});
    defer lb.stop();
    try testing.expectError(error.ReservedMetadataName, lb.chan.startRaw("/x", .{
        .metadata = &.{.{ .name = "grpc-timeout", .value = "1S" }},
    }));
}
```

- [ ] **Step 3: 实现 src/channel.zig**

```zig
//! Channel: one HTTP/2 connection multiplexing concurrent gRPC calls.
//! Transport-agnostic core (`init` over any reader/writer); `connectTcp`
//! (Task 10) adds the h2c dial convenience.

const std = @import("std");
const h2 = @import("zig_http2");
const call_mod = @import("call.zig");
const metadata_mod = @import("metadata.zig");
const Io = std.Io;

pub const Channel = struct {
    io: Io,
    gpa: std.mem.Allocator,
    h2c: h2.Client,
    opts: Options,

    pub const Options = struct {
        authority: []const u8 = "",
        scheme: []const u8 = "http",
        user_agent: []const u8 = "grpc-zig/0.1",
        max_recv_message_size: u32 = 4 << 20, // grpc-go 默认
        max_send_message_size: u32 = std.math.maxInt(u32),
    };

    /// `self` must stay at a stable address until `deinit` (the h2 client's
    /// reader thread holds pointers into it). Caller owns `r`/`w`.
    pub fn init(self: *Channel, io: Io, gpa: std.mem.Allocator, r: *Io.Reader, w: *Io.Writer, opts: Options) !void {
        self.* = .{ .io = io, .gpa = gpa, .h2c = undefined, .opts = opts };
        try self.h2c.init(io, gpa, r, w);
    }

    pub fn deinit(self: *Channel) void {
        self.h2c.deinit();
    }

    /// Opens a bytes-level call: sends request HEADERS, returns the RawCall.
    /// The returned value owns per-call state; call `deinit` when done.
    pub fn startRaw(self: *Channel, path: []const u8, call_opts: call_mod.CallOptions) !call_mod.RawCall {
        var headers: std.ArrayList(h2.Header) = .empty;
        defer headers.deinit(self.gpa);
        try headers.append(self.gpa, .{ .name = "te", .value = "trailers" });
        try headers.append(self.gpa, .{ .name = "content-type", .value = "application/grpc" });
        try headers.append(self.gpa, .{ .name = "user-agent", .value = self.opts.user_agent });
        var tbuf: [9]u8 = undefined;
        if (call_opts.timeout_ns) |ns| {
            try headers.append(self.gpa, .{ .name = "grpc-timeout", .value = metadata_mod.encodeTimeout(ns, &tbuf) });
        }
        for (call_opts.metadata) |e| {
            if (metadata_mod.isReservedName(e.name)) return error.ReservedMetadataName;
            try headers.append(self.gpa, .{ .name = e.name, .value = e.value });
        }
        const s = try self.h2c.openStream(.{
            .method = "POST",
            .scheme = self.opts.scheme,
            .path = path,
            .authority = self.opts.authority,
            .headers = headers.items,
        }, false);
        return call_mod.RawCall.init(self, s);
    }
};
```

- [ ] **Step 4: 实现 src/call.zig(发送路径)**

```zig
//! Per-call state. RawCall drives one h2 stream through the gRPC call
//! lifecycle; Call(M) (Task 9) is the typed wrapper on top.

const std = @import("std");
const h2 = @import("zig_http2");
const status_mod = @import("status.zig");
const metadata_mod = @import("metadata.zig");
const frame = @import("frame.zig");
const channel_mod = @import("channel.zig");

pub const Status = status_mod.Status;
pub const Metadata = metadata_mod.Metadata;

pub const CallOptions = struct {
    metadata: []const Metadata.Entry = &.{},
    /// Encoded as `grpc-timeout`; enforced by the SERVER. v1 has no local
    /// deadline (a blocked recv cannot be interrupted — see the spec).
    timeout_ns: ?u64 = null,
    /// For `Channel.unary` only: receives the final Status on non-OK.
    status_out: ?*Status = null,
};

pub const RawCall = struct {
    chan: *channel_mod.Channel,
    stream: *h2.Stream,
    assembler: frame.Assembler,
    /// Owns response headers/trailers/status message until `deinit`.
    arena_state: std.heap.ArenaAllocator,
    state: State = .awaiting_headers,
    resp_headers: ?Metadata = null,
    resp_trailers: Metadata = .{},
    stat: ?Status = null,
    send_closed: bool = false,

    const State = enum { awaiting_headers, open, done };

    pub fn init(chan: *channel_mod.Channel, stream: *h2.Stream) RawCall {
        return .{
            .chan = chan,
            .stream = stream,
            .assembler = frame.Assembler.init(chan.gpa, chan.opts.max_recv_message_size),
            .arena_state = std.heap.ArenaAllocator.init(chan.gpa),
        };
    }

    /// Frames `msg` (5-byte prefix) and sends it as flow-controlled DATA.
    /// Blocks while the send window is empty. One sender thread per call.
    pub fn sendMessage(self: *RawCall, msg: []const u8) !void {
        if (msg.len > self.chan.opts.max_send_message_size) return error.MessageTooLarge;
        const buf = try self.chan.gpa.alloc(u8, frame.prefix_len + msg.len);
        defer self.chan.gpa.free(buf);
        buf[0..frame.prefix_len].* = frame.encodePrefix(@intCast(msg.len));
        @memcpy(buf[frame.prefix_len..], msg);
        try self.stream.send(buf, false);
    }

    /// Half-closes the request direction (empty DATA + END_STREAM).
    pub fn closeSend(self: *RawCall) !void {
        if (self.send_closed) return;
        self.send_closed = true;
        try self.stream.send("", true);
    }

    /// Tells the server to cancel (RST_STREAM CANCEL) and releases the
    /// stream's concurrency slot. Does NOT interrupt a concurrently blocked
    /// recv on another thread — see the spec's threading section.
    pub fn cancel(self: *RawCall) void {
        self.stream.cancel() catch {};
        if (self.stat == null) self.stat = .{ .code = .cancelled, .message = "cancelled by client" };
        self.state = .done;
    }

    /// Releases the call. Must not race a concurrent sendMessage/recvMessage.
    pub fn deinit(self: *RawCall) void {
        self.stream.close();
        self.assembler.deinit();
        self.arena_state.deinit();
    }
};
```

- [ ] **Step 5: lib.zig 挂模块**

lib.zig 加:

```zig
const call_mod = @import("call.zig");
const channel_mod = @import("channel.zig");
pub const CallOptions = call_mod.CallOptions;
pub const RawCall = call_mod.RawCall;
pub const Channel = channel_mod.Channel;
```

test 块加 `_ = call_mod; _ = channel_mod;`
channel.zig 测试文件顶部需要 `const h2 = @import("zig_http2");`(handler 签名用)。

- [ ] **Step 6: 跑测试通过**

Run: `zig build test`
Expected: PASS。若 `Io.net`/`Server.userdata`/`sleep` 等名称对不上,以 `../zig-http2/src/client.zig:1062`(回环模式)与 `src/example.zig` 的实际用法为准修正 testutil,不改语义。

- [ ] **Step 7: Commit**

```bash
git add src/channel.zig src/call.zig src/testutil.zig src/lib.zig
git commit -m "feat: Channel with startRaw, RawCall send path, loopback test harness"
```

---

### Task 7: RawCall 接收状态机(正常路径)

**Files:**
- Modify: `src/call.zig`
- Test: `src/call.zig`(test 块)

**Interfaces:**
- Consumes: Task 6 的 RawCall 字段(`state`/`resp_headers`/`resp_trailers`/`stat`/`assembler`/`arena_state`)、`status_mod.codeFromGrpcStatus`/`codeFromHttpStatus`/`codeFromH2Error`/`percentDecode`、`h2.Stream.readEvent`
- Produces:
  - `RawCall.recvMessage(arena) !?[]u8` — null = 消息流结束(trailers 已达)
  - `RawCall.header() !Metadata` — 阻塞至响应 HEADERS;Trailers-Only 时返回空。**与 spec 的偏差**:不收 arena 参数,返回值由 call 内部 arena 持有(有效期至 deinit)——更简单且无悬垂;完成报告中注明
  - `RawCall.finish() !Status` — 排空剩余消息直到 trailers,返回最终状态
  - `RawCall.trailers() Metadata`

- [ ] **Step 1: 写失败测试**(src/call.zig 末尾)

```zig
const testing = std.testing;
const testutil = @import("testutil.zig");

// 手工写 gRPC 消息帧(服务端侧)。prefix 和 payload 分两次 write —— 每次
// write 是独立 DATA 帧,顺带覆盖客户端的跨帧重组。
fn writeMsgFrames(res: anytype, payload: []const u8) !void {
    var prefix: [5]u8 = .{ 0, 0, 0, 0, 0 };
    std.mem.writeInt(u32, prefix[1..5], @intCast(payload.len), .big);
    try res.write(&prefix);
    try res.write(payload);
}

fn readAllBody(ctx: *h2.Context, buf: []u8) !usize {
    var n: usize = 0;
    if (ctx.body_reader) |br| {
        while (true) {
            const r = try br.read(buf[n..]);
            if (r == 0) break;
            n += r;
        }
    }
    return n;
}

fn unaryEchoHandler(ctx: *h2.Context) anyerror!void {
    var buf: [256]u8 = undefined;
    const n = try readAllBody(ctx, &buf);
    ctx.res.status(200);
    try ctx.res.header("content-type", "application/grpc");
    try ctx.res.header("x-server", "loopback");
    try ctx.res.write(buf[0..n]); // 原样回显(已含 5 字节前缀)
    try ctx.res.trailer("grpc-status", "0");
    try ctx.res.trailer("x-trailer", "tv");
    try ctx.res.finish();
}

fn serverStream3Handler(ctx: *h2.Context) anyerror!void {
    var buf: [256]u8 = undefined;
    _ = try readAllBody(ctx, &buf);
    ctx.res.status(200);
    try ctx.res.header("content-type", "application/grpc");
    try writeMsgFrames(ctx.res, "m0");
    try writeMsgFrames(ctx.res, "m1");
    try writeMsgFrames(ctx.res, "m2");
    try ctx.res.trailer("grpc-status", "0");
    try ctx.res.finish();
}

fn notFoundHandler(ctx: *h2.Context) anyerror!void {
    var buf: [256]u8 = undefined;
    _ = try readAllBody(ctx, &buf);
    ctx.res.status(200);
    try ctx.res.header("content-type", "application/grpc");
    try ctx.res.trailer("grpc-status", "5");
    try ctx.res.trailer("grpc-message", "no%20such%20thing");
    try ctx.res.finish();
}

test "unary round-trip: message, response headers, trailers, OK status" {
    var lb: testutil.Loopback = undefined;
    try lb.start(testing.io, testing.allocator, unaryEchoHandler, null, .{});
    defer lb.stop();

    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    var call = try lb.chan.startRaw("/test.Svc/Echo", .{});
    defer call.deinit();
    try call.sendMessage("hello");
    try call.closeSend();

    const hdrs = try call.header();
    try testing.expectEqualStrings("loopback", hdrs.get("x-server").?);

    const msg = (try call.recvMessage(arena)).?;
    try testing.expectEqualStrings("hello", msg);
    try testing.expect((try call.recvMessage(arena)) == null);

    const st = try call.finish();
    try testing.expect(st.isOk());
    try testing.expectEqualStrings("tv", call.trailers().get("x-trailer").?);
}

test "server streaming: three messages split across DATA frames" {
    var lb: testutil.Loopback = undefined;
    try lb.start(testing.io, testing.allocator, serverStream3Handler, null, .{});
    defer lb.stop();

    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    var call = try lb.chan.startRaw("/test.Svc/Stream", .{});
    defer call.deinit();
    try call.sendMessage("req");
    try call.closeSend();

    var i: usize = 0;
    while (try call.recvMessage(arena)) |m| : (i += 1) {
        var expect_buf: [2]u8 = .{ 'm', '0' + @as(u8, @intCast(i)) };
        try testing.expectEqualStrings(&expect_buf, m);
    }
    try testing.expectEqual(@as(usize, 3), i);
    try testing.expect((try call.finish()).isOk());
}

test "non-OK status with percent-decoded message" {
    var lb: testutil.Loopback = undefined;
    try lb.start(testing.io, testing.allocator, notFoundHandler, null, .{});
    defer lb.stop();

    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();

    var call = try lb.chan.startRaw("/test.Svc/Nope", .{});
    defer call.deinit();
    try call.sendMessage("x");
    try call.closeSend();
    try testing.expect((try call.recvMessage(arena_state.allocator())) == null);
    const st = try call.finish();
    try testing.expectEqual(status_mod.Code.not_found, st.code);
    try testing.expectEqualStrings("no such thing", st.message);
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `zig build test`
Expected: FAIL(`recvMessage`/`header`/`finish`/`trailers` 未定义)

- [ ] **Step 3: 实现接收状态机**(加入 RawCall;`fn` 顺序随意,代码完整如下)

```zig
    /// Reads the next response message into `arena`. Returns null once the
    /// server has finished (trailers received) — then `finish()` has the
    /// status. Transport faults are Zig errors; `stat` still carries the
    /// mapped gRPC status afterwards. One receiver thread per call.
    pub fn recvMessage(self: *RawCall, arena: std.mem.Allocator) !?[]u8 {
        while (true) {
            const maybe = self.assembler.next(arena) catch |e| {
                if (self.stat == null) self.stat = switch (e) {
                    error.MessageTooLarge => .{ .code = .resource_exhausted, .message = "message exceeds max_recv_message_size" },
                    error.CompressedUnsupported => .{ .code = .internal, .message = "compressed message but compression is unsupported" },
                    error.MalformedFrame => .{ .code = .internal, .message = "malformed message frame" },
                    else => .{ .code = .internal, .message = "receive failure" },
                };
                self.abandon();
                return e;
            };
            if (maybe) |m| return m;
            if (self.state == .done) {
                if (self.assembler.hasPartial() and (self.stat == null or self.stat.?.isOk())) {
                    self.stat = .{ .code = .internal, .message = "stream ended mid-message" };
                }
                return null;
            }
            try self.step();
        }
    }

    /// Blocks until the response HEADERS arrive; empty for Trailers-Only.
    /// The returned Metadata is owned by the call (valid until deinit).
    pub fn header(self: *RawCall) !Metadata {
        while (self.resp_headers == null and self.state == .awaiting_headers) try self.step();
        return self.resp_headers orelse .{};
    }

    /// Drains any remaining messages, then returns the final status. Intended
    /// after recvMessage returned null (for streams) or directly (unary
    /// convenience). NOTE: blocks until the server ends the stream.
    pub fn finish(self: *RawCall) !Status {
        var scratch = std.heap.ArenaAllocator.init(self.chan.gpa);
        defer scratch.deinit();
        while (self.state != .done) {
            _ = self.recvMessage(scratch.allocator()) catch break;
            _ = scratch.reset(.retain_capacity);
        }
        return self.stat orelse .{ .code = .internal, .message = "call ended without status" };
    }

    /// Trailer metadata (excluding grpc-status/grpc-message); valid after
    /// finish()/recvMessage()==null, owned by the call until deinit.
    pub fn trailers(self: *const RawCall) Metadata {
        return self.resp_trailers;
    }

    /// Consumes one h2 event and advances the call state machine.
    fn step(self: *RawCall) !void {
        var scratch = std.heap.ArenaAllocator.init(self.chan.gpa);
        defer scratch.deinit();
        const ev = self.stream.readEvent(scratch.allocator()) catch |e| switch (e) {
            error.EndOfStream => {
                if (self.stat == null) self.stat = .{ .code = .internal, .message = "stream ended without grpc-status" };
                self.state = .done;
                return;
            },
            error.ConnectionClosed => {
                if (self.stat == null) self.stat = .{ .code = .unavailable, .message = "connection closed" };
                self.state = .done;
                return e;
            },
            else => return e,
        };
        switch (ev) {
            .headers => |hd| try self.onHeaders(hd.headers, hd.end_stream),
            .data => |d| {
                try self.assembler.feed(d.payload);
                if (d.end_stream) {
                    // gRPC 响应必须以 trailers 结束;DATA+END_STREAM 是违例。
                    if (self.stat == null) self.stat = .{ .code = .internal, .message = "stream ended without trailers" };
                    self.state = .done;
                }
            },
            .rst => |r| {
                if (self.stat == null) self.stat = .{
                    .code = status_mod.codeFromH2Error(r.code),
                    .message = "stream reset by server",
                };
                self.state = .done;
            },
            .goaway => {
                if (self.stat == null) self.stat = .{
                    .code = .unavailable,
                    .message = "stream refused by server GOAWAY (safe to retry)",
                };
                self.state = .done;
            },
        }
    }

    fn onHeaders(self: *RawCall, hs: []const h2.Header, end_stream: bool) !void {
        const arena = self.arena_state.allocator();
        switch (self.state) {
            .awaiting_headers => {
                if (end_stream) {
                    // Trailers-Only:整个响应就这一个 HEADERS 块。
                    self.stat = try parseTrailerStatus(arena, hs, true);
                    self.resp_trailers = try dupEntries(arena, hs);
                    self.state = .done;
                    return;
                }
                const http_status = findHeader(hs, ":status") orelse "";
                if (!std.mem.eql(u8, http_status, "200")) {
                    const parsed = std.fmt.parseInt(u16, http_status, 10) catch 0;
                    self.stat = .{
                        .code = status_mod.codeFromHttpStatus(parsed),
                        .message = try std.fmt.allocPrint(arena, "unexpected HTTP status \"{s}\"", .{http_status}),
                    };
                    self.abandon();
                    return;
                }
                const ct = findHeader(hs, "content-type") orelse "";
                if (!std.mem.startsWith(u8, ct, "application/grpc")) {
                    self.stat = .{
                        .code = .internal,
                        .message = try std.fmt.allocPrint(arena, "bad content-type \"{s}\"", .{ct}),
                    };
                    self.abandon();
                    return;
                }
                if (findHeader(hs, "grpc-encoding")) |enc| {
                    if (!std.mem.eql(u8, enc, "identity")) {
                        self.stat = .{
                            .code = .internal,
                            .message = try std.fmt.allocPrint(arena, "unsupported grpc-encoding \"{s}\"", .{enc}),
                        };
                        self.abandon();
                        return;
                    }
                }
                self.resp_headers = try dupEntries(arena, hs);
                self.state = .open;
            },
            .open => {
                self.stat = try parseTrailerStatus(arena, hs, false);
                self.resp_trailers = try dupEntries(arena, hs);
                self.state = .done;
            },
            .done => {},
        }
    }

    /// Cancels the underlying stream and ends the call (used on protocol
    /// violations so the server stops sending).
    fn abandon(self: *RawCall) void {
        self.stream.cancel() catch {};
        self.state = .done;
    }
```

以及文件级私有函数:

```zig
fn findHeader(hs: []const h2.Header, name: []const u8) ?[]const u8 {
    for (hs) |h| {
        if (std.ascii.eqlIgnoreCase(h.name, name)) return h.value;
    }
    return null;
}

/// Copies non-pseudo headers (minus grpc-status/grpc-message) into `arena`.
fn dupEntries(arena: std.mem.Allocator, hs: []const h2.Header) !Metadata {
    var list: std.ArrayList(Metadata.Entry) = .empty;
    for (hs) |h| {
        if (h.name.len == 0 or h.name[0] == ':') continue;
        if (std.ascii.eqlIgnoreCase(h.name, "grpc-status")) continue;
        if (std.ascii.eqlIgnoreCase(h.name, "grpc-message")) continue;
        try list.append(arena, .{
            .name = try arena.dupe(u8, h.name),
            .value = try arena.dupe(u8, h.value),
        });
    }
    return .{ .entries = list.items };
}

fn parseTrailerStatus(arena: std.mem.Allocator, hs: []const h2.Header, trailers_only: bool) !Status {
    const msg: []const u8 = if (findHeader(hs, "grpc-message")) |m|
        try status_mod.percentDecode(arena, m)
    else
        "";
    if (findHeader(hs, "grpc-status")) |gs| {
        return .{ .code = status_mod.codeFromGrpcStatus(gs), .message = msg };
    }
    if (trailers_only) {
        const hsv = findHeader(hs, ":status") orelse "";
        const parsed = std.fmt.parseInt(u16, hsv, 10) catch 0;
        return .{ .code = status_mod.codeFromHttpStatus(parsed), .message = "missing grpc-status" };
    }
    return .{ .code = .internal, .message = "missing grpc-status in trailers" };
}
```

- [ ] **Step 4: 跑测试通过**

Run: `zig build test`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/call.zig
git commit -m "feat: RawCall receive state machine (headers/messages/trailers/status)"
```

---

### Task 8: RawCall 异常路径(raw 帧服务端)

**Files:**
- Modify: `src/testutil.zig`(加 raw 帧服务端助手)
- Test: `src/call.zig`(test 块追加)

**Interfaces:**
- Consumes: `h2.proto`(`preface`/`FrameType`/`flag_ack`/`flag_end_stream`/`flag_end_headers`/`putHeader`/`parseHeader`/`ErrorCode`)、Task 7 的 recvMessage/finish
- Produces: `testutil.RawPeer` — 不经 h2.Server、直接读写 HTTP/2 帧的假服务端,用于 h2.Server 造不出的线上形态(真 Trailers-Only 单 HEADERS 帧、RST_STREAM、缺 grpc-status 等)

- [ ] **Step 1: 在 testutil.zig 追加 RawPeer**

```zig
/// A raw-frame HTTP/2 peer: does the h2 handshake then hands the test direct
/// frame-level read/write. For wire shapes h2.Server cannot produce (true
/// Trailers-Only, RST_STREAM injection, missing grpc-status).
pub const RawPeer = struct {
    io: Io,
    gpa: std.mem.Allocator,
    listener: Io.net.Server,
    cstream: Io.net.Stream,
    crbuf: [8192]u8 = undefined,
    cwbuf: [8192]u8 = undefined,
    csr: Io.net.Stream.Reader = undefined,
    csw: Io.net.Stream.Writer = undefined,
    accepted: Io.net.Stream = undefined,
    arbuf: [8192]u8 = undefined,
    awbuf: [8192]u8 = undefined,
    asr: Io.net.Stream.Reader = undefined,
    asw: Io.net.Stream.Writer = undefined,
    chan: channel_mod.Channel = undefined,

    /// Connects a Channel, then (on the test thread) consumes the client's
    /// preface + SETTINGS and answers SETTINGS + ack. After this returns the
    /// test drives the server side inline via readFrame/writeFrame.
    pub fn start(self: *RawPeer, io: Io, gpa: std.mem.Allocator) !void {
        self.io = io;
        self.gpa = gpa;
        const addr0 = try Io.net.IpAddress.parse("127.0.0.1", 0);
        self.listener = try addr0.listen(io, .{ .mode = .stream, .reuse_address = true });
        const port = self.listener.socket.address.ip4.port;
        const caddr = try Io.net.IpAddress.parse("127.0.0.1", port);
        self.cstream = try caddr.connect(io, .{ .mode = .stream });
        self.accepted = try self.listener.accept(io);
        self.csr = self.cstream.reader(io, &self.crbuf);
        self.csw = self.cstream.writer(io, &self.cwbuf);
        self.asr = self.accepted.reader(io, &self.arbuf);
        self.asw = self.accepted.writer(io, &self.awbuf);
        try self.chan.init(io, gpa, &self.csr.interface, &self.csw.interface, .{ .authority = "raw" });
        // 服务端握手:preface + 客户端 SETTINGS,回空 SETTINGS + ack
        var pf: [h2.proto.preface.len]u8 = undefined;
        try self.asr.interface.readSliceAll(&pf);
        var f = try self.readFrame();
        self.gpa.free(f.payload);
        try self.writeFrame(.settings, 0, 0, "");
        try self.writeFrame(.settings, h2.proto.flag_ack, 0, "");
    }

    pub const Frame = struct { hdr: h2.proto.ParsedHeader, payload: []u8 };

    pub fn readFrame(self: *RawPeer) !Frame {
        var hb: [9]u8 = undefined;
        try self.asr.interface.readSliceAll(&hb);
        const hdr = h2.proto.parseHeader(&hb);
        const payload = try self.gpa.alloc(u8, hdr.length);
        errdefer self.gpa.free(payload);
        if (payload.len > 0) try self.asr.interface.readSliceAll(payload);
        return .{ .hdr = hdr, .payload = payload };
    }

    /// Reads frames until one of type `t` on any stream; frees the others.
    pub fn readUntil(self: *RawPeer, t: h2.proto.FrameType) !Frame {
        while (true) {
            const f = try self.readFrame();
            if (f.hdr.ftype == t) return f;
            self.gpa.free(f.payload);
        }
    }

    pub fn writeFrame(self: *RawPeer, ftype: h2.proto.FrameType, flags: u8, sid: u31, payload: []const u8) !void {
        var hb: [9]u8 = undefined;
        h2.proto.putHeader(&hb, payload.len, ftype, flags, sid);
        try self.asw.interface.writeAll(&hb);
        if (payload.len > 0) try self.asw.interface.writeAll(payload);
        try self.asw.interface.flush();
    }

    pub fn writeRst(self: *RawPeer, sid: u31, code: h2.proto.ErrorCode) !void {
        var p: [4]u8 = undefined;
        std.mem.writeInt(u32, &p, @intFromEnum(code), .big);
        try self.writeFrame(.rst_stream, 0, sid, &p);
    }

    /// Hand-crafted HPACK block: `:status` via the static table, everything
    /// else as "literal without indexing, new name" (0x00 n v) — decodable by
    /// any compliant decoder with no encoder dependency.
    pub const HpackBlock = struct {
        buf: std.ArrayList(u8) = .empty,

        pub fn status200(self: *HpackBlock, gpa: std.mem.Allocator) !void {
            try self.buf.append(gpa, 0x88); // indexed: static 8 = :status 200
        }

        pub fn literal(self: *HpackBlock, gpa: std.mem.Allocator, name: []const u8, value: []const u8) !void {
            std.debug.assert(name.len < 127 and value.len < 127);
            try self.buf.append(gpa, 0x00);
            try self.buf.append(gpa, @intCast(name.len));
            try self.buf.appendSlice(gpa, name);
            try self.buf.append(gpa, @intCast(value.len));
            try self.buf.appendSlice(gpa, value);
        }

        pub fn deinit(self: *HpackBlock, gpa: std.mem.Allocator) void {
            self.buf.deinit(gpa);
        }
    };

    pub fn stop(self: *RawPeer) void {
        self.chan.deinit();
        self.cstream.close(self.io);
        self.accepted.close(self.io);
        self.listener.deinit(self.io);
    }
};
```

- [ ] **Step 2: 写失败测试**(src/call.zig test 块追加)

```zig
test "RST_STREAM(CANCEL) maps to cancelled status and StreamReset error" {
    var rp: testutil.RawPeer = undefined;
    try rp.start(testing.io, testing.allocator);
    defer rp.stop();

    var call = try rp.chan.startRaw("/test.Svc/X", .{});
    defer call.deinit();
    try call.closeSend();

    const hf = try rp.readUntil(.headers);
    testing.allocator.free(hf.payload);
    const sid = hf.hdr.sid;
    try rp.writeRst(sid, .cancel);

    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    // 事件序:.rst 事件 → state=done → recvMessage 返回 null(消息流终止)
    try testing.expect((try call.recvMessage(arena_state.allocator())) == null);
    const st = try call.finish();
    try testing.expectEqual(status_mod.Code.cancelled, st.code);
}

test "true Trailers-Only: single HEADERS with END_STREAM carries the status" {
    var rp: testutil.RawPeer = undefined;
    try rp.start(testing.io, testing.allocator);
    defer rp.stop();

    var call = try rp.chan.startRaw("/test.Svc/X", .{});
    defer call.deinit();
    try call.closeSend();

    const hf = try rp.readUntil(.headers);
    testing.allocator.free(hf.payload);
    const sid = hf.hdr.sid;

    var blk: testutil.RawPeer.HpackBlock = .{};
    defer blk.deinit(testing.allocator);
    try blk.status200(testing.allocator);
    try blk.literal(testing.allocator, "content-type", "application/grpc");
    try blk.literal(testing.allocator, "grpc-status", "12");
    try blk.literal(testing.allocator, "grpc-message", "unimplemented");
    try rp.writeFrame(.headers, h2.proto.flag_end_headers | h2.proto.flag_end_stream, sid, blk.buf.items);

    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    try testing.expect((try call.recvMessage(arena_state.allocator())) == null);
    const st = try call.finish();
    try testing.expectEqual(status_mod.Code.unimplemented, st.code);
    try testing.expectEqualStrings("unimplemented", st.message);
    // header() 在 Trailers-Only 下返回空 Metadata
    try testing.expectEqual(@as(usize, 0), (try call.header()).entries.len);
}

test "trailers missing grpc-status map to internal" {
    var rp: testutil.RawPeer = undefined;
    try rp.start(testing.io, testing.allocator);
    defer rp.stop();

    var call = try rp.chan.startRaw("/test.Svc/X", .{});
    defer call.deinit();
    try call.closeSend();

    const hf = try rp.readUntil(.headers);
    testing.allocator.free(hf.payload);
    const sid = hf.hdr.sid;

    // 初始 HEADERS(200 + grpc content-type,不带 END_STREAM)
    var blk: testutil.RawPeer.HpackBlock = .{};
    defer blk.deinit(testing.allocator);
    try blk.status200(testing.allocator);
    try blk.literal(testing.allocator, "content-type", "application/grpc");
    try rp.writeFrame(.headers, h2.proto.flag_end_headers, sid, blk.buf.items);
    // trailers 不带 grpc-status
    var tblk: testutil.RawPeer.HpackBlock = .{};
    defer tblk.deinit(testing.allocator);
    try tblk.literal(testing.allocator, "x-oops", "1");
    try rp.writeFrame(.headers, h2.proto.flag_end_headers | h2.proto.flag_end_stream, sid, tblk.buf.items);

    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    try testing.expect((try call.recvMessage(arena_state.allocator())) == null);
    try testing.expectEqual(status_mod.Code.internal, (try call.finish()).code);
}

test "non-grpc content-type maps to internal and cancels the stream" {
    var rp: testutil.RawPeer = undefined;
    try rp.start(testing.io, testing.allocator);
    defer rp.stop();

    var call = try rp.chan.startRaw("/test.Svc/X", .{});
    defer call.deinit();
    try call.closeSend();

    const hf = try rp.readUntil(.headers);
    testing.allocator.free(hf.payload);
    const sid = hf.hdr.sid;

    var blk: testutil.RawPeer.HpackBlock = .{};
    defer blk.deinit(testing.allocator);
    try blk.status200(testing.allocator);
    try blk.literal(testing.allocator, "content-type", "text/html");
    try rp.writeFrame(.headers, h2.proto.flag_end_headers, sid, blk.buf.items);

    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    try testing.expect((try call.recvMessage(arena_state.allocator())) == null);
    try testing.expectEqual(status_mod.Code.internal, (try call.finish()).code);
    // 客户端应发出 RST_STREAM(abandon)
    const rst = try rp.readUntil(.rst_stream);
    testing.allocator.free(rst.payload);
}

test "compressed-flag message errors and surfaces internal status" {
    var lb: testutil.Loopback = undefined;
    var lbh = struct {
        fn h(ctx: *h2.Context) anyerror!void {
            var buf: [64]u8 = undefined;
            _ = try readAllBody(ctx, &buf);
            ctx.res.status(200);
            try ctx.res.header("content-type", "application/grpc");
            try ctx.res.write(&.{ 1, 0, 0, 0, 0 }); // compressed flag = 1
            try ctx.res.trailer("grpc-status", "0");
            try ctx.res.finish();
        }
    };
    try lb.start(testing.io, testing.allocator, lbh.h, null, .{});
    defer lb.stop();

    var call = try lb.chan.startRaw("/test.Svc/X", .{});
    defer call.deinit();
    try call.sendMessage("x");
    try call.closeSend();
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    try testing.expectError(error.CompressedUnsupported, call.recvMessage(arena_state.allocator()));
    try testing.expectEqual(status_mod.Code.internal, (try call.finish()).code);
}
```

注:`.rst` 事件在 Task 7 的 step() 里已置 `state=.done`,recvMessage 对上层表现为 null + finish 出 status——与 spec"传输故障是 Zig error"的表述相比,**RST 归为"对端以 h2 错误码结束流"**,按状态而非 error 呈现更贴 gRPC 语义(grpc-go 亦然);completion report 中注明该细化。

- [ ] **Step 3: 跑测试确认失败 → 修实现直到通过**

Run: `zig build test`
Expected: 先 FAIL;若 `ErrorCode.cancel` 等枚举名对不上,以 `../zig-http2/src/proto.zig:30` 实际成员名为准。全部 PASS 后进入下一步。

- [ ] **Step 4: Commit**

```bash
git add src/testutil.zig src/call.zig
git commit -m "test: raw-frame peer; RST, trailers-only, missing-status, bad content-type paths"
```

---

### Task 9: 类型化层 — Call(M) / start / unary

**Files:**
- Modify: `src/call.zig`(加 `Call`)
- Modify: `src/channel.zig`(加 `start`/`unary`)
- Modify: `src/lib.zig`

**Interfaces:**
- Consumes: Task 5 `Method`(`M.path`/`M.encode_req`/`M.decode_res`/`@TypeOf(M).Req/.Res`)、Task 6/7 RawCall 全部
- Produces:
  - `Call(comptime M: anytype) type`:`send(Req) !void`、`recv(arena) !?Res`、`closeSend() !void`、`header() !Metadata`、`finish() !Status`、`trailers() Metadata`、`cancel() void`、`deinit() void`
  - `Channel.start(comptime M, CallOptions) !Call(M)`
  - `Channel.unary(comptime M, arena, req, CallOptions) !Res`——非 OK 返回 `error.RpcFailed`,`opts.status_out` 带出状态(message 已 dupe 进 arena);OK 但无响应消息 → `error.MissingResponse`

- [ ] **Step 1: 写失败测试**(src/call.zig test 块追加;`unaryEchoHandler`/`readAllBody` 复用 Task 7 的)

```zig
const TestMsg = struct {
    text: []const u8 = "",

    pub fn encode(self: TestMsg, gpa: std.mem.Allocator) ![]u8 {
        return gpa.dupe(u8, self.text);
    }

    pub fn decode(arena: std.mem.Allocator, bytes: []const u8) !TestMsg {
        return .{ .text = try arena.dupe(u8, bytes) };
    }
};
const codec_mod = @import("codec.zig");
const EchoM = codec_mod.Method(TestMsg, TestMsg){ .path = "/test.Svc/Echo" };

test "typed unary convenience round-trips" {
    var lb: testutil.Loopback = undefined;
    try lb.start(testing.io, testing.allocator, unaryEchoHandler, null, .{});
    defer lb.stop();

    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const reply = try lb.chan.unary(EchoM, arena_state.allocator(), .{ .text = "ping" }, .{});
    try testing.expectEqualStrings("ping", reply.text);
}

test "typed unary surfaces non-OK via error.RpcFailed and status_out" {
    var lb: testutil.Loopback = undefined;
    try lb.start(testing.io, testing.allocator, notFoundHandler, null, .{});
    defer lb.stop();

    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    var st: Status = undefined;
    try testing.expectError(error.RpcFailed, lb.chan.unary(
        EchoM,
        arena_state.allocator(),
        .{ .text = "x" },
        .{ .status_out = &st },
    ));
    try testing.expectEqual(status_mod.Code.not_found, st.code);
    try testing.expectEqualStrings("no such thing", st.message);
}

fn bidiEchoHandler(ctx: *h2.Context) anyerror!void {
    ctx.res.status(200);
    try ctx.res.header("content-type", "application/grpc");
    if (ctx.body_reader) |br| {
        var tmp: [512]u8 = undefined;
        while (true) {
            const n = try br.read(&tmp);
            if (n == 0) break;
            try ctx.res.write(tmp[0..n]); // 即读即回:全双工 ping-pong
        }
    }
    try ctx.res.trailer("grpc-status", "0");
    try ctx.res.finish();
}

test "typed bidi ping-pong on one stream" {
    var lb: testutil.Loopback = undefined;
    try lb.start(testing.io, testing.allocator, bidiEchoHandler, null, .{});
    defer lb.stop();

    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    var call = try lb.chan.start(EchoM, .{});
    defer call.deinit();

    try call.send(.{ .text = "one" });
    try testing.expectEqualStrings("one", (try call.recv(arena)).?.text);
    try call.send(.{ .text = "two" });
    try testing.expectEqualStrings("two", (try call.recv(arena)).?.text);
    try call.closeSend();
    try testing.expect((try call.recv(arena)) == null);
    try testing.expect((try call.finish()).isOk());
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `zig build test`
Expected: FAIL(`Call`/`start`/`unary` 未定义)

- [ ] **Step 3: 实现 Call(M)**(src/call.zig 追加)

```zig
/// Typed wrapper over RawCall for a comptime Method value. Same lifecycle
/// and threading rules as RawCall.
pub fn Call(comptime M: anytype) type {
    const Req = @TypeOf(M).Req;
    const Res = @TypeOf(M).Res;
    return struct {
        raw: RawCall,

        const Self = @This();

        pub fn send(self: *Self, msg: Req) !void {
            const bytes = try M.encode_req(self.raw.chan.gpa, msg);
            defer self.raw.chan.gpa.free(bytes);
            try self.raw.sendMessage(bytes);
        }

        pub fn recv(self: *Self, arena: std.mem.Allocator) !?Res {
            const bytes = (try self.raw.recvMessage(arena)) orelse return null;
            return try M.decode_res(arena, bytes);
        }

        pub fn closeSend(self: *Self) !void {
            return self.raw.closeSend();
        }

        pub fn header(self: *Self) !Metadata {
            return self.raw.header();
        }

        pub fn finish(self: *Self) !Status {
            return self.raw.finish();
        }

        pub fn trailers(self: *const Self) Metadata {
            return self.raw.trailers();
        }

        pub fn cancel(self: *Self) void {
            self.raw.cancel();
        }

        pub fn deinit(self: *Self) void {
            self.raw.deinit();
        }
    };
}
```

- [ ] **Step 4: 实现 Channel.start / Channel.unary**(src/channel.zig 追加)

```zig
    /// Opens a typed call for a comptime Method value.
    pub fn start(self: *Channel, comptime M: anytype, call_opts: call_mod.CallOptions) !call_mod.Call(M) {
        return .{ .raw = try self.startRaw(M.path, call_opts) };
    }

    /// One-shot unary RPC: send → half-close → receive one → status check.
    /// Non-OK becomes error.RpcFailed with the status (message duped into
    /// `arena`) written to `call_opts.status_out` when provided.
    pub fn unary(
        self: *Channel,
        comptime M: anytype,
        arena: std.mem.Allocator,
        req: @TypeOf(M).Req,
        call_opts: call_mod.CallOptions,
    ) !@TypeOf(M).Res {
        var c = try self.start(M, call_opts);
        defer c.deinit();
        try c.send(req);
        try c.closeSend();
        const res = try c.recv(arena);
        const st = try c.finish();
        if (call_opts.status_out) |out| {
            out.* = .{ .code = st.code, .message = try arena.dupe(u8, st.message) };
        }
        if (!st.isOk()) return error.RpcFailed;
        return res orelse error.MissingResponse;
    }
```

- [ ] **Step 5: lib.zig 导出 Call**

lib.zig 加 `pub const Call = call_mod.Call;`

- [ ] **Step 6: 跑测试通过**

Run: `zig build test`
Expected: PASS

- [ ] **Step 7: Commit**

```bash
git add src/call.zig src/channel.zig src/lib.zig
git commit -m "feat: typed Call(M), Channel.start and unary convenience"
```

---

### Task 10: connectTcp 便捷层

**Files:**
- Modify: `src/channel.zig`
- Test: `src/channel.zig`(test 块追加)

**Interfaces:**
- Consumes: `std.Io.net.IpAddress.parse/connect`、`std.Io.net.HostName.init/connect`(签名:`HostName.connect(hn, io, port, options) !Stream`)
- Produces: `Channel.connectTcp(*Channel, io, gpa, host, port, opts) !void`——拨 h2c 明文 TCP,Channel 拥有 socket 与缓冲;`authority` 未设时自动填 `host:port`;`deinit` 释放全部

- [ ] **Step 1: 写失败测试**(src/channel.zig test 块追加;handler 复用 Task 6 的 `captureHandler` 不合适,直接用 call.zig 里的 echo——为避免跨文件引用,本测试内联最小 handler)

```zig
fn tcpOkHandler(ctx: *h2.Context) anyerror!void {
    if (ctx.body_reader) |br| {
        var tmp: [64]u8 = undefined;
        while (true) {
            if (try br.read(&tmp) == 0) break;
        }
    }
    ctx.res.status(200);
    try ctx.res.header("content-type", "application/grpc");
    try ctx.res.trailer("grpc-status", "0");
    try ctx.res.finish();
}

const TcpSrv = struct {
    io: std.Io,
    listener: *std.Io.net.Server,
    srv: *h2.Server,

    fn run(self: *TcpSrv) void {
        var accepted = self.listener.accept(self.io) catch return;
        defer accepted.close(self.io);
        var rbuf: [8192]u8 = undefined;
        var wbuf: [8192]u8 = undefined;
        var sr = accepted.reader(self.io, &rbuf);
        var sw = accepted.writer(self.io, &wbuf);
        h2.serveConn(self.srv, &sr.interface, &sw.interface, null, "http");
    }
};

test "connectTcp dials h2c and completes a call" {
    const io = testing.io;
    const addr0 = try std.Io.net.IpAddress.parse("127.0.0.1", 0);
    var listener = try addr0.listen(io, .{ .mode = .stream, .reuse_address = true });
    defer listener.deinit(io);
    const port = listener.socket.address.ip4.port;

    var srv: h2.Server = .{ .io = io, .gpa = testing.allocator, .handler = tcpOkHandler };
    var tsrv: TcpSrv = .{ .io = io, .listener = &listener, .srv = &srv };
    const th = try std.Thread.spawn(.{}, TcpSrv.run, .{&tsrv});

    var chan: Channel = undefined;
    try chan.connectTcp(io, testing.allocator, "127.0.0.1", port, .{});

    var call = try chan.startRaw("/test.Svc/Ok", .{});
    try call.sendMessage("x");
    try call.closeSend();
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    try testing.expect((try call.recvMessage(arena_state.allocator())) == null);
    try testing.expect((try call.finish()).isOk());
    // authority 自动填充
    try testing.expect(std.mem.startsWith(u8, chan.opts.authority, "127.0.0.1:"));
    call.deinit();
    chan.deinit(); // 关闭 socket → 服务端读到 EOF → 线程退出
    th.join();
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `zig build test`
Expected: FAIL(`connectTcp` 未定义)

- [ ] **Step 3: 实现 connectTcp**(src/channel.zig:Channel 内追加字段与方法)

Channel 加字段:

```zig
    owned: ?*OwnedConn = null,
    authority_owned: bool = false,
```

Channel 内追加:

```zig
    /// Heap-pinned transport state for connectTcp: the reader/writer hold
    /// pointers into these buffers, so the block must never move.
    const OwnedConn = struct {
        stream: Io.net.Stream,
        rbuf: [8192]u8 = undefined,
        wbuf: [8192]u8 = undefined,
        sr: Io.net.Stream.Reader = undefined,
        sw: Io.net.Stream.Writer = undefined,
    };

    /// Dials plaintext h2c TCP (prior knowledge) and initializes the channel.
    /// `host` may be an IP literal or a hostname. The channel owns the socket.
    /// For TLS, wrap your own reader/writer and use `init` instead.
    pub fn connectTcp(self: *Channel, io: Io, gpa: std.mem.Allocator, host: []const u8, port: u16, opts: Options) !void {
        const oc = try gpa.create(OwnedConn);
        errdefer gpa.destroy(oc);
        oc.* = .{ .stream = undefined };
        if (Io.net.IpAddress.parse(host, port)) |a| {
            var addr = a;
            oc.stream = try addr.connect(io, .{ .mode = .stream });
        } else |_| {
            const hn = try Io.net.HostName.init(host);
            oc.stream = try hn.connect(io, port, .{ .mode = .stream });
        }
        errdefer oc.stream.close(io);
        oc.sr = oc.stream.reader(io, &oc.rbuf);
        oc.sw = oc.stream.writer(io, &oc.wbuf);

        var o = opts;
        var auth_owned = false;
        if (o.authority.len == 0) {
            o.authority = try std.fmt.allocPrint(gpa, "{s}:{d}", .{ host, port });
            auth_owned = true;
        }
        errdefer if (auth_owned) gpa.free(o.authority);

        try self.init(io, gpa, &oc.sr.interface, &oc.sw.interface, o);
        self.owned = oc;
        self.authority_owned = auth_owned;
    }
```

`deinit` 改为:

```zig
    pub fn deinit(self: *Channel) void {
        self.h2c.deinit();
        if (self.owned) |oc| {
            oc.stream.close(self.io);
            self.gpa.destroy(oc);
        }
        if (self.authority_owned) self.gpa.free(self.opts.authority);
    }
```

注意 `init` 的 `self.* = .{...}` 会清掉 `owned`/`authority_owned`,connectTcp 在 init 之后再赋值——保持这个顺序。

- [ ] **Step 4: 跑测试通过**

Run: `zig build test`
Expected: PASS。若 `IpAddress.connect` 的 options 结构对不上,以 `../zig-http2/src/client.zig:1069` 的用法(`.{ .mode = .stream }`)为准。

- [ ] **Step 5: Commit**

```bash
git add src/channel.zig
git commit -m "feat: connectTcp h2c dial convenience with owned transport"
```

---

### Task 11: server.zig 骨架

**Files:**
- Create: `src/server.zig`
- Modify: `src/lib.zig`

**Interfaces:**
- Consumes: `Status`、`Metadata`
- Produces(仅形态,spec 约定 server 细节后置):`ServerCall{request_metadata}`、`HandlerFn = *const fn (*ServerCall) anyerror!Status`、`Registry.register(gpa, path, HandlerFn) !void`(重复注册 → `error.DuplicateMethod`)、`Registry.lookup(path) ?HandlerFn`、`Registry.deinit(gpa)`

- [ ] **Step 1: 写失败测试**(src/server.zig 末尾)

```zig
const testing = std.testing;

fn dummyHandler(call: *ServerCall) anyerror!status_mod.Status {
    _ = call;
    return .ok;
}

test "registry registers, looks up, rejects duplicates" {
    var reg: Registry = .{};
    defer reg.deinit(testing.allocator);
    try reg.register(testing.allocator, "/test.Svc/A", dummyHandler);
    try testing.expect(reg.lookup("/test.Svc/A") != null);
    try testing.expect(reg.lookup("/test.Svc/B") == null);
    try testing.expectError(error.DuplicateMethod, reg.register(testing.allocator, "/test.Svc/A", dummyHandler));
}
```

- [ ] **Step 2: lib.zig 挂模块,跑测试确认失败**

lib.zig 加:

```zig
const server_mod = @import("server.zig");
pub const server = server_mod;
```

test 块加 `_ = server_mod;`

Run: `zig build test`
Expected: FAIL

- [ ] **Step 3: 实现 src/server.zig**

```zig
//! Server-side shapes. v1 pins down the shared types and the service
//! registry so client and server agree on Status/Metadata/HandlerFn; the
//! connection-serving path (bridging h2.serveConn) lands in the server
//! phase — see the design spec.

const std = @import("std");
const status_mod = @import("status.zig");
const metadata_mod = @import("metadata.zig");

/// One RPC as seen by a handler. Message I/O methods land with the server
/// implementation phase.
pub const ServerCall = struct {
    request_metadata: metadata_mod.Metadata = .{},
};

/// A handler's return value is the RPC's final Status; the framework owns
/// writing it as trailers.
pub const HandlerFn = *const fn (call: *ServerCall) anyerror!status_mod.Status;

/// Full-method-path → handler table ("/pkg.Service/Method").
pub const Registry = struct {
    map: std.StringHashMapUnmanaged(HandlerFn) = .empty,

    pub fn register(self: *Registry, gpa: std.mem.Allocator, path: []const u8, handler: HandlerFn) !void {
        const gop = try self.map.getOrPut(gpa, path);
        if (gop.found_existing) return error.DuplicateMethod;
        gop.value_ptr.* = handler;
    }

    pub fn lookup(self: *const Registry, path: []const u8) ?HandlerFn {
        return self.map.get(path);
    }

    pub fn deinit(self: *Registry, gpa: std.mem.Allocator) void {
        self.map.deinit(gpa);
    }
};
```

- [ ] **Step 4: 跑测试通过**

Run: `zig build test`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/server.zig src/lib.zig
git commit -m "feat: server-side shapes (ServerCall, HandlerFn, method registry)"
```

---

### Task 12: Go 互通 E2E + README

**Files:**
- Create: `testdata/go-server/main.go`
- Create: `testdata/go-server/go.mod`(经 `go mod tidy` 生成 go.sum)
- Create: `src/interop.zig`
- Create: `scripts/interop.sh`
- Modify: `build.zig`(加 interop step)
- Create: `README.md`

**Interfaces:**
- Consumes: 公共 API 全量(`Channel.connectTcp`/`unary`/`start`、`Method`、`CallOptions.status_out`)
- Produces: `zig build interop` 产出 `zig-out/bin/zig-grpc-interop`(硬编码连 `127.0.0.1:50099`,跑 5 个场景,全过退出码 0);`scripts/interop.sh` 一键起 Go server + 跑客户端
- 验收标准(spec):unary、server-stream、client-stream、bidi 四种模式 + Trailers-Only(UNIMPLEMENTED)全部对 grpc-go 通过

- [ ] **Step 1: 写 Go echo 服务**(testdata/go-server/main.go;使用 grpc-go examples 的 Echo proto,路径 `/grpc.examples.echo.Echo/*`)

```go
// Package main is the interop peer for zig-grpc: a plaintext (h2c) grpc-go
// server exposing the standard Echo service on 127.0.0.1:50099.
package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"net"

	"google.golang.org/grpc"
	pb "google.golang.org/grpc/examples/features/proto/echo"
)

type ecServer struct {
	pb.UnimplementedEchoServer
}

func (s *ecServer) UnaryEcho(_ context.Context, req *pb.EchoRequest) (*pb.EchoResponse, error) {
	return &pb.EchoResponse{Message: req.Message}, nil
}

func (s *ecServer) ServerStreamingEcho(req *pb.EchoRequest, stream pb.Echo_ServerStreamingEchoServer) error {
	for i := 0; i < 3; i++ {
		if err := stream.Send(&pb.EchoResponse{Message: fmt.Sprintf("%s-%d", req.Message, i)}); err != nil {
			return err
		}
	}
	return nil
}

func (s *ecServer) ClientStreamingEcho(stream pb.Echo_ClientStreamingEchoServer) error {
	var all string
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return stream.SendAndClose(&pb.EchoResponse{Message: all})
		}
		if err != nil {
			return err
		}
		all += req.Message
	}
}

func (s *ecServer) BidirectionalStreamingEcho(stream pb.Echo_BidirectionalStreamingEchoServer) error {
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if err := stream.Send(&pb.EchoResponse{Message: req.Message}); err != nil {
			return err
		}
	}
}

func main() {
	lis, err := net.Listen("tcp", "127.0.0.1:50099")
	if err != nil {
		log.Fatalf("listen: %v", err)
	}
	s := grpc.NewServer()
	pb.RegisterEchoServer(s, &ecServer{})
	log.Printf("echo server on %v", lis.Addr())
	if err := s.Serve(lis); err != nil {
		log.Fatalf("serve: %v", err)
	}
}
```

testdata/go-server/go.mod:

```
module zig-grpc-interop-server

go 1.22

require (
	google.golang.org/grpc v1.65.0
	google.golang.org/grpc/examples v0.0.0-20240701000000-000000000000
)
```

Run: `cd testdata/go-server && go mod tidy && go build .`
Expected: 编译通过(tidy 会改写 require 为可用版本并生成 go.sum;examples 的伪版本以 tidy 结果为准)

- [ ] **Step 2: 写 src/interop.zig**

```zig
//! Interop client: exercises unary / server-stream / client-stream / bidi /
//! trailers-only against the grpc-go echo server (testdata/go-server) on
//! 127.0.0.1:50099. Exits 0 only if every scenario passes.

const std = @import("std");
const grpc = @import("zig_grpc");

const port = 50099;

/// Hand-rolled protobuf codec for grpc.examples.echo.{EchoRequest,EchoResponse}
/// (one field: `string message = 1`) — enough wire format for interop without
/// a proto runtime.
const EchoMsg = struct {
    message: []const u8 = "",

    pub fn encode(self: EchoMsg, gpa: std.mem.Allocator) ![]u8 {
        var out: std.ArrayList(u8) = .empty;
        errdefer out.deinit(gpa);
        if (self.message.len > 0) {
            try out.append(gpa, 0x0a); // field 1, wire type 2 (LEN)
            try appendVarint(&out, gpa, self.message.len);
            try out.appendSlice(gpa, self.message);
        }
        return out.toOwnedSlice(gpa);
    }

    pub fn decode(arena: std.mem.Allocator, bytes: []const u8) !EchoMsg {
        var msg: EchoMsg = .{};
        var i: usize = 0;
        while (i < bytes.len) {
            const tag = try readVarint(bytes, &i);
            const field = tag >> 3;
            switch (@as(u3, @truncate(tag))) {
                2 => {
                    const len = try readVarint(bytes, &i);
                    if (i + len > bytes.len) return error.Malformed;
                    if (field == 1) msg.message = try arena.dupe(u8, bytes[i..][0..len]);
                    i += len;
                },
                0 => _ = try readVarint(bytes, &i),
                5 => i += 4,
                1 => i += 8,
                else => return error.Malformed,
            }
        }
        return msg;
    }
};

fn appendVarint(out: *std.ArrayList(u8), gpa: std.mem.Allocator, v: usize) !void {
    var x = v;
    while (x >= 0x80) {
        try out.append(gpa, @as(u8, @truncate(x)) | 0x80);
        x >>= 7;
    }
    try out.append(gpa, @truncate(x));
}

fn readVarint(bytes: []const u8, i: *usize) !usize {
    var shift: u6 = 0;
    var v: usize = 0;
    while (i.* < bytes.len) {
        const b = bytes[i.*];
        i.* += 1;
        v |= @as(usize, b & 0x7f) << shift;
        if (b & 0x80 == 0) return v;
        shift += 7;
        if (shift > 56) return error.Malformed;
    }
    return error.Malformed;
}

const UnaryEcho = grpc.Method(EchoMsg, EchoMsg){ .path = "/grpc.examples.echo.Echo/UnaryEcho" };
const ServerStream = grpc.Method(EchoMsg, EchoMsg){ .path = "/grpc.examples.echo.Echo/ServerStreamingEcho" };
const ClientStream = grpc.Method(EchoMsg, EchoMsg){ .path = "/grpc.examples.echo.Echo/ClientStreamingEcho" };
const BidiStream = grpc.Method(EchoMsg, EchoMsg){ .path = "/grpc.examples.echo.Echo/BidirectionalStreamingEcho" };
const NoSuchMethod = grpc.Method(EchoMsg, EchoMsg){ .path = "/grpc.examples.echo.Echo/NoSuch" };

fn expect(cond: bool, comptime what: []const u8) !void {
    if (!cond) {
        std.debug.print("FAIL: {s}\n", .{what});
        return error.InteropFailed;
    }
    std.debug.print("PASS: {s}\n", .{what});
}

pub fn main(init: std.process.Init) !void {
    const io = init.io;
    const gpa = init.gpa;

    var chan: grpc.Channel = undefined;
    try chan.connectTcp(io, gpa, "127.0.0.1", port, .{});
    defer chan.deinit();

    var arena_state = std.heap.ArenaAllocator.init(gpa);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    // 1) unary
    {
        const reply = try chan.unary(UnaryEcho, arena, .{ .message = "hello-zig" }, .{});
        try expect(std.mem.eql(u8, reply.message, "hello-zig"), "unary echo");
    }

    // 2) server streaming: 1 req -> 3 replies "m-0","m-1","m-2"
    {
        var call = try chan.start(ServerStream, .{});
        defer call.deinit();
        try call.send(.{ .message = "m" });
        try call.closeSend();
        var n: usize = 0;
        while (try call.recv(arena)) |r| : (n += 1) {
            var buf: [8]u8 = undefined;
            const want = try std.fmt.bufPrint(&buf, "m-{d}", .{n});
            try expect(std.mem.eql(u8, r.message, want), "server-stream message");
        }
        try expect(n == 3, "server-stream count");
        try expect((try call.finish()).isOk(), "server-stream status");
    }

    // 3) client streaming: "a","b","c" -> "abc"
    {
        var call = try chan.start(ClientStream, .{});
        defer call.deinit();
        try call.send(.{ .message = "a" });
        try call.send(.{ .message = "b" });
        try call.send(.{ .message = "c" });
        try call.closeSend();
        const r = (try call.recv(arena)).?;
        try expect(std.mem.eql(u8, r.message, "abc"), "client-stream concat");
        try expect((try call.recv(arena)) == null, "client-stream single reply");
        try expect((try call.finish()).isOk(), "client-stream status");
    }

    // 4) bidi ping-pong
    {
        var call = try chan.start(BidiStream, .{});
        defer call.deinit();
        for ([_][]const u8{ "p1", "p2", "p3" }) |m| {
            try call.send(.{ .message = m });
            const r = (try call.recv(arena)).?;
            try expect(std.mem.eql(u8, r.message, m), "bidi echo");
        }
        try call.closeSend();
        try expect((try call.recv(arena)) == null, "bidi end");
        try expect((try call.finish()).isOk(), "bidi status");
    }

    // 5) unknown method -> Trailers-Only UNIMPLEMENTED(对着真 grpc-go 验证该路径)
    {
        var st: grpc.Status = undefined;
        const r = chan.unary(NoSuchMethod, arena, .{ .message = "x" }, .{ .status_out = &st });
        try expect(r == error.RpcFailed, "unknown method fails");
        try expect(st.code == .unimplemented, "unknown method is UNIMPLEMENTED");
    }

    std.debug.print("interop: all scenarios passed\n", .{});
}
```

- [ ] **Step 3: build.zig 加 interop step**(build 函数末尾追加)

```zig
    // ---- `zig build interop` ----
    const interop_mod = b.createModule(.{
        .root_source_file = b.path("src/interop.zig"),
        .target = target,
        .optimize = optimize,
    });
    interop_mod.addImport("zig_grpc", mod);
    const interop_exe = b.addExecutable(.{ .name = "zig-grpc-interop", .root_module = interop_mod });
    const interop_step = b.step("interop", "Build the Go-interop client (see scripts/interop.sh)");
    interop_step.dependOn(&b.addInstallArtifact(interop_exe, .{}).step);
```

- [ ] **Step 4: 写 scripts/interop.sh 并加执行位**

```sh
#!/bin/sh
# One-shot interop run: build+start the grpc-go echo server, run the zig
# client against it, tear down. Requires a local Go toolchain.
set -e
cd "$(dirname "$0")/.."

go build -o zig-out/go-echo-server ./testdata/go-server
zig build interop

zig-out/go-echo-server &
GO_PID=$!
trap 'kill $GO_PID 2>/dev/null' EXIT
sleep 1

./zig-out/bin/zig-grpc-interop
echo "INTEROP OK"
```

Run: `chmod +x scripts/interop.sh`

- [ ] **Step 5: 跑通互通**

Run: `./scripts/interop.sh`
Expected: 逐行 `PASS: …`,最后 `interop: all scenarios passed` + `INTEROP OK`,退出码 0。
调试注意:失败时先用 `GODEBUG=http2debug=2 zig-out/go-echo-server` 看 Go 侧帧日志定位是客户端帧序问题还是状态机问题。

- [ ] **Step 6: 写 README.md**

内容(自拟措辞,涵盖):项目定位(gRPC on zig-http2,client 先行)、quickstart(下面的代码)、四种模式示例(从 interop.zig 精简)、v1 限制(identity-only、无本地 deadline、cancel 语义、单发送/单接收线程)、`scripts/interop.sh` 用法、指向 spec 文档。quickstart 代码:

```zig
const std = @import("std");
const grpc = @import("zig_grpc");

const HelloMsg = struct {
    message: []const u8 = "",
    pub fn encode(self: @This(), gpa: std.mem.Allocator) ![]u8 { ... } // 见 README 正文
    pub fn decode(arena: std.mem.Allocator, bytes: []const u8) !@This() { ... }
};
const SayHello = grpc.Method(HelloMsg, HelloMsg){ .path = "/grpc.examples.echo.Echo/UnaryEcho" };

pub fn main(init: std.process.Init) !void {
    var chan: grpc.Channel = undefined;
    try chan.connectTcp(init.io, init.gpa, "127.0.0.1", 50051, .{});
    defer chan.deinit();
    var arena_state = std.heap.ArenaAllocator.init(init.gpa);
    defer arena_state.deinit();
    const reply = try chan.unary(SayHello, arena_state.allocator(), .{ .message = "hi" }, .{});
    std.debug.print("{s}\n", .{reply.message});
}
```

(README 中 encode/decode 给完整实现,直接抄 interop.zig 的 EchoMsg。)

- [ ] **Step 7: 全量验证 + Commit**

Run: `zig build test && ./scripts/interop.sh`
Expected: 单测全绿 + INTEROP OK

```bash
git add testdata scripts src/interop.zig build.zig README.md
git commit -m "feat: Go interop suite (unary + 3 streaming modes + trailers-only) and README"
```

---

## Self-Review 记录

- **Spec 覆盖**:模块划分(T1-T11 与 spec 文件表一一对应)、共享类型(T2/T3)、codec 契约(T5)、Channel 双层(T6/T10)、RawCall 全 API 与状态机(T6/T7/T8)、错误模型含 status_out(T9)、server 骨架(T11)、三层测试策略(单元 T2-T5、回环 T6-T9、互通 T12)。spec 的 `header(arena)` 在 T7 改为无参返回内部 arena 所有权(已在任务内标注为有意偏差,完成时回写 spec);`RawCall.finish` 对 RST 的呈现细化同样在 T8 标注。
- **占位符**:T3 Step 1 的测试代码曾含笔误行,已在原地给出"最终以三断言为准"的修正;T12 README 的 encode/decode 指向 interop.zig 完整实现(同文件内可抄),无 TBD。
- **类型一致性**:`RawCall.init(chan, stream)`(T6)= T7/T8 使用;`Assembler.init(gpa,max)`(T4)= T6 使用;`Method` 的 `*const fn` 字段(T5)= T9 `M.encode_req(...)` 调用;`Loopback.start(..., userdata, chan_opts)`(T6)= T7/T9 传 `null, .{}`,一致。
