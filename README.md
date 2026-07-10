# zig-grpc

gRPC for Zig, built on [zig-http2](../zig-http2). The core is bytes-in / bytes-out
(HTTP/2 + gRPC framing); message encode/decode plugs in through a comptime
contract, so any protobuf library (or hand-rolled struct) works without the
runtime taking a dependency on one.

**v1 ships the client.** Unary, server-streaming, client-streaming, and
bidirectional streaming all work against a real grpc-go server
(see [Go interop](#go-interop)). The server side is scaffolded (shared types +
method registry); the connection-serving path is later work.

- Zig 0.16.0 (`std.Io` model)
- Single dependency: `zig_http2` (path `../zig-http2`)
- Plaintext h2c only (TLS is the caller's job — wrap your own reader/writer)

## Quickstart

```zig
const std = @import("std");
const grpc = @import("zig_grpc");

// Your message: anything with `encode(self, gpa) ![]u8` and
// `decode(arena, bytes) !T` (zig-protobuf messages fit; here it's hand-rolled
// for the Echo proto's `string message = 1`).
const HelloMsg = struct {
    message: []const u8 = "",

    pub fn encode(self: HelloMsg, gpa: std.mem.Allocator) ![]u8 {
        var out: std.ArrayList(u8) = .empty;
        errdefer out.deinit(gpa);
        if (self.message.len > 0) {
            try out.append(gpa, 0x0a); // field 1, wire type 2 (LEN)
            try appendVarint(&out, gpa, self.message.len);
            try out.appendSlice(gpa, self.message);
        }
        return out.toOwnedSlice(gpa);
    }

    pub fn decode(arena: std.mem.Allocator, bytes: []const u8) !HelloMsg {
        var msg: HelloMsg = .{};
        var i: usize = 0;
        while (i < bytes.len) {
            const tag = try readVarint(bytes, &i);
            if (tag >> 3 == 1 and @as(u3, @truncate(tag)) == 2) {
                const len = try readVarint(bytes, &i);
                msg.message = try arena.dupe(u8, bytes[i..][0..len]);
                i += len;
            } else switch (@as(u3, @truncate(tag))) {
                0 => _ = try readVarint(bytes, &i),
                5 => i += 4,
                1 => i += 8,
                else => return error.Malformed,
            }
        }
        return msg;
    }
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

`appendVarint` / `readVarint` are the usual LEB128 helpers — see
[src/interop.zig](src/interop.zig) for a complete, copy-pasteable version.

## The four RPC modes

A `grpc.Method(Req, Res)` value ties a `"/pkg.Service/Method"` path to a codec.
`Channel.start(M, opts)` returns a typed `Call(M)`:

```zig
var call = try chan.start(SomeBidi, .{});
defer call.deinit();
try call.send(.{ ... });          // send a request message
const reply = (try call.recv(arena)).?;  // receive a response message
try call.closeSend();             // half-close the request side
while (try call.recv(arena)) |r| { ... } // drain server-stream / bidi
const status = try call.finish(); // final gRPC Status (await trailers)
```

- **Unary:** `chan.unary(M, arena, req, opts)` does send → half-close → receive
  one → status check in one call. On a non-OK status it returns `error.RpcFailed`
  and, if `opts.status_out` is set, writes the `Status` there.
- **Server / client / bidi streaming:** use `start` + `send` / `recv` /
  `closeSend` / `finish`. `recv` returns `null` once the server has finished.

`CallOptions` carries request `metadata`, an optional `timeout_ns` (sent as
`grpc-timeout`, enforced by the **server**), and the optional `status_out`
pointer for `unary`.

## Threading model & v1 limits

- One **sender** thread and one **receiver** thread per call (inherited from
  `h2.Stream`). `deinit` must not race a concurrent `send`/`recv`.
- `cancel()` is thread-safe and **does** interrupt a `recv` blocked on another
  thread (it sends RST_STREAM and locally wakes the blocked read). That makes a
  client-side deadline implementable with a watchdog thread that calls
  `cancel()` on timeout.
- **Compression:** identity only (`grpc-encoding: identity`); a compressed
  message surfaces as an error.
- **No connection pool / reconnect / load balancing** — a `Channel` is one
  HTTP/2 connection.
- **TLS:** not built in. `Channel.init` takes any `*std.Io.Reader` /
  `*std.Io.Writer`, so wrap a TLS stream yourself; `connectTcp` is the plaintext
  h2c convenience.

## Go interop

`testdata/go-server` is a plaintext (h2c) grpc-go server implementing the
standard Echo service on `127.0.0.1:50099`. The `interop` client
(`zig build interop`) runs unary + the three streaming modes + an unknown-method
Trailers-Only case against it.

```sh
./scripts/interop.sh   # needs a local Go toolchain
```

On success it prints `PASS:` lines for each scenario, then `INTEROP OK`.

## Layout

- `src/status.zig` — gRPC status codes + HTTP/h2/RST mappings + `grpc-message` decoding
- `src/metadata.zig` — metadata, `-bin` base64, `grpc-timeout` encoding
- `src/frame.zig` — length-prefixed message framing + cross-DATA reassembly
- `src/codec.zig` — the comptime `Method` contract
- `src/channel.zig` — `Channel` (one h2 connection) + `connectTcp`
- `src/call.zig` — `RawCall` (byte-level gRPC state machine) + typed `Call(M)`
- `src/server.zig` — server-side shapes (`ServerCall`, `HandlerFn`, `Registry`)
- `src/interop.zig` — the grpc-go interop client
