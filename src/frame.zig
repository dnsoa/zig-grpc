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
        self.reclaim();
        return msg;
    }

    /// Drops already-consumed bytes so a long-lived stream whose messages never
    /// align to DATA-frame boundaries stays bounded. Full drain clears the
    /// buffer; otherwise the tail is compacted to the front once the consumed
    /// prefix reaches half the buffer, which keeps the copy work amortized O(1).
    fn reclaim(self: *Assembler) void {
        if (self.head == self.buf.items.len) {
            self.buf.clearRetainingCapacity();
            self.head = 0;
        } else if (self.head >= self.buf.items.len - self.head) {
            const remaining = self.buf.items.len - self.head;
            std.mem.copyForwards(u8, self.buf.items[0..remaining], self.buf.items[self.head..]);
            self.buf.shrinkRetainingCapacity(remaining);
            self.head = 0;
        }
    }

    /// True when leftover bytes do not form a complete message — a protocol
    /// error if the stream has already ended.
    pub fn hasPartial(self: *const Assembler) bool {
        return self.buf.items.len > self.head;
    }
};

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

test "long stream with unaligned frames stays bounded (compaction)" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();
    var a = Assembler.init(testing.allocator, 1024);
    defer a.deinit();

    const payload = "0123456789"; // 10-byte payload -> 15-byte frame
    const prefix = encodePrefix(payload.len);
    const bound = prefix_len + payload.len + prefix_len; // one full frame + a pending prefix

    // Prime with the first message's prefix, then on each iteration complete the
    // pending message and start the next one's prefix so the buffer never fully
    // drains — without compaction its length would grow by a frame every round.
    try a.feed(&prefix);
    var iter: usize = 0;
    while (iter < 1000) : (iter += 1) {
        try a.feed(payload);
        try a.feed(&prefix);
        const m = (try a.next(arena)).?;
        try testing.expectEqualStrings(payload, m);
        _ = arena_state.reset(.retain_capacity);
        try testing.expect(a.buf.items.len <= bound);
    }
    try testing.expect(a.hasPartial());
}

test "hasPartial reports a truncated message" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    var a = Assembler.init(testing.allocator, 1024);
    defer a.deinit();
    const p = encodePrefix(4);
    try a.feed(&p);
    try a.feed("ab"); // 4 declared, only 2 arrived
    try testing.expect((try a.next(arena_state.allocator())) == null);
    try testing.expect(a.hasPartial());
}
