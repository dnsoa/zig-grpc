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
        // Manually remove trailing '=' padding
        var trimmed_len = v.len;
        while (trimmed_len > 0 and v[trimmed_len - 1] == '=') {
            trimmed_len -= 1;
        }
        const trimmed = v[0..trimmed_len];
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
    const threshold: u64 = 999;
    const units = [_]struct { div: u64, unit: u8 }{
        .{ .div = 1, .unit = 'n' },
        .{ .div = std.time.ns_per_us, .unit = 'u' },
        .{ .div = std.time.ns_per_ms, .unit = 'm' },
    };
    // First pass: prefer units where v <= threshold
    for (units) |u| {
        const v = std.math.divCeil(u64, ns, u.div) catch unreachable;
        if (v <= threshold) return std.fmt.bufPrint(buf, "{d}{c}", .{ v, u.unit }) catch unreachable;
    }
    // Second pass: fall back to v <= max
    for (units) |u| {
        const v = std.math.divCeil(u64, ns, u.div) catch unreachable;
        if (v <= max) return std.fmt.bufPrint(buf, "{d}{c}", .{ v, u.unit }) catch unreachable;
    }
    // Over 99999999 milliseconds (~11 days): use hours with clamping.
    const v_h = std.math.divCeil(u64, ns, std.time.ns_per_hour) catch unreachable;
    const clamped = @min(v_h, max);
    return std.fmt.bufPrint(buf, "{d}H", .{clamped}) catch unreachable;
}

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

test "encodeTimeout falls back to hours without trailing garbage" {
    var buf: [9]u8 = undefined;
    // > 99_999_999 ms (~27.8h) forces the hours branch; the returned slice
    // must be exactly the printed value, not the whole backing buffer.
    try testing.expectEqualStrings("48H", encodeTimeout(48 * std.time.ns_per_hour, &buf));
    // Largest u64 ns is ~5.1M hours (8 digits) — still a clean slice, no tail.
    try testing.expectEqualStrings("5124096H", encodeTimeout(std.math.maxInt(u64), &buf));
}
