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
    /// A name without the `-bin` suffix is `error.NotBinaryMetadata`: this is a
    /// library call taking a caller-supplied string, and an assert would abort
    /// the process over it in every safe build.
    pub fn getBin(self: Metadata, arena: std.mem.Allocator, name: []const u8) !?[]u8 {
        if (name.len < 4 or !std.ascii.eqlIgnoreCase(name[name.len - 4 ..], "-bin")) {
            return error.NotBinaryMetadata;
        }
        const v = self.get(name) orelse return null;
        // Manually remove at most two trailing '=' padding bytes when aligned.
        var trimmed_len = v.len;
        while (trimmed_len > 0 and v[trimmed_len - 1] == '=') {
            trimmed_len -= 1;
        }
        const pad_count = v.len - trimmed_len;
        if (pad_count > 0 and (pad_count > 2 or trimmed_len == 0 or v.len % 4 != 0)) {
            return error.InvalidBase64;
        }
        const trimmed = v[0..trimmed_len];
        const dec = std.base64.standard_no_pad.Decoder;
        const n = dec.calcSizeForSlice(trimmed) catch return error.InvalidBase64;
        const out = try arena.alloc(u8, n);
        errdefer arena.free(out);
        dec.decode(out, trimmed) catch return error.InvalidBase64;
        return out;
    }
};

/// Header names the client manages itself or that HTTP/2 forbids on the wire
/// (connection-specific headers, RFC 9113 §8.2.2); user metadata may not use
/// them.
pub fn isReservedName(name: []const u8) bool {
    if (name.len == 0) return true;
    if (name[0] == ':') return true;
    if (std.ascii.startsWithIgnoreCase(name, "grpc-")) return true;
    const reserved = [_][]const u8{
        // gRPC/HTTP/2 transport headers managed by the client
        "content-type",
        "te",
        "user-agent",
        // HTTP/2 forbidden connection-specific headers (RFC 9113 §8.2.2)
        "connection",
        "keep-alive",
        "proxy-connection",
        "transfer-encoding",
        "upgrade",
    };
    for (reserved) |r| {
        if (std.ascii.eqlIgnoreCase(name, r)) return true;
    }
    return false;
}

/// Whether a metadata key is legal to put on the wire. The gRPC spec allows
/// digits, lowercase letters, `_`, `-` and `.`; uppercase is accepted here
/// because HPACK lowercases names on the way out (HTTP/2 field names must be
/// lowercase), so `X-Trace-Id` and `x-trace-id` are the same header.
///
/// The point is to reject everything else — spaces, `:`, and above all CR/LF
/// and NUL. HPACK is length-prefixed, so a stray CRLF cannot split a frame
/// here, but a gateway translating the request down to HTTP/1.1 turns it into
/// response splitting. Validating at the edge is what the other gRPC
/// implementations do.
pub fn isValidName(name: []const u8) bool {
    if (name.len == 0) return false;
    for (name) |ch| {
        const ok = (ch >= '0' and ch <= '9') or
            (ch >= 'a' and ch <= 'z') or
            (ch >= 'A' and ch <= 'Z') or
            ch == '_' or ch == '-' or ch == '.';
        if (!ok) return false;
    }
    return true;
}

/// Whether a metadata value is legal to put on the wire: printable ASCII
/// (0x20–0x7E), per the gRPC spec's `ASCII-Value`. Binary metadata travels
/// base64-encoded under a `-bin` key, so it is printable too. Empty is allowed
/// — the spec's grammar says otherwise but real implementations send it.
pub fn isValidValue(value: []const u8) bool {
    for (value) |ch| {
        if (ch < 0x20 or ch > 0x7e) return false;
    }
    return true;
}

/// Encodes a deadline as a `grpc-timeout` value: at most 8 digits plus a
/// unit (`H`/`M`/`S`/`m`/`u`/`n`). Prefers the coarsest unit that represents
/// `ns` exactly within 8 digits (e.g. 30s -> `30S`, 1.5s -> `1500m`), and
/// otherwise falls back to the smallest unit that fits, rounding up so a
/// deadline never shortens.
pub fn encodeTimeout(ns: u64, buf: *[9]u8) []const u8 {
    const max: u64 = 99_999_999;
    const threshold: u64 = 999;
    const units = [_]struct { div: u64, unit: u8 }{
        .{ .div = 1, .unit = 'n' },
        .{ .div = std.time.ns_per_us, .unit = 'u' },
        .{ .div = std.time.ns_per_ms, .unit = 'm' },
        .{ .div = std.time.ns_per_s, .unit = 'S' },
        .{ .div = std.time.ns_per_min, .unit = 'M' },
        .{ .div = std.time.ns_per_hour, .unit = 'H' },
    };
    // Exact pass: prefer the coarsest unit (H..u) that fits without rounding,
    // e.g. 30s -> "30S" and 1.5s -> "1500m" (not "2S").
    if (ns > 0) {
        var i: usize = units.len;
        while (i > 1) {
            i -= 1;
            const u = units[i];
            if (ns % u.div == 0) {
                const v = ns / u.div;
                if (v <= max) return std.fmt.bufPrint(buf, "{d}{c}", .{ v, u.unit }) catch unreachable;
                // Every finer unit produces an even larger quotient (> max).
                break;
            }
        }
    }
    // First rounding pass: prefer compact sub-second units (n/u/m) where v <= threshold.
    for (units[0..3]) |u| {
        const v = std.math.divCeil(u64, ns, u.div) catch unreachable;
        if (v <= threshold) return std.fmt.bufPrint(buf, "{d}{c}", .{ v, u.unit }) catch unreachable;
    }
    // Second rounding pass: smallest spec unit (n/u/m/S/M) that fits within 8 digits.
    for (units[0..5]) |u| {
        const v = std.math.divCeil(u64, ns, u.div) catch unreachable;
        if (v <= max) return std.fmt.bufPrint(buf, "{d}{c}", .{ v, u.unit }) catch unreachable;
    }
    // Over 99_999_999 minutes: fall back to hours. Largest u64 ns is ~5.1M hours
    // (7 digits), so it always fits the 8-digit budget.
    const v_h = std.math.divCeil(u64, ns, std.time.ns_per_hour) catch unreachable;
    return std.fmt.bufPrint(buf, "{d}H", .{v_h}) catch unreachable;
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

test "isReservedName blocks pseudo, grpc-*, transport, and connection-specific headers" {
    try testing.expect(isReservedName(":path"));
    try testing.expect(isReservedName("grpc-timeout"));
    try testing.expect(isReservedName("content-type"));
    try testing.expect(isReservedName("te"));
    try testing.expect(isReservedName("user-agent"));
    // RFC 9113 §8.2.2 connection-specific headers (case-insensitive)
    try testing.expect(isReservedName("connection"));
    try testing.expect(isReservedName("Connection"));
    try testing.expect(isReservedName("keep-alive"));
    try testing.expect(isReservedName("proxy-connection"));
    try testing.expect(isReservedName("transfer-encoding"));
    try testing.expect(isReservedName("Transfer-Encoding"));
    try testing.expect(isReservedName("upgrade"));
    try testing.expect(isReservedName("UPGRADE"));
    try testing.expect(isReservedName(""));
    try testing.expect(!isReservedName("x-custom"));
    try testing.expect(!isReservedName("authorization"));
}

test "encodeTimeout prefers exact coarser units and otherwise rounds up" {
    var buf: [9]u8 = undefined;
    try testing.expectEqualStrings("0n", encodeTimeout(0, &buf));
    try testing.expectEqualStrings("100n", encodeTimeout(100, &buf));
    try testing.expectEqualStrings("1500u", encodeTimeout(1_500 * std.time.ns_per_us, &buf));
    try testing.expectEqualStrings("1500m", encodeTimeout(1_500 * std.time.ns_per_ms, &buf));
    try testing.expectEqualStrings("1S", encodeTimeout(std.time.ns_per_s, &buf));
    try testing.expectEqualStrings("30S", encodeTimeout(30 * std.time.ns_per_s, &buf));
    try testing.expectEqualStrings("100S", encodeTimeout(100 * std.time.ns_per_s, &buf));
    try testing.expectEqualStrings("5M", encodeTimeout(5 * std.time.ns_per_min, &buf));
    // Non-exact durations keep smallest-unit-that-fits and round up so deadlines never shorten.
    try testing.expectEqualStrings("2u", encodeTimeout(1_500, &buf));
    try testing.expectEqualStrings("1000001u", encodeTimeout(std.time.ns_per_s + 1, &buf));
    try testing.expectEqualStrings("100001m", encodeTimeout(100 * std.time.ns_per_s + 1, &buf));
}

test "encodeTimeout uses S/M before falling back to hours without trailing garbage" {
    var buf: [9]u8 = undefined;
    // Exact multi-hour/minute values prefer coarser units without rounding loss.
    try testing.expectEqualStrings("1681M", encodeTimeout(28 * std.time.ns_per_hour + std.time.ns_per_min, &buf));
    try testing.expectEqualStrings("48H", encodeTimeout(48 * std.time.ns_per_hour, &buf));
    // > 99_999_999 ms (~27.8h) non-exact uses 'S' instead of rounding up by nearly an hour.
    try testing.expectEqualStrings("100861S", encodeTimeout(28 * std.time.ns_per_hour + std.time.ns_per_min + 500 * std.time.ns_per_ms, &buf));
    // > 99_999_999 S (~3.17y) uses 'M' (both exact overflow and non-exact).
    try testing.expectEqualStrings("3333334M", encodeTimeout(200_000_000 * std.time.ns_per_s, &buf));
    try testing.expectEqualStrings("2000001M", encodeTimeout(2_000_000 * std.time.ns_per_min + 1, &buf));
    // > 99_999_999 M forces the hours branch; returned slice has no trailing garbage.
    try testing.expectEqualStrings("2000001H", encodeTimeout(2_000_000 * std.time.ns_per_hour + 1, &buf));
    try testing.expectEqualStrings("5124096H", encodeTimeout(std.math.maxInt(u64), &buf));
}

test "isValidName accepts gRPC key characters, rejects the rest" {
    try testing.expect(isValidName("x-trace-id"));
    try testing.expect(isValidName("grpc.internal_key-1"));
    // Uppercase is accepted: HPACK lowercases names on the way out.
    try testing.expect(isValidName("X-Trace-Id"));
    try testing.expect(!isValidName(""));
    try testing.expect(!isValidName("has space"));
    try testing.expect(!isValidName("colon:name"));
    try testing.expect(!isValidName("crlf\r\nname"));
    try testing.expect(!isValidName("nul\x00name"));
}

test "isValidValue accepts printable ASCII, rejects control bytes" {
    try testing.expect(isValidValue("plain value 123"));
    try testing.expect(isValidValue("")); // empty is tolerated
    try testing.expect(isValidValue("aGk=")); // base64 for -bin keys
    // The ones that matter: a gateway downgrading to HTTP/1.1 would turn these
    // into response splitting.
    try testing.expect(!isValidValue("v\r\nX-Injected: 1"));
    try testing.expect(!isValidValue("v\n"));
    try testing.expect(!isValidValue("v\x00"));
    try testing.expect(!isValidValue("hi\x7f"));
}

test "getBin rejects a non -bin name instead of asserting" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const md: Metadata = .{ .entries = &.{.{ .name = "k-bin", .value = "aGk=" }} };
    // Previously std.debug.assert — an abort, in every safe build, over a
    // caller-supplied string.
    try testing.expectError(error.NotBinaryMetadata, md.getBin(arena_state.allocator(), "k"));
}

test "getBin accepts case-insensitive -bin suffix and rejects malformed '=' padding" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();
    const md: Metadata = .{ .entries = &.{
        .{ .name = "x-payload-bin", .value = "aGk=" },
        .{ .name = "empty-bin", .value = "" },
        .{ .name = "only-pad-bin", .value = "====" },
        .{ .name = "over-pad-bin", .value = "aGk=====" },
        .{ .name = "misaligned-pad-bin", .value = "aGk==" },
    } };
    try testing.expectEqualStrings("hi", (try md.getBin(arena, "X-Payload-Bin")).?);
    try testing.expectEqualStrings("hi", (try md.getBin(arena, "X-PAYLOAD-BIN")).?);
    try testing.expectEqualStrings("", (try md.getBin(arena, "empty-bin")).?);
    try testing.expectError(error.InvalidBase64, md.getBin(arena, "only-pad-bin"));
    try testing.expectError(error.InvalidBase64, md.getBin(arena, "over-pad-bin"));
    try testing.expectError(error.InvalidBase64, md.getBin(arena, "misaligned-pad-bin"));
}
