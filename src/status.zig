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
