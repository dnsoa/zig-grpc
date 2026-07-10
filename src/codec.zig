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
