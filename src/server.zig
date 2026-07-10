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
