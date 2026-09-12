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
///
/// Owns its keys. Registration paths are routinely built rather than written
/// out — formatted from a service name, read from generated code, held in an
/// arena that is reset after setup — and a registry outliving them would then
/// be hashing freed memory on every lookup.
pub const Registry = struct {
    map: std.StringHashMapUnmanaged(HandlerFn) = .empty,

    pub fn register(self: *Registry, gpa: std.mem.Allocator, path: []const u8, handler: HandlerFn) !void {
        const gop = try self.map.getOrPut(gpa, path);
        if (gop.found_existing) return error.DuplicateMethod;
        // getOrPut stored the caller's slice as the key; swap in our own copy.
        // Same bytes, so the hash is unchanged and the entry stays valid.
        gop.key_ptr.* = gpa.dupe(u8, path) catch |err| {
            _ = self.map.remove(path);
            return err;
        };
        gop.value_ptr.* = handler;
    }

    pub fn lookup(self: *const Registry, path: []const u8) ?HandlerFn {
        return self.map.get(path);
    }

    pub fn deinit(self: *Registry, gpa: std.mem.Allocator) void {
        var it = self.map.keyIterator();
        while (it.next()) |k| gpa.free(k.*);
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

test "registry owns its keys (a caller's temporary path stays valid)" {
    var reg: Registry = .{};
    defer reg.deinit(testing.allocator);

    // The shape that used to dangle: the path lives in an arena that is gone
    // before the first lookup.
    {
        var tmp = std.heap.ArenaAllocator.init(testing.allocator);
        defer tmp.deinit();
        const path = try std.fmt.allocPrint(tmp.allocator(), "/{s}.Svc/{s}", .{ "pkg", "Method" });
        try reg.register(testing.allocator, path, dummyHandler);
    }

    try testing.expect(reg.lookup("/pkg.Svc/Method") != null);
    // Duplicate detection still works against the owned copy.
    try testing.expectError(error.DuplicateMethod, reg.register(testing.allocator, "/pkg.Svc/Method", dummyHandler));
}
