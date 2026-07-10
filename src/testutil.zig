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
