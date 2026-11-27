//! Test-only helper: an in-process zig-http2 server the client talks to over
//! an AF_UNIX socketpair (the same pattern zig-http2's own tests use).
//! Not referenced outside `test` blocks.

const std = @import("std");
const h2 = @import("zig_http2");
const channel_mod = @import("channel.zig");
const Io = std.Io;

pub fn makeSocketPairStreams() !struct { client: Io.net.Stream, server: Io.net.Stream } {
    var fds: [2]c_int = undefined;
    if (std.c.socketpair(std.posix.AF.UNIX, std.posix.SOCK.STREAM, 0, &fds) != 0) {
        return error.SocketPairFailed;
    }
    const dummy_addr = Io.net.IpAddress.parse("127.0.0.1", 0) catch unreachable;
    return .{
        .client = .{ .socket = .{ .handle = fds[0], .address = dummy_addr } },
        .server = .{ .socket = .{ .handle = fds[1], .address = dummy_addr } },
    };
}

pub const Loopback = struct {
    io: Io,
    gpa: std.mem.Allocator,
    cstream: Io.net.Stream,
    sstream: Io.net.Stream,
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
        const pair = try makeSocketPairStreams();
        self.cstream = pair.client;
        self.sstream = pair.server;
        errdefer self.cstream.close(io);
        self.srv = .{ .io = io, .gpa = gpa, .handler = handler, .userdata = userdata };
        self.thread = std.Thread.spawn(.{}, serve, .{self}) catch |err| {
            self.sstream.close(io);
            return err;
        };
        self.csr = self.cstream.reader(io, &self.crbuf);
        self.csw = self.cstream.writer(io, &self.cwbuf);
        var opts = chan_opts;
        if (opts.authority.len == 0) opts.authority = "loopback";
        try self.chan.init(io, gpa, &self.csr.interface, &self.csw.interface, opts);
    }

    fn serve(self: *Loopback) void {
        defer self.sstream.close(self.io);
        var rbuf: [8192]u8 = undefined;
        var wbuf: [8192]u8 = undefined;
        var sr = self.sstream.reader(self.io, &rbuf);
        var sw = self.sstream.writer(self.io, &wbuf);
        h2.serveConn(&self.srv, &sr.interface, &sw.interface, null, "http");
    }

    /// Tears down in LIFO order: shutdown+close client socket so both reader
    /// loops see EOF → channel deinit → join server thread.
    pub fn stop(self: *Loopback) void {
        self.cstream.shutdown(self.io, .both) catch {};
        self.chan.deinit();
        self.cstream.close(self.io);
        if (self.thread) |t| t.join();
    }
};

/// A raw-frame HTTP/2 peer: does the h2 handshake then hands the test direct
/// frame-level read/write. For wire shapes h2.Server cannot produce (true
/// Trailers-Only, RST_STREAM injection, missing grpc-status).
pub const RawPeer = struct {
    io: Io,
    gpa: std.mem.Allocator,
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
        const pair = try makeSocketPairStreams();
        self.cstream = pair.client;
        self.accepted = pair.server;
        errdefer {
            self.cstream.close(io);
            self.accepted.close(io);
        }
        self.csr = self.cstream.reader(io, &self.crbuf);
        self.csw = self.cstream.writer(io, &self.cwbuf);
        self.asr = self.accepted.reader(io, &self.arbuf);
        self.asw = self.accepted.writer(io, &self.awbuf);
        try self.chan.init(io, gpa, &self.csr.interface, &self.csw.interface, .{ .authority = "raw" });
        // Server handshake: preface + client SETTINGS, reply empty SETTINGS + ack.
        var pf: [h2.proto.preface.len]u8 = undefined;
        try self.asr.interface.readSliceAll(&pf);
        const f = try self.readFrame();
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
        std.mem.writeInt(u32, &p, @backingInt(code), .big);
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
        self.cstream.shutdown(self.io, .both) catch {};
        self.accepted.shutdown(self.io, .both) catch {};
        self.chan.deinit();
        self.cstream.close(self.io);
        self.accepted.close(self.io);
    }
};
