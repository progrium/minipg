//! Where minipg keeps things outside the .pgdb: runtime state (run.json,
//! the control socket), logs, and the root cache. The .pgdb itself holds
//! only data and config.
//!
//!   what        Linux                                macOS
//!   runtime     $XDG_RUNTIME_DIR/minipg/<key>/       $TMPDIR/minipg-$UID/<key>/
//!   logs        ~/.local/state/minipg/logs/<key>.log ~/Library/Logs/minipg/<key>.log
//!   roots       ~/.cache/minipg/roots/<digest>/      ~/Library/Caches/minipg/roots/<digest>/
//!
//!   what        Windows
//!   runtime     %LOCALAPPDATA%\minipg\run\<key>\
//!   logs        %LOCALAPPDATA%\minipg\logs\<key>.log
//!   roots       %LOCALAPPDATA%\minipg\roots\<digest>\
//!
//! <key> is a hash of the .pgdb directory's device and inode (on Windows,
//! its volume and file index), so it follows the directory through
//! renames. Linux falls back to ~/.cache/minipg/run/ without
//! XDG_RUNTIME_DIR, and honors XDG_STATE_HOME and XDG_CACHE_HOME. Runtime
//! directories are mode 0700; on Windows, %LOCALAPPDATA% is already the
//! user's own. On macOS, $TMPDIR is taken by its real path (/var is a
//! symlink to /private/var).
//!
//! A unix socket's path has a small limit (103 bytes on macOS, 107 on
//! Linux). When the control socket's path in the runtime directory would
//! be longer, it goes in /tmp/minipg-$UID/<key>.sock instead; run.json,
//! which names it, stays in the runtime directory.

const std = @import("std");
const builtin = @import("builtin");
const sys = @import("sys.zig");

const Environ = std.process.Environ;

pub const Dirs = struct {
    /// Parent of the per-.pgdb runtime directories.
    runtime: []const u8,
    /// Directory of per-.pgdb log files.
    logs: []const u8,
    /// The root cache.
    roots: []const u8,

    pub fn runtimeFor(d: Dirs, arena: std.mem.Allocator, k: []const u8) ![]const u8 {
        return std.fs.path.join(arena, &.{ d.runtime, k });
    }

    pub fn logFor(d: Dirs, arena: std.mem.Allocator, k: []const u8) ![]const u8 {
        return std.fmt.allocPrint(arena, "{s}{c}{s}.log", .{ d.logs, std.fs.path.sep, k });
    }
};

fn get(env: *const Environ.Map, name: []const u8) ?[]const u8 {
    const v = env.get(name) orelse return null;
    return if (v.len > 0) v else null;
}

/// What `dirs` needs and couldn't find, for the error message.
pub const home_var = if (sys.is_windows) "LOCALAPPDATA" else "HOME";

pub fn dirs(arena: std.mem.Allocator, env: *const Environ.Map) !Dirs {
    if (sys.is_windows) {
        const local = get(env, "LOCALAPPDATA") orelse return error.NoHomeDir;
        return .{
            .runtime = try std.fs.path.join(arena, &.{ local, "minipg", "run" }),
            .logs = try std.fs.path.join(arena, &.{ local, "minipg", "logs" }),
            .roots = try std.fs.path.join(arena, &.{ local, "minipg", "roots" }),
        };
    }
    const home = get(env, "HOME") orelse return error.NoHomeDir;
    if (builtin.os.tag == .macos) {
        const tmp = try realPath(arena, get(env, "TMPDIR") orelse "/tmp");
        return .{
            .runtime = try std.fmt.allocPrint(arena, "{s}/minipg-{d}", .{ std.mem.trimEnd(u8, tmp, "/"), std.c.getuid() }),
            .logs = try std.fs.path.join(arena, &.{ home, "Library/Logs/minipg" }),
            .roots = try std.fs.path.join(arena, &.{ home, "Library/Caches/minipg/roots" }),
        };
    }
    const cache = get(env, "XDG_CACHE_HOME") orelse try std.fs.path.join(arena, &.{ home, ".cache" });
    const state = get(env, "XDG_STATE_HOME") orelse try std.fs.path.join(arena, &.{ home, ".local/state" });
    const runtime = if (get(env, "XDG_RUNTIME_DIR")) |r|
        try std.fs.path.join(arena, &.{ r, "minipg" })
    else
        try std.fs.path.join(arena, &.{ cache, "minipg/run" });
    return .{
        .runtime = runtime,
        .logs = try std.fs.path.join(arena, &.{ state, "minipg/logs" }),
        .roots = try std.fs.path.join(arena, &.{ cache, "minipg/roots" }),
    };
}

/// `path` with symlinks resolved, or `path` itself if it can't be.
fn realPath(arena: std.mem.Allocator, path: []const u8) ![]const u8 {
    var buf: [std.c.PATH_MAX]u8 = undefined;
    const r = std.c.realpath(try arena.dupeZ(u8, path), &buf) orelse return path;
    return arena.dupe(u8, std.mem.sliceTo(r, 0));
}

/// The longest path a unix socket can have: sun_path less its NUL.
/// Windows has no such limit that std doesn't already handle.
pub const socket_path_max = if (sys.is_windows)
    std.Io.net.UnixAddress.max_len
else
    @typeInfo(@FieldType(std.c.sockaddr.un, "path")).array.len - 1;

/// Where a socket named `name` in `dir` goes: there, when its path fits
/// in a unix socket address, else /tmp/minipg-$UID/<fallback>, a short
/// private directory. Not on Windows, which needs no fallback.
pub fn socketPath(arena: std.mem.Allocator, dir: []const u8, name: []const u8, fallback: []const u8) ![]const u8 {
    const p = try std.fs.path.join(arena, &.{ dir, name });
    if (sys.is_windows or p.len <= socket_path_max) return p;
    const short = try std.fmt.allocPrint(arena, "/tmp/minipg-{d}", .{std.c.getuid()});
    try makeOwnedPrivateDir(arena, short);
    const q = try std.fs.path.join(arena, &.{ short, fallback });
    if (q.len > socket_path_max) return error.NameTooLong;
    return q;
}

/// Makes `path` a directory only this user can use, refusing one that
/// someone else made first or that is a symlink: /tmp is shared.
fn makeOwnedPrivateDir(arena: std.mem.Allocator, path: []const u8) !void {
    const z = try arena.dupeZ(u8, path);
    _ = std.c.mkdir(z, 0o700);
    const st = try lstatOwner(z);
    if (st.mode & std.c.S.IFMT != std.c.S.IFDIR or st.uid != std.c.getuid()) return error.AccessDenied;
    if (st.mode & 0o777 != 0o700 and std.c.chmod(z, 0o700) != 0) return error.AccessDenied;
}

/// A path's mode and owner, not following a final symlink. statx on
/// Linux, where std.c has no fstatat; fstatat elsewhere (see `key`).
fn lstatOwner(z: [:0]const u8) !struct { mode: u32, uid: u32 } {
    if (builtin.os.tag == .linux) {
        const linux = std.os.linux;
        var st: linux.Statx = undefined;
        if (linux.errno(linux.statx(linux.AT.FDCWD, z, linux.AT.SYMLINK_NOFOLLOW, .{ .TYPE = true, .MODE = true, .UID = true }, &st)) != .SUCCESS) return error.FileNotFound;
        return .{ .mode = st.mode, .uid = st.uid };
    }
    var st: std.c.Stat = undefined;
    if (std.c.fstatat(std.c.AT.FDCWD, z, &st, std.c.AT.SYMLINK_NOFOLLOW) != 0) return error.FileNotFound;
    return .{ .mode = @intCast(st.mode), .uid = @intCast(st.uid) };
}

/// The key for a .pgdb directory: its device and inode, hashed.
pub fn key(arena: std.mem.Allocator, dir: []const u8) ![]const u8 {
    const z = try arena.dupeZ(u8, dir);
    var h: std.hash.Wyhash = .init(0);
    if (sys.is_windows) {
        const id = try sys.fileId(arena, dir);
        h.update(std.mem.asBytes(&id));
        return std.fmt.allocPrint(arena, "{x:0>16}", .{h.final()});
    }
    if (builtin.os.tag == .linux) {
        const linux = std.os.linux;
        var st: linux.Statx = undefined;
        if (linux.errno(linux.statx(linux.AT.FDCWD, z, 0, .{ .INO = true }, &st)) != .SUCCESS) return error.FileNotFound;
        h.update(std.mem.asBytes(&st.dev_major));
        h.update(std.mem.asBytes(&st.dev_minor));
        h.update(std.mem.asBytes(&st.ino));
        return std.fmt.allocPrint(arena, "{x:0>16}", .{h.final()});
    }
    // fstatat, not stat: Zig 0.16's std.c.stat names a private.stat it
    // never declares, so it fails to compile for aarch64-macos.
    var st: std.c.Stat = undefined;
    if (std.c.fstatat(std.c.AT.FDCWD, z, &st, 0) != 0) return error.FileNotFound;
    h.update(std.mem.asBytes(&st.dev));
    h.update(std.mem.asBytes(&st.ino));
    return std.fmt.allocPrint(arena, "{x:0>16}", .{h.final()});
}

/// Creates `path` and its parents, and makes `path` itself mode 0700.
pub fn makePrivateDir(io: std.Io, arena: std.mem.Allocator, path: []const u8) !void {
    try std.Io.Dir.cwd().createDirPath(io, path);
    try chmod(arena, path, 0o700);
}

/// Sets a file's mode on POSIX; a no-op on Windows, which has none.
pub fn chmod(arena: std.mem.Allocator, path: []const u8, mode: u32) !void {
    if (sys.is_windows) return;
    _ = std.c.chmod(try arena.dupeZ(u8, path), @intCast(mode));
}

/// Writes `data` to `path` through a temporary file and a rename, so
/// readers see the old contents or the new, never part.
pub fn writeAtomic(io: std.Io, arena: std.mem.Allocator, path: []const u8, data: []const u8, mode: u32) !void {
    const tmp = try std.fmt.allocPrint(arena, "{s}.tmp{d}", .{ path, sys.pid() });
    const cwd = std.Io.Dir.cwd();
    try cwd.writeFile(io, .{ .sub_path = tmp, .data = data });
    try chmod(arena, tmp, mode);
    cwd.rename(tmp, cwd, path, io) catch |err| {
        cwd.deleteFile(io, tmp) catch {};
        return err;
    };
}

test socketPath {
    if (sys.is_windows) return;
    var arena: std.heap.ArenaAllocator = .init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    try std.testing.expectEqualStrings("/run/x/control.sock", try socketPath(a, "/run/x", "control.sock", "k.sock"));
    const long = "/" ++ "d" ** 120;
    const p = try socketPath(a, long, "control.sock", "k.sock");
    try std.testing.expect(p.len <= socket_path_max);
    try std.testing.expect(std.mem.startsWith(u8, p, "/tmp/minipg-"));
    try std.testing.expect(std.mem.endsWith(u8, p, "/k.sock"));
}

test key {
    var arena: std.heap.ArenaAllocator = .init(std.testing.allocator);
    defer arena.deinit();
    const a = try key(arena.allocator(), ".");
    try std.testing.expectEqual(@as(usize, 16), a.len);
    try std.testing.expectEqualStrings(a, try key(arena.allocator(), "."));
}
