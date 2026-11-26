//! Sizes the server machine's memory from the cluster's settings.
//!
//! h9 copies a process's private memory when it forks, rather than sharing
//! it copy-on-write, so every Postgres backend costs a full copy of the
//! postmaster: about 33 MB on Postgres 18, mostly its executable and
//! libraries. Shared memory (shared_buffers) is shared. A machine of fixed
//! size therefore runs out of memory long before max_connections: a 2 GB
//! one forked about 50 backends. So the machine gets room for the
//! postmaster's own processes, shared memory, and every backend and worker
//! the settings allow.
//!
//! The settings come from postgresql.conf, then postgresql.auto.conf (ALTER
//! SYSTEM), then postgres's command line (`serve -- -c name=value`), as
//! Postgres reads them. Include directives aren't followed.

const std = @import("std");

const mib: u64 = 1 << 20;

/// What one backend or worker process costs: the copy of the postmaster
/// at fork, about 33 MB, plus its own heap and some work_mem.
pub const per_backend: u64 = 40 * mib;
/// The postmaster and its auxiliary processes (checkpointer, background
/// and WAL writers, launchers, I/O workers), h9's own use, and slack.
pub const base: u64 = 384 * mib;
/// Shared memory beyond shared_buffers (lock tables, WAL buffers, ...).
pub const shmem_extra: u64 = 32 * mib;
/// Memory is rounded up to a multiple of this.
pub const granule: u64 = 256 * mib;
/// Never less than the fixed size minipg used before.
pub const min_memory: u64 = 2 << 30;
/// h9's limit.
pub const max_memory: u64 = 32 << 30;

/// The settings that decide how much memory Postgres can use, with
/// Postgres's defaults.
pub const Settings = struct {
    max_connections: u64 = 100,
    shared_buffers: u64 = 128 * mib,
    autovacuum_max_workers: u64 = 3,
    max_worker_processes: u64 = 8,

    /// Applies `name = value`; names Postgres would reject, or values that
    /// don't parse, are ignored, as they'd keep Postgres from starting.
    pub fn set(s: *Settings, name: []const u8, value: []const u8) void {
        if (eq(name, "max_connections")) {
            s.max_connections = parseInt(value) orelse return;
        } else if (eq(name, "shared_buffers")) {
            s.shared_buffers = parseMemory(value) orelse return;
        } else if (eq(name, "autovacuum_max_workers")) {
            s.autovacuum_max_workers = parseInt(value) orelse return;
        } else if (eq(name, "max_worker_processes")) {
            s.max_worker_processes = parseInt(value) orelse return;
        }
    }

    /// Applies a configuration file's text (postgresql.conf syntax).
    pub fn applyFile(s: *Settings, text: []const u8) void {
        var lines = std.mem.splitScalar(u8, text, '\n');
        while (lines.next()) |line| {
            const kv = parseLine(line) orelse continue;
            s.set(kv.name, kv.value);
        }
    }

    /// Applies postgres's command-line options: `-c name=value`,
    /// `-cname=value`, `--name=value`, `-N connections`, `-B buffers`.
    pub fn applyArgs(s: *Settings, args: []const []const u8) void {
        var i: usize = 0;
        while (i < args.len) : (i += 1) {
            const a = args[i];
            const flag: u8, const rest: []const u8 = if (a.len >= 2 and a[0] == '-' and a[1] != '-')
                .{ a[1], a[2..] }
            else if (std.mem.startsWith(u8, a, "--")) {
                if (std.mem.indexOfScalar(u8, a, '=')) |e| s.set(a[2..e], a[e + 1 ..]);
                continue;
            } else continue;
            if (flag != 'c' and flag != 'N' and flag != 'B') continue;
            const v = if (rest.len > 0) rest else blk: {
                if (i + 1 >= args.len) return;
                i += 1;
                break :blk args[i];
            };
            switch (flag) {
                'c' => if (std.mem.indexOfScalar(u8, v, '=')) |e| s.set(v[0..e], v[e + 1 ..]),
                'N' => s.set("max_connections", v),
                'B' => s.set("shared_buffers", v),
                else => unreachable,
            }
        }
    }

    /// The processes that can run at once besides the auxiliary ones in
    /// `base`: client backends, autovacuum workers, and background workers
    /// (parallel query among them).
    pub fn processes(s: Settings) u64 {
        return s.max_connections +| s.autovacuum_max_workers +| s.max_worker_processes;
    }

    /// The machine memory these settings need, unclamped.
    pub fn need(s: Settings) u64 {
        const shmem = s.shared_buffers +| s.shared_buffers / 16 +| shmem_extra;
        return base +| shmem +| s.processes() *| per_backend;
    }

    /// The machine memory to give the server: `need`, rounded up, and
    /// kept between `min_memory` and `max_memory`.
    pub fn memory(s: Settings) u64 {
        const n = s.need();
        const rounded = if (n > max_memory) max_memory else std.mem.alignForward(u64, n, granule);
        return std.math.clamp(rounded, min_memory, max_memory);
    }

    /// The most client connections `max_memory` fits with these
    /// settings' other values.
    pub fn maxFittingConnections(s: Settings) u64 {
        var t = s;
        t.max_connections = 0;
        const fixed = t.need();
        if (fixed >= max_memory) return 0;
        return (max_memory - fixed) / per_backend;
    }
};

/// Reads the cluster's settings: pgdata's postgresql.conf, then
/// postgresql.auto.conf, then postgres's arguments. Missing files leave
/// the defaults, as for a cluster that initdb hasn't written yet.
pub fn read(arena: std.mem.Allocator, io: std.Io, pgdata: []const u8, args: []const []const u8) Settings {
    var s: Settings = .{};
    for ([_][]const u8{ "postgresql.conf", "postgresql.auto.conf" }) |name| {
        const path = std.fs.path.join(arena, &.{ pgdata, name }) catch continue;
        const text = std.Io.Dir.cwd().readFileAlloc(io, path, arena, .limited(4 << 20)) catch continue;
        s.applyFile(text);
    }
    s.applyArgs(args);
    return s;
}

const KeyValue = struct { name: []const u8, value: []const u8 };

/// One postgresql.conf line: `name [=] value [# comment]`, the value
/// optionally in single quotes ('' for a quote inside, which none of the
/// settings read here need).
fn parseLine(raw: []const u8) ?KeyValue {
    const line = std.mem.trim(u8, raw, " \t\r");
    if (line.len == 0 or line[0] == '#') return null;
    var i: usize = 0;
    while (i < line.len and (std.ascii.isAlphanumeric(line[i]) or line[i] == '_' or line[i] == '.')) i += 1;
    if (i == 0) return null;
    const name = line[0..i];
    var rest = std.mem.trimStart(u8, line[i..], " \t");
    if (rest.len > 0 and rest[0] == '=') rest = std.mem.trimStart(u8, rest[1..], " \t");
    if (rest.len == 0) return null;
    if (rest[0] == '\'') {
        const end = std.mem.indexOfScalarPos(u8, rest, 1, '\'') orelse return null;
        return .{ .name = name, .value = rest[1..end] };
    }
    var end: usize = 0;
    while (end < rest.len and rest[end] != '#' and rest[end] != ' ' and rest[end] != '\t') end += 1;
    return .{ .name = name, .value = rest[0..end] };
}

/// Compares setting names as Postgres does: ignoring case, and with the
/// dashes command-line names may use for underscores.
fn eq(name: []const u8, want: []const u8) bool {
    if (name.len != want.len) return false;
    for (name, want) |a, b| {
        const c = if (a == '-') '_' else std.ascii.toLower(a);
        if (c != b) return false;
    }
    return true;
}

fn parseInt(v: []const u8) ?u64 {
    return std.fmt.parseInt(u64, std.mem.trim(u8, v, " \t"), 0) catch null;
}

/// A memory setting in bytes: a number of 8 kB blocks, or a number with
/// a unit (B, kB, MB, GB, TB), as Postgres reads shared_buffers.
fn parseMemory(raw: []const u8) ?u64 {
    const v = std.mem.trim(u8, raw, " \t");
    var i: usize = 0;
    while (i < v.len and std.ascii.isDigit(v[i])) i += 1;
    if (i == 0) return null;
    const n = std.fmt.parseInt(u64, v[0..i], 10) catch return null;
    const unit = std.mem.trim(u8, v[i..], " \t");
    const scale: u64 = if (unit.len == 0)
        8 << 10
    else if (std.mem.eql(u8, unit, "B"))
        1
    else if (std.mem.eql(u8, unit, "kB"))
        1 << 10
    else if (std.mem.eql(u8, unit, "MB"))
        1 << 20
    else if (std.mem.eql(u8, unit, "GB"))
        1 << 30
    else if (std.mem.eql(u8, unit, "TB"))
        1 << 40
    else
        return null;
    return std.math.mul(u64, n, scale) catch null;
}

const testing = std.testing;

test "defaults fit 100 connections" {
    const s: Settings = .{};
    // 384 + 128 + 8 + 32 + 111 * 40 = 4992 MiB, rounded to 5 GiB.
    try testing.expectEqual(@as(u64, 5 << 30), s.memory());
    try testing.expect(s.need() >= s.processes() * 33 * mib + s.shared_buffers);
}

test "small settings keep the old minimum" {
    var s: Settings = .{};
    s.set("max_connections", "10");
    try testing.expectEqual(min_memory, s.memory());
}

test "huge settings are capped at h9's limit" {
    var s: Settings = .{};
    s.set("max_connections", "5000");
    try testing.expectEqual(max_memory, s.memory());
    try testing.expect(s.maxFittingConnections() > 700);
    try testing.expect(s.maxFittingConnections() < 5000);
}

test "postgresql.conf lines" {
    var s: Settings = .{};
    s.applyFile("# max_connections = 7\n" ++
        "#shared_buffers = 1GB\n" ++
        "max_connections = 200\t\t\t# (change requires restart)\n" ++
        "shared_buffers = 256MB\t\t\t# min 128kB\n" ++
        "Autovacuum_Max_Workers 5\n" ++
        "max_worker_processes='16'\n" ++
        "work_mem = 64MB\n");
    try testing.expectEqual(@as(u64, 200), s.max_connections);
    try testing.expectEqual(@as(u64, 256 * mib), s.shared_buffers);
    try testing.expectEqual(@as(u64, 5), s.autovacuum_max_workers);
    try testing.expectEqual(@as(u64, 16), s.max_worker_processes);
    // Later lines win, as postgresql.auto.conf does over postgresql.conf.
    s.applyFile("shared_buffers = '16384'\nmax_connections = bogus\n");
    try testing.expectEqual(@as(u64, 128 * mib), s.shared_buffers);
    try testing.expectEqual(@as(u64, 200), s.max_connections);
}

test "command-line options" {
    var s: Settings = .{};
    s.applyArgs(&.{ "-c", "max_connections=300", "-cshared_buffers=1GB", "--autovacuum-max-workers=4", "-N", "250", "-c", "log_statement=all" });
    try testing.expectEqual(@as(u64, 250), s.max_connections);
    try testing.expectEqual(@as(u64, 1 << 30), s.shared_buffers);
    try testing.expectEqual(@as(u64, 4), s.autovacuum_max_workers);
    s.applyArgs(&.{ "-B", "1024", "-c" });
    try testing.expectEqual(@as(u64, 8 * mib), s.shared_buffers);
}

test "memory units" {
    try testing.expectEqual(@as(?u64, 128 * mib), parseMemory("128MB"));
    try testing.expectEqual(@as(?u64, 128 * mib), parseMemory("16384"));
    try testing.expectEqual(@as(?u64, 2 << 30), parseMemory("2 GB"));
    try testing.expectEqual(@as(?u64, null), parseMemory("2gb"));
    try testing.expectEqual(@as(?u64, null), parseMemory("lots"));
}
