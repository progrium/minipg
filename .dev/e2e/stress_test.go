//go:build stress

package e2e

// The stress suite: crash and durability, load and soak, resource limits,
// and recovery drills. It's slow, so it only builds with -tags stress:
//
//	make stress                         # builds ./minipg, then runs it
//	STRESS_ROUNDS=50 STRESS_SOAK=30m make stress
//
// Knobs (environment):
//
//	STRESS_ROUNDS  crash rounds per kill mode (default 6)
//	STRESS_SOAK    pgbench soak duration (default 2m)
//	STRESS_SCALE   pgbench scale factor (default 10, about 160 MB)
//	STRESS_SEED    random seed, to replay a run (default: the time)
//	STRESS_RUN     a -run pattern, through make (default Stress)
//	STRESS_PGCHECKSUMS=tool  run pg_checksums in a tool machine even where
//	               the host could run the root's own binary
//	MINIPG_E2E_CMD_TIMEOUT  how long one minipg command may run (default 15m)
//
// Every cluster is initialized with --data-checksums, and every check ends
// with amcheck over all btree indexes and the heap, and a pg_checksums pass
// over the stopped cluster.
//
// It runs on Linux, macOS and Windows. The kills that target h9 stubs (the
// stub crash round and TestStressIdleStubDeath) need h9's native backend
// and skip on a hypervisor (KVM, HVF, WHP). A client that waits on a server
// that has stopped answering gives up and fails its test rather than
// hanging the suite.

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
)

var stressRand *rand.Rand

func init() {
	seed := time.Now().UnixNano()
	if s := os.Getenv("STRESS_SEED"); s != "" {
		seed, _ = strconv.ParseInt(s, 10, 64)
	}
	fmt.Fprintf(os.Stderr, "stress: STRESS_SEED=%d\n", seed)
	stressRand = rand.New(rand.NewSource(seed))
}

var randMu sync.Mutex

func rnd(n int) int {
	randMu.Lock()
	defer randMu.Unlock()
	return stressRand.Intn(n)
}

func envInt(name string, def int) int {
	if v, err := strconv.Atoi(os.Getenv(name)); err == nil && v > 0 {
		return v
	}
	return def
}

func envDuration(name string, def time.Duration) time.Duration {
	if v, err := time.ParseDuration(os.Getenv(name)); err == nil && v > 0 {
		return v
	}
	return def
}

// stressEnv is an env whose clusters have data checksums and whose
// background servers don't stop on their own.
func stressEnv(t *testing.T) *env {
	e := newEnv(t)
	e.vars = append(e.vars, "MINIPG_IDLE=3600", "POSTGRES_INITDB_ARGS=--data-checksums")
	return e
}

// start makes sure a background server is running and returns a pool-less
// URL for it.
func (e *env) start(pgdb string) string {
	e.t.Helper()
	e.query(pgdb, "select 1")
	return e.url(pgdb)
}

// serverPid is the running server's pid, from minipg status, which works
// wherever minipg keeps its runtime files.
func (e *env) serverPid(pgdb string) int {
	e.t.Helper()
	out, errs, code := e.run("status", pgdb)
	if code != 0 {
		e.t.Fatalf("status %s: exit %d: %s", pgdb, code, errs)
	}
	for _, line := range strings.Split(out, "\n") {
		if f := strings.Fields(line); len(f) == 2 && f[0] == "pid" {
			if pid, err := strconv.Atoi(f[1]); err == nil {
				return pid
			}
		}
	}
	e.t.Fatalf("no pid in minipg status %s:\n%s", pgdb, out)
	return 0
}

// serverProcs, procRSS and tcpSockets differ by host: procs_*_test.go.

// stubs lists a server's h9 stubs. Only h9's native backend has them; on
// a hypervisor (KVM, HVF, WHP) guest processes aren't host processes, and
// the Linux build's Postgres processes are plain ones.
func stubs(server int) []int {
	var out []int
	for pid, name := range serverProcs(server) {
		if pid != server && strings.Contains(name, "stub") {
			out = append(out, pid)
		}
	}
	return out
}

// waitStopped waits for a killed server's lock to be released.
func (e *env) waitStopped(pgdb string, pgid int) {
	e.t.Helper()
	start := time.Now()
	deadline := start.Add(60 * time.Second)
	for {
		_, _, code := e.run("status", pgdb)
		if code == 3 {
			break
		}
		if time.Now().After(deadline) {
			e.t.Fatalf("the lock on %s is still held 60s after the kill; the server's processes: %v", pgdb, serverProcs(pgid))
		}
		time.Sleep(100 * time.Millisecond)
	}
	if left := serverProcs(pgid); len(left) > 0 {
		e.t.Errorf("the lock is free but the server's processes remain: %v", left)
	}
	e.t.Logf("lock released %v after the kill", time.Since(start).Round(time.Millisecond))
}

// requireStubs skips a test that kills h9 stubs on a server that has none:
// one on a hypervisor (KVM, HVF, WHP), or the Linux build's.
func requireStubs(t *testing.T, server int) {
	t.Helper()
	if len(stubs(server)) == 0 {
		t.Skipf("server %d has no h9 stubs (its host processes: %v); stub kills need h9's native backend", server, serverProcs(server))
	}
}

// Bounds on waits that would otherwise be forever. A client blocked on a
// server that never answers is a hang to report, not to sit through.
const (
	connectTimeout = 30 * time.Second
	txTimeout      = 2 * time.Minute
	opTimeout      = 10 * time.Minute
)

// errHung marks a client that gave up on a server that stopped answering.
var errHung = errors.New("hung")

// stressConnect connects with a timeout.
func stressConnect(ctx context.Context, url string) (*pgx.Conn, error) {
	cfg, err := pgx.ParseConfig(url)
	if err != nil {
		return nil, err
	}
	cfg.ConnectTimeout = connectTimeout
	return pgx.ConnectConfig(ctx, cfg)
}

// hung wraps err in errHung if ctx timed out, rather than being canceled.
func hung(ctx context.Context, what string, err error) error {
	if err != nil && errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return fmt.Errorf("%w: %s: no answer in %v: %v", errHung, what, txTimeout, err)
	}
	return err
}

// waitWithin waits for wait to return, for up to d.
func waitWithin(wait func(), d time.Duration) bool {
	done := make(chan struct{})
	go func() {
		wait()
		close(done)
	}()
	select {
	case <-done:
		return true
	case <-time.After(d):
		return false
	}
}

// finish cancels a workload and waits for its clients, failing the test if
// they don't stop.
func finish(t *testing.T, cancel context.CancelFunc, wait func()) {
	t.Helper()
	cancel()
	if !waitWithin(wait, time.Minute) {
		t.Fatalf("the workload's clients were still running a minute after being canceled")
	}
}

// check runs the integrity checks that work on a running server: amcheck
// on every btree index (with heapallindexed), verify_heapam on every table,
// and Postgres's count of checksum failures.
func (e *env) check(pgdb string) {
	e.t.Helper()
	e.query(pgdb, "create extension if not exists amcheck")
	if got := e.query(pgdb, `select count(*) from (
		select bt_index_check(c.oid, true)
		from pg_index i join pg_class c on c.oid = i.indexrelid join pg_am a on a.oid = c.relam
		where a.amname = 'btree' and c.relpersistence <> 't') s`); got == "" {
		e.t.Errorf("bt_index_check returned nothing")
	}
	if got := e.query(pgdb, `select coalesce(string_agg(format('%s blk %s: %s', c.relname, v.blkno, v.msg), E'\n'), '')
		from pg_class c, verify_heapam(c.oid) v
		where c.relkind in ('r', 'm', 't') and c.relpersistence <> 't'`); got != "" {
		e.t.Errorf("verify_heapam:\n%s", got)
	}
	if got := e.query(pgdb, "select coalesce(sum(checksum_failures), 0) from pg_stat_database"); got != "0" {
		e.t.Errorf("checksum failures: %s", got)
	}
}

// pgChecksums stops the server and runs pg_checksums --check over the
// stopped cluster. On Linux, with an unpacked root in the e2e cache, it runs
// the root's own binary on the host through the root's musl loader.
// Elsewhere (macOS and Windows keep the root where the loader can't run, or
// can't run Linux binaries at all) it runs pg_checksums in a tool machine
// of a helper .pgdb's server: a tool's cwd is the caller's, so it sees the
// stopped cluster as a relative path. STRESS_PGCHECKSUMS=tool forces that
// way on Linux too.
func (e *env) pgChecksums(pgdb string) {
	e.t.Helper()
	e.must("stop", pgdb)
	var out []byte
	var err error
	if loader, root := e.muslRoot(); loader != "" && os.Getenv("STRESS_PGCHECKSUMS") != "tool" {
		c := exec.Command(loader, "--library-path", filepath.Join(root, "lib")+":"+filepath.Join(root, "usr/lib")+":"+filepath.Join(root, "usr/local/lib"),
			filepath.Join(root, "usr/local/bin/pg_checksums"), "--check", "-D", e.path(pgdb+"/pgdata"))
		out, err = c.CombinedOutput()
	} else {
		const helper = "pgchecksums-helper.pgdb"
		if _, serr := os.Stat(e.path(helper)); serr != nil {
			e.must("init", helper)
		}
		stdout, stderr, code := e.run("exec", helper, "--", "pg_checksums", "--check", "-D", filepath.ToSlash(pgdb)+"/pgdata")
		out = []byte(stdout + stderr)
		if code != 0 {
			err = fmt.Errorf("exit %d (in a tool machine)", code)
		}
		e.run("stop", helper)
	}
	if err != nil {
		e.t.Errorf("pg_checksums --check: %v\n%s", err, out)
		return
	}
	if !strings.Contains(string(out), "Bad checksums:  0") {
		e.t.Errorf("pg_checksums --check:\n%s", out)
	}
	e.t.Logf("pg_checksums: %s", strings.Join(strings.Fields(string(out)), " "))
}

// muslRoot finds an unpacked root with a musl loader in the e2e cache, on
// a Linux host, which can run it.
func (e *env) muslRoot() (loader, root string) {
	if runtime.GOOS != "linux" {
		return "", ""
	}
	cache := os.Getenv("MINIPG_E2E_CACHE")
	if cache == "" {
		cache = filepath.Join(os.TempDir(), "minipg-e2e-cache")
	}
	loaders, _ := filepath.Glob(filepath.Join(cache, "minipg", "roots", "*", "lib", "ld-musl-*.so.1"))
	if len(loaders) == 0 {
		return "", ""
	}
	return loaders[0], filepath.Dir(filepath.Dir(loaders[0]))
}

// ---------------------------------------------------------------------------
// Crash and durability

// The workload: writers append rows to a log, each in a transaction that
// also moves money between two accounts. A writer records a sequence
// number as acknowledged only after COMMIT returns. After a crash, every
// acknowledged row must be there with the right contents, at most the one
// in-flight row past it may be, the log has no gaps, and the accounts
// still sum to what they started with.
const (
	nAccounts = 100
	startBal  = 1000
)

const crashSchema = `
create table log (writer int, seq bigint, reps int, payload text, primary key (writer, seq));
create index log_payload on log (md5(payload));
create table accounts (id int primary key, balance bigint not null);
insert into accounts select g, 1000 from generate_series(1, 100) g;
create table bulk (id bigserial primary key, writer int, data bytea);
`

type crashWorkload struct {
	writers int
	acked   []atomic.Int64 // per writer: the last acknowledged seq
	errs    []error
}

// run writes until ctx is done or the connection breaks. Each writer
// continues from the seq it finds in the table.
func (w *crashWorkload) run(ctx context.Context, url string) (wait func()) {
	var wg sync.WaitGroup
	w.errs = make([]error, w.writers+1)
	for i := 0; i < w.writers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			w.errs[id] = w.writer(ctx, url, id)
		}(i)
	}
	// A checkpointer and a bulk writer, so kills land during checkpoints
	// and big multi-page writes too.
	wg.Add(1)
	go func() {
		defer wg.Done()
		w.errs[w.writers] = w.checkpointer(ctx, url)
	}()
	return wg.Wait
}

// checkpointer checkpoints and writes big rows until ctx is done or the
// connection breaks. Only a hang is its error to report.
func (w *crashWorkload) checkpointer(ctx context.Context, url string) error {
	conn, err := stressConnect(ctx, url)
	if err != nil {
		return nil
	}
	defer conn.Close(context.Background())
	for ctx.Err() == nil {
		time.Sleep(time.Duration(200+rnd(800)) * time.Millisecond)
		tctx, cancel := context.WithTimeout(ctx, txTimeout)
		_, err := conn.Exec(tctx, "checkpoint")
		if err == nil {
			_, err = conn.Exec(tctx, "insert into bulk (writer, data) select -1, decode(repeat(md5(random()::text), 2000), 'hex') from generate_series(1, 20)")
		}
		if err == nil {
			conn.Exec(tctx, "delete from bulk where id in (select id from bulk order by id limit 10)")
		}
		err = hung(tctx, "checkpointer", err)
		cancel()
		if errors.Is(err, errHung) {
			return err
		}
		if err != nil {
			return nil
		}
	}
	return nil
}

func (w *crashWorkload) writer(ctx context.Context, url string, id int) error {
	conn, err := stressConnect(ctx, url)
	if err != nil {
		return err
	}
	defer conn.Close(context.Background())
	var seq int64
	qctx, cancel := context.WithTimeout(ctx, txTimeout)
	err = conn.QueryRow(qctx, "select coalesce(max(seq), 0) from log where writer = $1", id).Scan(&seq)
	err = hung(qctx, "reading the last seq", err)
	cancel()
	if err != nil {
		return err
	}
	w.acked[id].Store(seq)
	for ctx.Err() == nil {
		seq++
		reps := 1 + rnd(4)
		if rnd(20) == 0 {
			reps = 200 + rnd(400) // a TOASTed row, 6 to 19 KB
		}
		a, b := 1+rnd(nAccounts), 1+rnd(nAccounts)
		if a > b {
			a, b = b, a
		}
		amt := rnd(50)
		if err := w.transact(ctx, conn, id, seq, reps, a, b, amt); err != nil {
			return err
		}
		w.acked[id].Store(seq)
	}
	return nil
}

// transact commits one writer's transaction, giving up (errHung) on a
// server that doesn't answer within txTimeout.
func (w *crashWorkload) transact(ctx context.Context, conn *pgx.Conn, id int, seq int64, reps, a, b, amt int) error {
	tctx, cancel := context.WithTimeout(ctx, txTimeout)
	defer cancel()
	tx, err := conn.Begin(tctx)
	if err != nil {
		return hung(tctx, fmt.Sprintf("seq %d: begin", seq), err)
	}
	batch := &pgx.Batch{}
	batch.Queue("insert into log values ($1::int, $2::bigint, $3::int, repeat(md5($1::int::text || ':' || $2::bigint::text), $3::int))", id, seq, reps)
	batch.Queue("update accounts set balance = balance - $2 where id = $1", a, amt)
	batch.Queue("update accounts set balance = balance + $2 where id = $1", b, amt)
	if err := tx.SendBatch(tctx, batch).Close(); err != nil {
		tx.Rollback(context.Background())
		return hung(tctx, fmt.Sprintf("seq %d: batch", seq), err)
	}
	return hung(tctx, fmt.Sprintf("seq %d: commit", seq), tx.Commit(tctx))
}

// checkErrs fails the test for a writer that stopped for any reason but
// its connection going away (or its context ending), and for any client
// that gave up on a hung server.
func (w *crashWorkload) checkErrs(t *testing.T) {
	t.Helper()
	for i, err := range w.errs {
		var pe *pgconn.PgError
		switch {
		case errors.Is(err, errHung):
			t.Errorf("writer %d: %v", i, err)
		case errors.As(err, &pe) && !strings.HasPrefix(pe.Code, "57") && !strings.HasPrefix(pe.Code, "08"):
			t.Errorf("writer %d stopped with %v", i, err)
		}
	}
}

// verify checks the workload's invariants against the recovered cluster.
func (w *crashWorkload) verify(t *testing.T, e *env, pgdb string) (rows int64) {
	t.Helper()
	conn := e.connect(e.start(pgdb))
	defer conn.Close(context.Background())
	ctx, cancel := context.WithTimeout(context.Background(), opTimeout)
	defer cancel()
	for id := 0; id < w.writers; id++ {
		var n, max int64
		if err := conn.QueryRow(ctx, "select count(*), coalesce(max(seq), 0) from log where writer = $1", id).Scan(&n, &max); err != nil {
			t.Fatal(err)
		}
		acked := w.acked[id].Load()
		switch {
		case max < acked:
			t.Errorf("LOST COMMITS: writer %d had seq %d acknowledged, but the table ends at %d", id, acked, max)
		case max > acked+1:
			t.Errorf("writer %d: table has seq %d, but only %d was acknowledged and one was in flight", id, max, acked)
		}
		if n != max {
			t.Errorf("writer %d: %d rows for seqs 1..%d (gaps or duplicates)", id, n, max)
		}
		rows += n
	}
	var bad int64
	if err := conn.QueryRow(ctx, "select count(*) from log where payload <> repeat(md5(writer::text || ':' || seq::text), reps)").Scan(&bad); err != nil {
		t.Fatal(err)
	}
	if bad != 0 {
		t.Errorf("%d log rows have the wrong payload", bad)
	}
	// The index agrees with the heap, row for row.
	conn.Exec(ctx, "set enable_seqscan = off")
	var viaIndex int64
	if err := conn.QueryRow(ctx, "select count(*) from log where md5(payload) is not null").Scan(&viaIndex); err != nil {
		t.Fatal(err)
	}
	conn.Exec(ctx, "reset enable_seqscan")
	if viaIndex != rows {
		t.Errorf("log_payload index sees %d rows, the writers %d", viaIndex, rows)
	}
	var sum, cnt int64
	if err := conn.QueryRow(ctx, "select sum(balance), count(*) from accounts").Scan(&sum, &cnt); err != nil {
		t.Fatal(err)
	}
	if sum != nAccounts*startBal || cnt != nAccounts {
		t.Errorf("accounts: %d rows summing to %d, want %d summing to %d", cnt, sum, nAccounts, nAccounts*startBal)
	}
	return rows
}

// A kill is one way to crash a running server. Host kills SIGKILL host
// processes; guest kills SIGKILL a Postgres process inside the machine
// (through COPY TO PROGRAM, as the superuser), which is how a Postgres
// process dies on a real host, and which h9 has to deliver.
type kill struct {
	name string
	// inPlace kills leave the server up: Postgres reinitializes its
	// processes and recovers without restarting.
	inPlace bool
	// needsStubs kills need h9's native backend, whose guest processes
	// are host processes (stubs).
	needsStubs bool
	// do kills; fresh lists the h9 stubs started since the workload began
	// (its backends, busy writing).
	do func(t *testing.T, e *env, pgdb string, server int, fresh []int) string
}

func hostKill(t *testing.T, pids ...int) string {
	for _, pid := range pids {
		if err := killPid(pid); err != nil {
			t.Logf("kill %d: %v", pid, err)
		}
	}
	return fmt.Sprint(pids)
}

// guestKill SIGKILLs the guest processes the query names, from inside the
// machine.
func guestKill(t *testing.T, e *env, pgdb, pidQuery string) string {
	t.Helper()
	pids := e.query(pgdb, pidQuery)
	if pids == "" {
		t.Fatalf("no process to kill: %s", pidQuery)
	}
	pids = strings.Join(strings.Fields(pids), " ")
	// The kill takes this backend down too if it's among them; that's fine.
	e.run("exec", pgdb, "--", "psql", "-X", "-c", fmt.Sprintf("copy (select 1) to program 'kill -9 %s'", pids))
	return "guest " + pids
}

var kills = []kill{
	// The server and every h9 stub at once: everything minipg runs dies in
	// the same instant, the closest thing here to a power cut (short of
	// the host's own page cache).
	{"all", false, false, func(t *testing.T, e *env, pgdb string, server int, fresh []int) string {
		pids := []int{server}
		for pid := range serverProcs(server) {
			if pid != server {
				pids = append(pids, pid)
			}
		}
		return hostKill(t, pids...)
	}},
	// Only the minipg server process, leaving h9's stubs to notice.
	{"server", false, false, func(t *testing.T, e *env, pgdb string, server int, fresh []int) string {
		return hostKill(t, server)
	}},
	// The h9 stub of one busy backend, as the host's OOM killer might: the
	// guest process vanishes under h9, which reports it to Postgres as an
	// exit, and Postgres recovers in place.
	{"stub", true, true, func(t *testing.T, e *env, pgdb string, server int, fresh []int) string {
		if len(fresh) == 0 {
			t.Fatalf("no new h9 stubs under server %d: %v", server, serverProcs(server))
		}
		return hostKill(t, fresh[rnd(len(fresh))])
	}},
	// The postmaster, from inside the machine; its children follow.
	{"postmaster", false, false, func(t *testing.T, e *env, pgdb string, server int, fresh []int) string {
		return guestKill(t, e, pgdb, "select split_part(pg_read_file('postmaster.pid'), E'\\n', 1)")
	}},
	// Every client backend that's writing: Postgres reinitializes and
	// recovers in place.
	{"backend", true, false, func(t *testing.T, e *env, pgdb string, server int, fresh []int) string {
		return guestKill(t, e, pgdb, "select string_agg(pid::text, ' ') from pg_stat_activity where backend_type = 'client backend' and pid <> pg_backend_pid() and datname = current_database()")
	}},
	// The WAL writer and the checkpointer.
	{"walwriter", true, false, func(t *testing.T, e *env, pgdb string, server int, fresh []int) string {
		return guestKill(t, e, pgdb, "select string_agg(pid::text, ' ') from pg_stat_activity where backend_type in ('walwriter', 'checkpointer')")
	}},
}

func TestStressCrash(t *testing.T) {
	rounds := envInt("STRESS_ROUNDS", 6)
	for _, k := range kills {
		t.Run(k.name, func(t *testing.T) {
			e := stressEnv(t)
			e.must("init", "crash.pgdb")
			e.query("crash.pgdb", crashSchema)
			w := &crashWorkload{writers: 8, acked: make([]atomic.Int64, 8)}
			var total int64
			for r := 1; r <= rounds; r++ {
				url := e.start("crash.pgdb")
				server := e.serverPid("crash.pgdb")
				if k.needsStubs && r == 1 {
					requireStubs(t, server)
				}
				old := map[int]bool{}
				for _, pid := range stubs(server) {
					old[pid] = true
				}
				ctx, cancel := context.WithCancel(context.Background())
				wait := w.run(ctx, url)
				time.Sleep(time.Duration(1000+rnd(4000)) * time.Millisecond)
				var fresh []int
				for _, pid := range stubs(server) {
					if !old[pid] {
						fresh = append(fresh, pid)
					}
				}
				what := k.do(t, e, "crash.pgdb", server, fresh)
				before := total
				if k.inPlace {
					// The clients see their connections drop; the server
					// stays up.
					if !waitWithin(wait, 90*time.Second) {
						t.Errorf("round %d: writers still running 90s after the kill; their connections should have dropped\n%s", r, e.hangReport("crash.pgdb"))
					}
					finish(t, cancel, wait)
					waitFor(t, "Postgres to recover in place", 60*time.Second, func() bool {
						_, _, code := e.run("crash.pgdb", "select 1")
						return code == 0
					})
					if now := e.serverPid("crash.pgdb"); now != server {
						t.Errorf("round %d: the server restarted (%d, then %d); Postgres should have recovered in place", r, server, now)
					}
				} else {
					e.waitStopped("crash.pgdb", server)
					finish(t, cancel, wait)
				}
				w.checkErrs(t)
				failed := t.Failed()
				total = w.verify(t, e, "crash.pgdb")
				t.Logf("round %d: killed %s %s; %d rows survive (+%d)", r, k.name, what, total, total-before)
				if total == before {
					t.Errorf("round %d: no writes landed", r)
				}
				// Lost or torn data ends the test; a writer's odd error
				// doesn't.
				if t.Failed() && !failed {
					t.Logf("server log:\n%s", tail(e.must("logs", "crash.pgdb"), 60))
					return
				}
			}
			e.check("crash.pgdb")
			if k.inPlace {
				if logs := e.must("logs", "crash.pgdb"); !strings.Contains(logs, "terminating any other active server processes") {
					t.Errorf("no in-place crash recovery in the log")
				}
			}
			e.pgChecksums("crash.pgdb")
		})
	}
}

// TestStressIdleStubDeath kills the h9 stub of a backend that's idle in a
// transaction, holding a row lock, as the host's OOM killer might. Postgres
// has to learn that the process died, run crash recovery, and free the
// lock; a process h9 never reports as dead holds it forever.
func TestStressIdleStubDeath(t *testing.T) {
	e := stressEnv(t)
	e.must("init", "idle.pgdb")
	url := e.start("idle.pgdb")
	e.query("idle.pgdb", "create table r (id int primary key, v int); insert into r values (1, 0)")
	server := e.serverPid("idle.pgdb")
	requireStubs(t, server)
	old := map[int]bool{}
	for _, pid := range stubs(server) {
		old[pid] = true
	}
	ctx, cancel := context.WithTimeout(context.Background(), opTimeout)
	defer cancel()
	holder := e.connect(url)
	defer holder.Close(ctx)
	if _, err := holder.Exec(ctx, "begin; update r set v = v + 1 where id = 1"); err != nil {
		t.Fatal(err)
	}
	var fresh []int
	for _, pid := range stubs(server) {
		if !old[pid] {
			fresh = append(fresh, pid)
		}
	}
	if len(fresh) != 1 {
		t.Fatalf("want one new stub for the holder's backend, got %v", fresh)
	}
	hostKill(t, fresh[0])
	// Within a minute, someone else can take the lock.
	other := e.connect(url)
	defer other.Close(ctx)
	other.Exec(ctx, "set lock_timeout = '60s'")
	start := time.Now()
	if _, err := other.Exec(ctx, "update r set v = v + 1 where id = 1"); err != nil {
		t.Errorf("the dead backend's row lock is still held %v after its h9 stub was killed: %v", time.Since(start).Round(time.Second), err)
	} else {
		t.Logf("lock freed %v after the kill", time.Since(start).Round(time.Millisecond))
	}
}

// TestStressCrashDuringBulkLoad kills the server group in the middle of
// large single transactions (COPY-sized inserts and an index build), which
// write many WAL segments: none of the killed transactions may be partly
// visible, and every committed one must be whole.
func TestStressCrashDuringBulkLoad(t *testing.T) {
	e := stressEnv(t)
	e.must("init", "bulk.pgdb")
	e.query("bulk.pgdb", "create table big (batch int, i int, s text)")
	committed := 0
	for r := 1; r <= envInt("STRESS_ROUNDS", 6); r++ {
		url := e.start("bulk.pgdb")
		server := e.serverPid("bulk.pgdb")
		conn := e.connect(url)
		ctx, cancel := context.WithTimeout(context.Background(), opTimeout)
		// One batch that commits, then one that's killed partway.
		if _, err := conn.Exec(ctx, "insert into big select $1, g, repeat('y', 100) from generate_series(1, 200000) g", r*2); err != nil {
			t.Fatal(err)
		}
		committed++
		done := make(chan error, 1)
		go func() {
			_, err := conn.Exec(ctx, "insert into big select $1, g, repeat('z', 100) from generate_series(1, 2000000) g", r*2+1)
			done <- err
		}()
		time.Sleep(time.Duration(500+rnd(2500)) * time.Millisecond)
		kills[0].do(t, e, "bulk.pgdb", server, nil)
		select {
		case err := <-done:
			if err == nil {
				t.Logf("round %d: the big insert finished before the kill", r)
				committed++
			}
		case <-time.After(2 * time.Minute):
			t.Fatalf("round %d: the big insert's client still waiting 2m after the kill", r)
		}
		cancel()
		conn.Close(context.Background())
		e.waitStopped("bulk.pgdb", server)
		got := e.query("bulk.pgdb", "select string_agg(format('%s:%s', batch, n), ' ' order by batch) from (select batch, count(*) n from big group by batch) s")
		for _, f := range strings.Fields(got) {
			b, n, _ := strings.Cut(f, ":")
			bi, _ := strconv.Atoi(b)
			want := "200000"
			if bi%2 == 1 {
				want = "2000000"
			}
			if n != want {
				t.Errorf("round %d: batch %s has %s rows, want %s (a torn transaction)", r, b, n, want)
			}
		}
		t.Logf("round %d: %s", r, got)
	}
	e.query("bulk.pgdb", "create index on big (batch, i)")
	e.check("bulk.pgdb")
	e.pgChecksums("bulk.pgdb")
}

// TestStressCrashCopy takes a crash-consistent copy of a .pgdb: after a
// kill, the directory is copied before anything opens it, and the copy
// must open and hold every acknowledged commit, as a filesystem snapshot
// or a backup of a crashed host would.
func TestStressCrashCopy(t *testing.T) {
	e := stressEnv(t)
	e.must("init", "src.pgdb")
	e.query("src.pgdb", crashSchema)
	w := &crashWorkload{writers: 4, acked: make([]atomic.Int64, 4)}
	url := e.start("src.pgdb")
	server := e.serverPid("src.pgdb")
	ctx, cancel := context.WithCancel(context.Background())
	wait := w.run(ctx, url)
	time.Sleep(3 * time.Second)
	kills[0].do(t, e, "src.pgdb", server, nil)
	e.waitStopped("src.pgdb", server)
	finish(t, cancel, wait)
	w.checkErrs(t)
	if err := copyDir(e.path("src.pgdb"), e.path("copy.pgdb")); err != nil {
		t.Fatalf("copying src.pgdb: %v", err)
	}
	rows := w.verify(t, e, "copy.pgdb")
	t.Logf("the copy has %d rows", rows)
	e.check("copy.pgdb")
	// The original and the copy recover to the same thing.
	if a, b := e.query("src.pgdb", "select md5(string_agg(payload, '' order by writer, seq)) from log"),
		e.query("copy.pgdb", "select md5(string_agg(payload, '' order by writer, seq)) from log"); a != b {
		t.Errorf("the original and the copy differ: %s %s", a, b)
	}
}

// ---------------------------------------------------------------------------
// Load and soak

// pgbench runs pgbench in a tool machine against the server.
// A run that outlives its -T by three minutes has hung: it's killed, and
// the error says what the server and the host's sockets were doing.
func (e *env) pgbench(pgdb string, args ...string) (string, error) {
	limit := 3 * time.Minute
	for i, a := range args {
		if a == "-T" && i+1 < len(args) {
			n, _ := strconv.Atoi(args[i+1])
			limit += time.Duration(n) * time.Second
		}
	}
	c := e.cmd("", nil, append([]string{"exec", pgdb, "--", "pgbench"}, args...)...)
	var out strings.Builder
	var mu sync.Mutex
	w := writerFunc(func(b []byte) (int, error) { mu.Lock(); defer mu.Unlock(); return out.Write(b) })
	c.Stdout, c.Stderr = w, w
	if err := c.Start(); err != nil {
		return "", err
	}
	done := make(chan error, 1)
	go func() { done <- c.Wait() }()
	select {
	case err := <-done:
		mu.Lock()
		defer mu.Unlock()
		return out.String(), err
	case <-time.After(limit):
	}
	diag := e.hangReport(pgdb)
	c.Process.Kill()
	<-done
	mu.Lock()
	defer mu.Unlock()
	return out.String(), fmt.Errorf("pgbench %v hung (still running after %v)\n%s", args, limit, diag)
}

type writerFunc func([]byte) (int, error)

func (f writerFunc) Write(b []byte) (int, error) { return f(b) }

// hangReport describes a server's client backends and the host's TCP
// sockets on its port, with their unread bytes: data sitting in a socket
// that a waiting process never reads is a lost wakeup.
func (e *env) hangReport(pgdb string) string {
	var b strings.Builder
	out, _, _ := e.run("exec", pgdb, "--", "psql", "-X", "-c",
		"select pid, state, wait_event, client_port, now() - state_change as for, left(query, 60) as query from pg_stat_activity where backend_type = 'client backend'")
	b.WriteString(out)
	b.WriteString(tcpSockets(e.status(pgdb).port))
	return b.String()
}

// pgbenchConsistent checks pgbench's own invariant: every transaction adds
// the same delta to one account, one teller, and one branch, so the three
// sums agree, and history has one row per transaction.
func (e *env) pgbenchConsistent(pgdb string) {
	e.t.Helper()
	got := e.query(pgdb, `select (select sum(abalance) from pgbench_accounts) || ' ' ||
		(select sum(tbalance) from pgbench_tellers) || ' ' ||
		(select sum(bbalance) from pgbench_branches) || ' ' ||
		(select coalesce(sum(delta), 0) from pgbench_history)`)
	f := strings.Fields(got)
	if len(f) != 4 || f[0] != f[1] || f[1] != f[2] || f[2] != f[3] {
		e.t.Errorf("pgbench balances disagree (accounts tellers branches history): %s", got)
	}
}

func TestStressSoak(t *testing.T) {
	soak := envDuration("STRESS_SOAK", 2*time.Minute)
	scale := envInt("STRESS_SCALE", 10)
	e := stressEnv(t)
	e.must("init", "soak.pgdb")
	e.start("soak.pgdb")
	if out, err := e.pgbench("soak.pgdb", "-i", "-q", "-s", strconv.Itoa(scale)); err != nil {
		t.Fatalf("pgbench -i: %v\n%s", err, out)
	}
	// The server's and its stubs' RSS, summed: shared memory counts once
	// per stub, so it overstates, but its drift over a soak is the signal.
	rss := func() int64 {
		var total int64
		for pid := range serverProcs(e.serverPid("soak.pgdb")) {
			total += procRSS(pid)
		}
		return total / 1024
	}
	size := func() string {
		return e.query("soak.pgdb", "select pg_size_pretty(pg_database_size(current_database()))")
	}
	t.Logf("before: %s, RSS %d MB", size(), rss())
	// -n everywhere: without it, every pgbench run truncates
	// pgbench_history, and the history check below needs all of it.
	// Mixed load: the TPC-B-like script, plus select-only clients, with
	// connections churned (-C) on a few.
	var wg sync.WaitGroup
	results := make([]string, 3)
	errs := make([]error, 3)
	secs := strconv.Itoa(int(soak.Seconds()))
	runs := [][]string{
		{"-n", "-c", "6", "-j", "2", "-T", secs, "-P", "30", "-r"},
		{"-n", "-S", "-c", "4", "-j", "2", "-T", secs},
		{"-n", "-C", "-c", "2", "-T", secs},
	}
	for i, a := range runs {
		wg.Add(1)
		go func(i int, a []string) {
			defer wg.Done()
			results[i], errs[i] = e.pgbench("soak.pgdb", a...)
		}(i, a)
	}
	peak := int64(0)
	stop := make(chan struct{})
	go func() {
		for {
			select {
			case <-stop:
				return
			case <-time.After(10 * time.Second):
				if r := rss(); r > peak {
					peak = r
				}
			}
		}
	}()
	wg.Wait()
	close(stop)
	for i, out := range results {
		if errs[i] != nil {
			t.Errorf("pgbench %v: %v\n%s", runs[i], errs[i], out)
			continue
		}
		for _, l := range strings.Split(out, "\n") {
			if strings.HasPrefix(l, "tps") || strings.HasPrefix(l, "number of failed") || strings.HasPrefix(l, "latency average") || strings.HasPrefix(l, "progress") {
				t.Logf("pgbench %d: %s", i, l)
			}
			if strings.HasPrefix(l, "number of failed transactions") && !strings.HasPrefix(l, "number of failed transactions: 0 ") {
				t.Errorf("pgbench %v: %s", runs[i], l)
			}
		}
	}
	t.Logf("after: %s, RSS %d MB (peak %d MB)", size(), rss(), peak)
	e.pgbenchConsistent("soak.pgdb")
	e.check("soak.pgdb")
	// A soak that ends in a crash: kill the group mid-pgbench and check the
	// balances still agree after recovery.
	server := e.serverPid("soak.pgdb")
	go e.pgbench("soak.pgdb", "-n", "-c", "6", "-j", "2", "-T", "60")
	time.Sleep(10 * time.Second)
	kills[0].do(t, e, "soak.pgdb", server, nil)
	e.waitStopped("soak.pgdb", server)
	e.pgbenchConsistent("soak.pgdb")
	e.check("soak.pgdb")
	e.pgChecksums("soak.pgdb")
}

// TestStressLostUpdates hammers one row from many connections: every
// increment must land exactly once.
func TestStressLostUpdates(t *testing.T) {
	e := stressEnv(t)
	e.must("init", "ctr.pgdb")
	e.query("ctr.pgdb", "create table ctr (id int primary key, n bigint); insert into ctr values (1, 0)")
	url := e.start("ctr.pgdb")
	const clients, each = 16, 300
	var wg sync.WaitGroup
	var failed atomic.Int64
	for i := 0; i < clients; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			conn, err := connectErr(url)
			if err != nil {
				t.Errorf("connect: %v", err)
				failed.Add(each)
				return
			}
			defer conn.Close(context.Background())
			ctx, cancel := context.WithTimeout(context.Background(), opTimeout)
			defer cancel()
			for j := 0; j < each; j++ {
				if _, err := conn.Exec(ctx, "update ctr set n = n + 1 where id = 1"); err != nil {
					if ctx.Err() != nil {
						t.Errorf("increments still running after %v: %v", opTimeout, err)
						failed.Add(int64(each - j))
						return
					}
					failed.Add(1)
				}
			}
		}()
	}
	wg.Wait()
	want := clients*each - int(failed.Load())
	if got := e.query("ctr.pgdb", "select n from ctr"); got != strconv.Itoa(want) {
		t.Errorf("counter = %s, want %d (%d failed)", got, want, failed.Load())
	}
	// Serializable transfers between accounts, retried on serialization
	// failures: the total never changes.
	e.query("ctr.pgdb", "create table acct (id int primary key, bal int); insert into acct select g, 100 from generate_series(1, 10) g")
	var retries atomic.Int64
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			conn, err := connectErr(url)
			if err != nil {
				t.Errorf("connect: %v", err)
				return
			}
			defer conn.Close(context.Background())
			ctx, cancel := context.WithTimeout(context.Background(), opTimeout)
			defer cancel()
			for j := 0; j < 100; j++ {
				for {
					err := pgx.BeginTxFunc(ctx, conn, pgx.TxOptions{IsoLevel: pgx.Serializable}, func(tx pgx.Tx) error {
						a, b := 1+rnd(10), 1+rnd(10)
						var bal int
						if err := tx.QueryRow(ctx, "select bal from acct where id = $1", a).Scan(&bal); err != nil {
							return err
						}
						if bal < 5 {
							return nil
						}
						if _, err := tx.Exec(ctx, "update acct set bal = bal - 5 where id = $1", a); err != nil {
							return err
						}
						_, err := tx.Exec(ctx, "update acct set bal = bal + 5 where id = $1", b)
						return err
					})
					if err == nil {
						break
					}
					if !strings.Contains(err.Error(), "40001") && !strings.Contains(err.Error(), "40P01") {
						t.Errorf("transfer: %v", err)
						return
					}
					retries.Add(1)
				}
			}
		}()
	}
	wg.Wait()
	if got := e.query("ctr.pgdb", "select sum(bal) || ' ' || min(bal) from acct"); !strings.HasPrefix(got, "1000 ") || strings.HasPrefix(strings.Fields(got)[1], "-") {
		t.Errorf("serializable transfers: sum and min %s, want sum 1000 and nothing negative", got)
	}
	t.Logf("%d serialization retries", retries.Load())
}

// ---------------------------------------------------------------------------
// Resource limits

// TestStressConnections exhausts max_connections: extra clients are
// refused cleanly, and the server keeps serving the ones it has and new
// ones once they leave.
func TestStressConnections(t *testing.T) {
	e := stressEnv(t)
	e.must("init", "conn.pgdb")
	url := e.start("conn.pgdb")
	max, _ := strconv.Atoi(e.query("conn.pgdb", "show max_connections"))
	reserved, _ := strconv.Atoi(e.query("conn.pgdb", "show superuser_reserved_connections"))
	var conns []*pgx.Conn
	var mu sync.Mutex
	var wg sync.WaitGroup
	var refused, forkFailed atomic.Int64
	var other []string
	// Eight at a time: past the limit, not a fork-rate test.
	sem := make(chan struct{}, 8)
	start := time.Now()
	for i := 0; i < max+20; i++ {
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			c, err := connectCapped(url)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				switch {
				case strings.Contains(err.Error(), "too many clients") || strings.Contains(err.Error(), "remaining connection slots"):
					refused.Add(1)
				case strings.Contains(err.Error(), "invalid body length"):
					// The postmaster couldn't fork a backend, and said so
					// in a bare protocol-2 message ("Ecould not fork...").
					forkFailed.Add(1)
				default:
					other = append(other, err.Error())
				}
				return
			}
			conns = append(conns, c)
		}()
	}
	wg.Wait()
	t.Logf("%d connection attempts took %v", max+20, time.Since(start).Round(time.Millisecond))
	t.Logf("max_connections %d (%d reserved): %d connected, %d refused, %d failed to fork", max, reserved, len(conns), refused.Load(), forkFailed.Load())
	if n := forkFailed.Load(); n > 0 {
		logs := e.must("logs", "conn.pgdb")
		var why string
		for _, l := range strings.Split(logs, "\n") {
			if strings.Contains(l, "could not fork") {
				why = l
			}
		}
		t.Errorf("the server couldn't fork a backend for %d clients with only %d connected, under max_connections %d: %s", n, len(conns), max, why)
	}
	if len(other) > 0 {
		t.Errorf("%d connections failed some other way, first: %s", len(other), other[0])
	}
	if refused.Load() == 0 && forkFailed.Load() == 0 {
		t.Errorf("no connection was refused past max_connections")
	}
	// Everyone connected can still work.
	for _, c := range conns {
		var one int
		qctx, cancel := context.WithTimeout(context.Background(), connectTimeout)
		err := c.QueryRow(qctx, "select 1").Scan(&one)
		cancel()
		if err != nil {
			t.Errorf("a connected client: %v", err)
			break
		}
	}
	for _, c := range conns {
		c.Close(context.Background())
	}
	waitFor(t, "connections to drain", 30*time.Second, func() bool {
		c, err := connectErr(url)
		if err != nil {
			return false
		}
		c.Close(context.Background())
		return true
	})
	if st := e.status("conn.pgdb"); !st.running {
		t.Errorf("server not running after the connection storm")
	}
}

// connectCapped connects with a cap on message size: a postmaster that
// can't fork answers with a bare protocol-2 error, whose text pgx would
// read as a gigabyte-long message and allocate.
func connectCapped(url string) (*pgx.Conn, error) {
	cfg, err := pgx.ParseConfig(url)
	if err != nil {
		return nil, err
	}
	cfg.BuildFrontend = func(r io.Reader, w io.Writer) *pgproto3.Frontend {
		f := pgproto3.NewFrontend(r, w)
		f.SetMaxBodyLen(16 << 20)
		return f
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	return pgx.ConnectConfig(ctx, cfg)
}

// TestStressMemory runs queries that want far more memory than work_mem,
// so they spill to disk, and one that builds a big value in memory.
func TestStressMemory(t *testing.T) {
	e := stressEnv(t)
	e.must("init", "mem.pgdb")
	e.start("mem.pgdb")
	// A sort and a hash aggregate over 5M rows with 1 MB of work_mem.
	got := lastLine(e.query("mem.pgdb", `set work_mem = '1MB';
		select count(*) from (select g, md5(g::text) s from generate_series(1, 5000000) g order by s) x`))
	if got != "5000000" {
		t.Errorf("spilled sort: %q", got)
	}
	got = lastLine(e.query("mem.pgdb", `set work_mem = '1MB';
		select count(*) from (select md5(g::text), count(*) from generate_series(1, 3000000) g group by 1) x`))
	if got != "3000000" {
		t.Errorf("spilled hash aggregate: %q", got)
	}
	// A 200 MB value, stored and read back.
	e.query("mem.pgdb", "create table blob (b text); insert into blob select repeat(md5('x'), 200 * 1024 * 1024 / 32)")
	if got := e.query("mem.pgdb", "select length(b) from blob"); got != strconv.Itoa(200*1024*1024) {
		t.Errorf("200 MB value: length %q", got)
	}
	// Past Postgres's 1 GB value limit: a clean error, and a live server.
	_, errs, code := e.run("exec", "mem.pgdb", "--", "psql", "-X", "-c", "select length(repeat(md5('x'), 40000000))")
	if code == 0 || !strings.Contains(errs, "out of memory") && !strings.Contains(errs, "too large") && !strings.Contains(errs, "exceeds") {
		t.Errorf("a 1.2 GB value: exit %d %q", code, errs)
	}
	if got := e.query("mem.pgdb", "select 1"); got != "1" {
		t.Errorf("server after a failed allocation: %q", got)
	}
	e.check("mem.pgdb")
}

// TestStressDiskFull fills the disk under a running server: Postgres must
// fail cleanly (or PANIC and recover), never corrupt, and come back once
// there's space. It needs a small filesystem, so it mounts a tmpfs, which
// takes root; it's skipped otherwise.
func TestStressDiskFull(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("needs root to mount a small tmpfs")
	}
	e := stressEnv(t)
	mnt := e.path("small")
	os.MkdirAll(mnt, 0o755)
	if out, err := exec.Command("mount", "-t", "tmpfs", "-o", "size=300m", "tmpfs", mnt).CombinedOutput(); err != nil {
		t.Skipf("mount tmpfs: %v %s", err, out)
	}
	t.Cleanup(func() { exec.Command("umount", "-l", mnt).Run() })
	// .pgdbs live in the test dir; this one lives on the tmpfs.
	e.must("init", "small/full.pgdb")
	e.query("small/full.pgdb", "create table keep (i int primary key, s text); insert into keep select g, md5(g::text) from generate_series(1, 10000) g")
	// Plain storage: no compression, so every row costs its 2 KB.
	e.query("small/full.pgdb", "create table fill (s text); alter table fill alter s set storage plain")
	// Fill until it fails.
	var lastErr string
	for i := 0; i < 200; i++ {
		_, errs, code := e.run("exec", "small/full.pgdb", "--", "psql", "-X", "-v", "ON_ERROR_STOP=1", "-c",
			"insert into fill select repeat(md5(random()::text), 64) from generate_series(1, 5000)")
		if code != 0 {
			lastErr = errs
			break
		}
	}
	t.Logf("filling failed with: %s", strings.TrimSpace(lastErr))
	if !strings.Contains(lastErr, "No space left") && !strings.Contains(lastErr, "could not extend") &&
		!strings.Contains(lastErr, "could not write") && !strings.Contains(lastErr, "terminated") &&
		!strings.Contains(lastErr, "connection") && !strings.Contains(lastErr, "server closed") {
		t.Errorf("filling the disk didn't fail with a disk-full error: %q", lastErr)
	}
	logs := e.must("logs", "small/full.pgdb")
	t.Logf("server log tail:\n%s", tail(logs, 15))
	// Free the space: drop the filler (or, if Postgres is down, delete an
	// external file we wrote) and check everything is intact.
	ok := false
	for i := 0; i < 30 && !ok; i++ {
		_, _, code := e.run("small/full.pgdb", "drop table if exists fill")
		ok = code == 0
		if !ok {
			time.Sleep(2 * time.Second)
		}
	}
	if !ok {
		t.Fatalf("the server never came back after the disk filled:\n%s", tail(e.must("logs", "small/full.pgdb"), 30))
	}
	e.query("small/full.pgdb", "checkpoint")
	if got := e.query("small/full.pgdb", "select count(*) || ' ' || md5(string_agg(s, '' order by i)) from keep"); !strings.HasPrefix(got, "10000 ") {
		t.Errorf("keep after disk full: %q", got)
	}
	e.check("small/full.pgdb")
	e.pgChecksums("small/full.pgdb")
}

// ---------------------------------------------------------------------------
// Recovery drills

// TestStressBackupRestore dumps a .pgdb while writers run, restores the
// dump into a new .pgdb, and checks the restore matches the snapshot the
// dump saw; then copies a stopped .pgdb and opens the copy.
func TestStressBackupRestore(t *testing.T) {
	e := stressEnv(t)
	e.must("init", "live.pgdb")
	e.query("live.pgdb", crashSchema)
	w := &crashWorkload{writers: 4, acked: make([]atomic.Int64, 4)}
	url := e.start("live.pgdb")
	ctx, cancel := context.WithCancel(context.Background())
	wait := w.run(ctx, url)
	time.Sleep(3 * time.Second)
	// A custom-format dump under load, and the same snapshot's checksum.
	conn := e.connect(url)
	tctx, tcancel := context.WithTimeout(context.Background(), opTimeout)
	defer tcancel()
	tx, err := conn.BeginTx(tctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		t.Fatal(err)
	}
	var snap string
	if err := tx.QueryRow(tctx, "select pg_export_snapshot()").Scan(&snap); err != nil {
		t.Fatal(err)
	}
	var want string
	tx.QueryRow(tctx, "select count(*) || ' ' || md5(string_agg(payload, '' order by writer, seq)) from log").Scan(&want)
	e.must("exec", "live.pgdb", "--", "pg_dump", "-Fc", "--snapshot="+snap, "-f", "live.dump")
	tx.Rollback(context.Background())
	conn.Close(context.Background())
	time.Sleep(2 * time.Second)
	finish(t, cancel, wait)
	w.checkErrs(t)

	e.must("init", "restored.pgdb")
	if out, errs, code := e.run("restore", "restored.pgdb", "live.dump"); code != 0 {
		t.Fatalf("restore: exit %d\n%s%s", code, out, errs)
	}
	if got := e.query("restored.pgdb", "select count(*) || ' ' || md5(string_agg(payload, '' order by writer, seq)) from log"); got != want {
		t.Errorf("restored log %q, the dump's snapshot had %q", got, want)
	}
	if got := e.query("restored.pgdb", "select sum(balance) from accounts"); got != strconv.Itoa(nAccounts*startBal) {
		t.Errorf("restored accounts sum to %s", got)
	}
	e.check("restored.pgdb")

	// Cold copy of a stopped .pgdb.
	e.must("stop", "live.pgdb")
	if err := copyDir(e.path("live.pgdb"), e.path("cold.pgdb")); err != nil {
		t.Fatalf("copying live.pgdb: %v", err)
	}
	w.verify(t, e, "cold.pgdb")
	e.check("cold.pgdb")
	e.pgChecksums("cold.pgdb")
}

// ---------------------------------------------------------------------------

func lastLine(s string) string { return tail(s, 1) }

// copyDir copies a directory tree with its files' modes, as cp -a would,
// without needing cp (Windows has none).
func copyDir(src, dst string) error {
	return filepath.WalkDir(src, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}
		to := filepath.Join(dst, rel)
		info, err := d.Info()
		if err != nil {
			return err
		}
		switch {
		case d.IsDir():
			return os.MkdirAll(to, info.Mode().Perm())
		case d.Type()&fs.ModeSymlink != 0:
			target, err := os.Readlink(path)
			if err != nil {
				return err
			}
			return os.Symlink(target, to)
		case !d.Type().IsRegular():
			return nil // sockets and the like
		}
		in, err := os.Open(path)
		if err != nil {
			return err
		}
		defer in.Close()
		out, err := os.OpenFile(to, os.O_WRONLY|os.O_CREATE|os.O_EXCL, info.Mode().Perm())
		if err != nil {
			return err
		}
		if _, err := io.Copy(out, in); err != nil {
			out.Close()
			return err
		}
		if err := out.Close(); err != nil {
			return err
		}
		return os.Chtimes(to, info.ModTime(), info.ModTime())
	})
}
