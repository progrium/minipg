//go:build !windows

package e2e

import (
	"os"
	"strings"
	"syscall"
	"testing"
	"time"
)

// TestRunStop: `run --stop` stops the server it started when the command
// exits, and a SIGTERM to its process group (what an on-demand launcher
// sends) reaches the command and still ends in a clean shutdown.
func TestRunStop(t *testing.T) {
	e := newEnv(t)
	e.must("init", "app.pgdb")

	// The command's exit code passes through, and the server is gone.
	if _, errs, code := e.run("run", "--stop", "app.pgdb", "--", "sh", "-c", "exit 5"); code != 5 {
		t.Fatalf("run --stop's exit code: %d, want 5\n%s", code, errs)
	}
	if e.status("app.pgdb").running {
		t.Fatalf("server still running after run --stop")
	}

	// SIGTERM to the whole process group, run included.
	c := e.cmd("", nil, "run", "--stop", "app.pgdb", "--", "sh", "-c", `trap "exit 0" TERM; touch up; while :; do sleep 0.1; done`)
	c.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := c.Start(); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "the command to start", 60*time.Second, func() bool {
		_, err := os.Stat(e.path("up"))
		return err == nil
	})
	if err := syscall.Kill(-c.Process.Pid, syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- c.Wait() }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("run --stop after SIGTERM: %v", err)
		}
	case <-time.After(60 * time.Second):
		c.Process.Kill()
		t.Fatalf("run --stop didn't exit after SIGTERM")
	}
	// run waits for the server's lock, so it's stopped by the time run is.
	if e.status("app.pgdb").running {
		t.Fatalf("server still running after run --stop got SIGTERM")
	}
	// The next start finds a cleanly shut down cluster.
	e.query("app.pgdb", "select 1")
	if logs := e.must("logs", "app.pgdb"); strings.Contains(logs, "not properly shut down") {
		t.Errorf("the server crashed instead of stopping:\n%s", logs)
	}

	// A server that was already running is left alone.
	e.must("run", "--stop", "app.pgdb", "--", "true")
	if !e.status("app.pgdb").running {
		t.Errorf("run --stop stopped a server it didn't start")
	}
}
