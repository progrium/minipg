//go:build !windows

package e2e

import (
	"os"
	"os/exec"
	"syscall"
)

// interrupt asks a foreground `minipg serve` to shut down as Ctrl-C would.
func (e *env) interrupt(c *exec.Cmd, pgdb string) error {
	return c.Process.Signal(syscall.SIGINT)
}

// linkDir makes link a symlink to the directory target.
func linkDir(target, link string) error {
	return os.Symlink(target, link)
}
