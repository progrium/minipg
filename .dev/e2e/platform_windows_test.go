//go:build windows

package e2e

import (
	"os"
	"os/exec"
)

// interrupt asks a foreground `minipg serve` to shut down. Windows has no
// SIGINT to send another process (a console Ctrl-C reaches every process
// on the console, the test binary too), so it goes through the control
// socket with `minipg stop`, which shuts down the same way.
func (e *env) interrupt(c *exec.Cmd, pgdb string) error {
	return e.cmd("", nil, "stop", pgdb).Run()
}

// linkDir makes link point at the directory target: a symlink where the
// account may make them (Developer Mode or an elevated shell), otherwise a
// directory junction, which needs no privilege.
func linkDir(target, link string) error {
	if os.Symlink(target, link) == nil {
		return nil
	}
	return exec.Command("cmd", "/c", "mklink", "/J", link, target).Run()
}
