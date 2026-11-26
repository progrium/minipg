//go:build stress && !linux && !windows

package e2e

import (
	"fmt"
	"os/exec"
	"strconv"
	"strings"
)

// serverProcs lists a server's host processes by pid, with their names:
// the minipg server, and anything it started or that shares its process
// group. On a hypervisor backend (HVF) that's the server alone; its guest
// processes live inside it. Zombies are left out. There's no /proc here, so
// it asks ps.
func serverProcs(server int) map[int]string {
	out := map[int]string{}
	b, err := exec.Command("ps", "-A", "-o", "pid=,ppid=,pgid=,stat=,comm=").Output()
	if err != nil {
		return out
	}
	for _, l := range strings.Split(string(b), "\n") {
		f := strings.Fields(l)
		if len(f) < 5 || strings.HasPrefix(f[3], "Z") {
			continue
		}
		pid, _ := strconv.Atoi(f[0])
		ppid, _ := strconv.Atoi(f[1])
		pgid, _ := strconv.Atoi(f[2])
		if pid == server || ppid == server || pgid == server {
			out[pid] = strings.Join(f[4:], " ")
		}
	}
	return out
}

// procRSS is a process's resident memory in KB, or 0.
func procRSS(pid int) int64 {
	b, _ := exec.Command("ps", "-o", "rss=", "-p", strconv.Itoa(pid)).Output()
	n, _ := strconv.ParseInt(strings.TrimSpace(string(b)), 10, 64)
	return n
}

// tcpSockets describes the host's TCP sockets on port, with their queued
// bytes, from netstat.
func tcpSockets(port int) string {
	var b strings.Builder
	b.WriteString("host TCP sockets on the server's port (netstat: proto recv-q send-q local remote state):\n")
	out, _ := exec.Command("netstat", "-an", "-p", "tcp").Output()
	suffix := fmt.Sprintf(".%d", port)
	for _, l := range strings.Split(string(out), "\n") {
		f := strings.Fields(l)
		if len(f) > 4 && (strings.HasSuffix(f[3], suffix) || strings.HasSuffix(f[4], suffix)) {
			fmt.Fprintf(&b, "  %s\n", strings.Join(f, " "))
		}
	}
	return b.String()
}
