//go:build stress && linux

package e2e

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// serverProcs lists a server's host processes: the minipg server itself
// and, on h9's native backend, its stubs (one per guest address space,
// each a child of the server in its own process group), by pid, with
// their names. Zombies are left out.
func serverProcs(server int) map[int]string {
	out := map[int]string{}
	entries, _ := os.ReadDir("/proc")
	for _, de := range entries {
		pid, err := strconv.Atoi(de.Name())
		if err != nil {
			continue
		}
		b, err := os.ReadFile(filepath.Join("/proc", de.Name(), "stat"))
		if err != nil {
			continue
		}
		s := string(b)
		i, j := strings.IndexByte(s, '('), strings.LastIndexByte(s, ')')
		if i < 0 || j < i {
			continue
		}
		f := strings.Fields(s[j+1:])
		// state ppid pgrp ...
		if len(f) < 3 || f[0] == "Z" {
			continue
		}
		ppid, _ := strconv.Atoi(f[1])
		pgrp, _ := strconv.Atoi(f[2])
		if pid == server || ppid == server || pgrp == server {
			out[pid] = s[i+1 : j]
		}
	}
	return out
}

// procRSS is a process's resident memory in KB, or 0.
func procRSS(pid int) int64 {
	b, _ := os.ReadFile(fmt.Sprintf("/proc/%d/status", pid))
	for _, l := range strings.Split(string(b), "\n") {
		if strings.HasPrefix(l, "VmRSS:") {
			if f := strings.Fields(l); len(f) > 1 {
				n, _ := strconv.ParseInt(f[1], 10, 64)
				return n
			}
		}
	}
	return 0
}

// tcpSockets describes the host's TCP sockets on port, with their queued
// bytes.
func tcpSockets(port int) string {
	var b strings.Builder
	tcp, _ := os.ReadFile("/proc/net/tcp")
	hex := fmt.Sprintf(":%04X", port)
	b.WriteString("host TCP sockets on the server's port (local remote state tx:rx queues):\n")
	for _, l := range strings.Split(string(tcp), "\n") {
		f := strings.Fields(l)
		if len(f) > 4 && (strings.HasSuffix(f[1], hex) || strings.HasSuffix(f[2], hex)) {
			fmt.Fprintf(&b, "  %s %s %s %s\n", f[1], f[2], f[3], f[4])
		}
	}
	return b.String()
}
