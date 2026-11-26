//go:build stress && windows

package e2e

import (
	"encoding/csv"
	"fmt"
	"os/exec"
	"strconv"
	"strings"
)

// serverProcs lists a server's host processes by pid, with their names. On
// WHP that's the minipg server alone: its guest processes live inside it.
func serverProcs(server int) map[int]string {
	out := map[int]string{}
	if row := tasklist(server); row != nil {
		out[server] = row[0]
	}
	return out
}

// tasklist returns tasklist's CSV row for pid (image name, pid, session
// name, session#, mem usage), or nil if there's no such process.
func tasklist(pid int) []string {
	b, err := exec.Command("tasklist", "/FI", fmt.Sprintf("PID eq %d", pid), "/FO", "CSV", "/NH").Output()
	if err != nil {
		return nil
	}
	rows, _ := csv.NewReader(strings.NewReader(string(b))).ReadAll()
	for _, r := range rows {
		if len(r) >= 5 && r[1] == strconv.Itoa(pid) {
			return r
		}
	}
	return nil
}

// procRSS is a process's working set in KB, or 0.
func procRSS(pid int) int64 {
	row := tasklist(pid)
	if row == nil {
		return 0
	}
	// "123,456 K", with the locale's digit grouping.
	digits := strings.Map(func(r rune) rune {
		if r >= '0' && r <= '9' {
			return r
		}
		return -1
	}, row[4])
	n, _ := strconv.ParseInt(digits, 10, 64)
	return n
}

// tcpSockets describes the host's TCP sockets on port, from netstat.
// Windows's netstat doesn't show queued bytes.
func tcpSockets(port int) string {
	var b strings.Builder
	b.WriteString("host TCP sockets on the server's port (netstat: proto local remote state pid):\n")
	out, _ := exec.Command("netstat", "-ano", "-p", "tcp").Output()
	suffix := fmt.Sprintf(":%d", port)
	for _, l := range strings.Split(string(out), "\n") {
		f := strings.Fields(l)
		if len(f) > 3 && (strings.HasSuffix(f[1], suffix) || strings.HasSuffix(f[2], suffix)) {
			fmt.Fprintf(&b, "  %s\n", strings.Join(f, " "))
		}
	}
	return b.String()
}
