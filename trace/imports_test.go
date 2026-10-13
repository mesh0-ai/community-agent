package trace_test

import (
	"go/parser"
	"go/token"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// allowedImports is everything the trace package's non-test files may import.
// It must stay free of sockets, the datagram wire and token verification so
// client-go can embed it. Widen this list only for a pure, in-process need.
var allowedImports = map[string]bool{
	"bytes": true, "context": true, "crypto/rand": true, "encoding/hex": true,
	"encoding/json": true, "errors": true, "fmt": true, "log/slog": true,
	"sort": true, "strconv": true, "strings": true, "sync": true,
	"sync/atomic": true, "time": true,
}

func TestTracePackageImportsNoTransport(t *testing.T) {
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	fset := token.NewFileSet()
	for _, f := range files {
		if strings.HasSuffix(f, "_test.go") {
			continue
		}
		af, err := parser.ParseFile(fset, f, nil, parser.ImportsOnly)
		if err != nil {
			t.Fatal(err)
		}
		for _, imp := range af.Imports {
			p, _ := strconv.Unquote(imp.Path.Value)
			if !allowedImports[p] {
				t.Errorf("%s imports %q, which is not on the trace package's allowlist", f, p)
			}
		}
	}
}

// TestTracePackageDependsOnNoNetwork checks the transitive closure too.
func TestTracePackageDependsOnNoNetwork(t *testing.T) {
	if _, err := exec.LookPath("go"); err != nil {
		t.Skip("go tool not on PATH")
	}
	cmd := exec.Command("go", "list", "-deps", ".")
	cmd.Env = append(os.Environ(), "GOFLAGS=")
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("go list: %v", err)
	}
	for _, dep := range strings.Fields(string(out)) {
		if dep == "net" || strings.HasPrefix(dep, "net/") ||
			strings.HasPrefix(dep, "crypto/ed25519") || strings.HasPrefix(dep, "github.com/jackc/") ||
			strings.HasPrefix(dep, "modernc.org/") || strings.Contains(dep, "community-agent/cmd") {
			t.Errorf("trace depends on %s", dep)
		}
	}
}
