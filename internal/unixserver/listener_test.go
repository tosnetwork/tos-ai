package unixserver

import (
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tosnetwork/tos-ai/internal/dirlock"
)

func TestListenCreatesPrivateSocketAndCleansUp(t *testing.T) {
	path := privateSocketPath(t)
	private := filepath.Dir(path)
	listener, err := Listen(path)
	if err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("socket mode = %o", info.Mode().Perm())
	}
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("socket remains after close: %v", err)
	}
	lockPath := filepath.Join(private, ".worker.sock.lock")
	if info, err := os.Lstat(lockPath); err != nil ||
		!info.Mode().IsRegular() || info.Mode().Perm() != 0o600 {
		t.Fatalf("socket ownership lock info=%v err=%v", info, err)
	}
}

func TestListenRejectsNonSocketTarget(t *testing.T) {
	path := privateSocketPath(t)
	if err := os.WriteFile(path, []byte("do not replace"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Listen(path); err == nil {
		t.Fatal("non-socket target replaced")
	}
}

func TestListenExclusivityAndIdempotentCloseProtectSuccessor(t *testing.T) {
	path := privateSocketPath(t)
	first, err := Listen(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Listen(path); err == nil {
		t.Fatal("second listener replaced active socket")
	}
	connection, err := net.DialTimeout("unix", path, time.Second)
	if err != nil {
		t.Fatalf("first listener became unreachable: %v", err)
	}
	_ = connection.Close()
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}

	second, err := Listen(path)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	connection, err = net.DialTimeout("unix", path, time.Second)
	if err != nil {
		t.Fatalf("repeated old close removed successor: %v", err)
	}
	_ = connection.Close()
}

func TestConcurrentCloseIsBoundedAndReusable(t *testing.T) {
	path := privateSocketPath(t)
	listener, err := ListenLimited(path, 2)
	if err != nil {
		t.Fatal(err)
	}
	const closers = 32
	results := make(chan error, closers)
	var wait sync.WaitGroup
	for range closers {
		wait.Add(1)
		go func() {
			defer wait.Done()
			results <- listener.Close()
		}()
	}
	wait.Wait()
	close(results)
	for err := range results {
		if err != nil {
			t.Fatalf("concurrent close error=%v", err)
		}
	}
	replacement, err := ListenLimited(path, 2)
	if err != nil {
		t.Fatalf("socket ownership was not reusable: %v", err)
	}
	if err := replacement.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestListenDistinguishesActiveAndStaleLegacySocket(t *testing.T) {
	path := privateSocketPath(t)
	legacy, err := net.ListenUnix("unix", &net.UnixAddr{
		Name: path, Net: "unix",
	})
	if err != nil {
		t.Fatal(err)
	}
	legacy.SetUnlinkOnClose(false)
	if err := os.Chmod(path, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Listen(path); err == nil {
		t.Fatal("active legacy listener was replaced")
	}
	if _, err := os.Lstat(path); err != nil {
		t.Fatalf("active legacy socket was removed: %v", err)
	}
	if err := legacy.Close(); err != nil {
		t.Fatal(err)
	}

	replacement, err := Listen(path)
	if err != nil {
		t.Fatalf("stale legacy socket was not recovered: %v", err)
	}
	if err := replacement.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestListenRejectsUnsafeDirectoryAndOversizedName(t *testing.T) {
	t.Run("directory mode", func(t *testing.T) {
		directory := t.TempDir()
		if err := os.Chmod(directory, 0o750); err != nil {
			t.Fatal(err)
		}
		if _, err := Listen(
			filepath.Join(directory, "worker.sock"),
		); err == nil {
			t.Fatal("non-private socket directory accepted")
		}
	})
	t.Run("directory symlink", func(t *testing.T) {
		target := t.TempDir()
		link := filepath.Join(t.TempDir(), "socket-dir")
		if err := os.Symlink(target, link); err != nil {
			t.Fatal(err)
		}
		if _, err := Listen(
			filepath.Join(link, "worker.sock"),
		); err == nil {
			t.Fatal("symlink socket directory accepted")
		}
	})
	t.Run("socket name", func(t *testing.T) {
		name := strings.Repeat("s", dirlock.MaxNameBytes) + ".sock"
		directory := filepath.Dir(privateSocketPath(t))
		if _, err := Listen(filepath.Join(directory, name)); err == nil {
			t.Fatal("oversized socket name accepted")
		}
	})
}

func TestPreparePrivateFileTargetRejectsSymlinkAndInsecureFile(t *testing.T) {
	directory := filepath.Dir(privateSocketPath(t))
	target := filepath.Join(directory, "target.db")
	if err := os.WriteFile(target, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(directory, "linked.db")
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}
	if err := PreparePrivateFileTarget(link); err == nil {
		t.Fatal("state-file symlink was accepted")
	}
	if err := os.Chmod(target, 0o640); err != nil {
		t.Fatal(err)
	}
	if err := PreparePrivateFileTarget(target); err == nil {
		t.Fatal("insecure state-file mode was accepted")
	}
	if err := PreparePrivateFileTarget(
		filepath.Join(directory, "new.db"),
	); err != nil {
		t.Fatal(err)
	}
}

func privateSocketPath(t *testing.T) string {
	t.Helper()
	root, err := os.MkdirTemp("/tmp", "us-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	directory := filepath.Join(root, "private")
	if err := os.Mkdir(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	return filepath.Join(directory, "worker.sock")
}
