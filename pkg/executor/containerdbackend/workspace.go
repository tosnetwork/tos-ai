package containerdbackend

import (
	"archive/tar"
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"
)

const maxWorkspaceEntries = 100_000
const workspaceRootName = "workspaces"

// prepareWorkspace extracts a deliberately small tar subset into a new,
// runtime-owned directory. Links, devices, sparse files, duplicate paths and
// path traversal are rejected so a workload can never select a host path.
func prepareWorkspace(root, identity string, archive []byte, maximum uint64) (string, func() error, error) {
	if !runtimeIdentifier.MatchString(identity) || maximum == 0 || uint64(len(archive)) > maximum {
		return "", nil, errors.New("invalid containerd workspace archive")
	}
	workspaceRoot := filepath.Join(root, workspaceRootName)
	if err := ensurePrivateDirectory(workspaceRoot); err != nil {
		return "", nil, errors.New("invalid containerd workspace root")
	}
	directory := filepath.Join(workspaceRoot, identity)
	if err := os.Mkdir(directory, 0o700); err != nil {
		return "", nil, errors.New("create containerd workspace")
	}
	cleanup := func() error { return removeWorkspace(directory) }
	if err := extractWorkspaceTar(directory, archive, maximum); err != nil {
		_ = cleanup()
		return "", nil, err
	}
	if err := makeWorkspaceReadOnly(directory); err != nil {
		_ = cleanup()
		return "", nil, err
	}
	return directory, cleanup, nil
}

func removeWorkspace(directory string) error {
	_ = filepath.Walk(directory, func(current string, info os.FileInfo, err error) error {
		if err == nil && info.IsDir() {
			_ = os.Chmod(current, 0o700)
		}
		return nil
	})
	return os.RemoveAll(directory)
}

func extractWorkspaceTar(root string, archive []byte, maximum uint64) error {
	reader := tar.NewReader(bytes.NewReader(archive))
	seen := make(map[string]struct{})
	var total uint64
	for entries := 0; ; entries++ {
		if entries >= maxWorkspaceEntries {
			return errors.New("containerd workspace has too many entries")
		}
		header, err := reader.Next()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return errors.New("invalid containerd workspace tar")
		}
		name, err := safeWorkspaceName(header.Name)
		if err != nil {
			return err
		}
		if _, duplicate := seen[name]; duplicate {
			return errors.New("duplicate containerd workspace path")
		}
		seen[name] = struct{}{}
		target := filepath.Join(root, filepath.FromSlash(name))
		switch header.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(target, 0o700); err != nil {
				return errors.New("create containerd workspace directory")
			}
		case tar.TypeReg, tar.TypeRegA:
			if header.Size < 0 || uint64(header.Size) > maximum-total {
				return errors.New("containerd workspace exceeds disk limit")
			}
			total += uint64(header.Size)
			if err := os.MkdirAll(filepath.Dir(target), 0o700); err != nil {
				return errors.New("create containerd workspace parent")
			}
			file, err := os.OpenFile(target, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o444)
			if err != nil {
				return errors.New("create containerd workspace file")
			}
			_, copyErr := io.CopyN(file, reader, header.Size)
			closeErr := file.Close()
			if copyErr != nil || closeErr != nil {
				return errors.New("write containerd workspace file")
			}
			mode := os.FileMode(0o444)
			if header.FileInfo().Mode()&0o111 != 0 {
				mode = 0o555
			}
			if err := os.Chmod(target, mode); err != nil {
				return errors.New("protect containerd workspace file")
			}
		default:
			return fmt.Errorf("unsupported containerd workspace tar entry %q", name)
		}
	}
}

func safeWorkspaceName(value string) (string, error) {
	if value == "" || strings.IndexByte(value, 0) >= 0 || strings.HasPrefix(value, "/") || len(value) > 4096 {
		return "", errors.New("invalid containerd workspace path")
	}
	cleaned := path.Clean(value)
	if cleaned == "." || cleaned == ".." || strings.HasPrefix(cleaned, "../") || cleaned != strings.TrimSuffix(value, "/") {
		return "", errors.New("invalid containerd workspace path")
	}
	return cleaned, nil
}

func makeWorkspaceReadOnly(root string) error {
	return filepath.Walk(root, func(current string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return errors.New("containerd workspace contains a symlink")
		}
		if info.IsDir() {
			return os.Chmod(current, 0o555)
		}
		return nil
	})
}
