package containerdbackend

import (
	"archive/tar"
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestWorkspaceExtractionRejectsTraversalLinksDuplicatesAndOverflow(t *testing.T) {
	archive := func(entries ...tar.Header) []byte {
		var buffer bytes.Buffer
		writer := tar.NewWriter(&buffer)
		for index := range entries {
			header := entries[index]
			if err := writer.WriteHeader(&header); err != nil {
				t.Fatal(err)
			}
			if header.Size > 0 {
				_, _ = writer.Write(bytes.Repeat([]byte{'x'}, int(header.Size)))
			}
		}
		if err := writer.Close(); err != nil {
			t.Fatal(err)
		}
		return buffer.Bytes()
	}
	cases := [][]byte{
		archive(tar.Header{Name: "../escape", Typeflag: tar.TypeReg, Size: 1}),
		archive(tar.Header{Name: "link", Typeflag: tar.TypeSymlink, Linkname: "/etc/passwd"}),
		archive(tar.Header{Name: "same", Typeflag: tar.TypeReg}, tar.Header{Name: "same", Typeflag: tar.TypeReg}),
		archive(tar.Header{Name: "large", Typeflag: tar.TypeReg, Size: 9}),
	}
	root := filepath.Join(t.TempDir(), "runtime")
	if err := os.Mkdir(root, 0o700); err != nil {
		t.Fatal(err)
	}
	for index, value := range cases {
		if _, _, err := prepareWorkspace(root, "0123456789abcdef", value, 8); err == nil {
			t.Fatalf("unsafe workspace case %d accepted", index)
		}
	}
}

func TestWorkspaceExtractionProducesReadOnlyTree(t *testing.T) {
	var buffer bytes.Buffer
	writer := tar.NewWriter(&buffer)
	body := []byte("package work")
	if err := writer.WriteHeader(&tar.Header{Name: "src/work.go", Typeflag: tar.TypeReg, Mode: 0o644, Size: int64(len(body))}); err != nil {
		t.Fatal(err)
	}
	_, _ = writer.Write(body)
	_ = writer.Close()
	root := filepath.Join(t.TempDir(), "runtime")
	if err := os.Mkdir(root, 0o700); err != nil {
		t.Fatal(err)
	}
	directory, cleanup, err := prepareWorkspace(root, "0123456789abcdef", buffer.Bytes(), 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	info, err := os.Stat(filepath.Join(directory, "src", "work.go"))
	if err != nil || info.Mode().Perm() != 0o444 {
		t.Fatalf("workspace file mode=%v err=%v", info.Mode(), err)
	}
	if info, err = os.Stat(directory); err != nil || info.Mode().Perm() != 0o555 {
		t.Fatalf("workspace root mode=%v err=%v", info.Mode(), err)
	}
}
