package modelmanager

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
)

const ArtifactVerifyBufferBytes = 1 << 20

// Verify rehashes the complete already-open artifact represented by this
// lease. It does not reopen or publish a host path and uses one fixed-size
// buffer. Callers retain the lease after verification until Close.
func (l *ArtifactLease) Verify(ctx context.Context) error {
	if ctx == nil || l == nil {
		return ErrArtifact
	}
	model := l.Model()
	if model.SizeBytes == 0 || !validModelDigest(model.Digest) {
		return ErrArtifact
	}
	hasher := sha256.New()
	buffer := make([]byte, min(uint64(ArtifactVerifyBufferBytes), model.SizeBytes))
	var offset uint64
	for offset < model.SizeBytes {
		if err := ctx.Err(); err != nil {
			return err
		}
		want := min(uint64(len(buffer)), model.SizeBytes-offset)
		count, err := l.ReadAt(buffer[:want], int64(offset))
		if count > 0 {
			if _, writeErr := hasher.Write(buffer[:count]); writeErr != nil {
				return ErrArtifact
			}
			offset += uint64(count)
		}
		if err != nil && !(errors.Is(err, io.EOF) && offset == model.SizeBytes) {
			return ErrArtifact
		}
		if count == 0 && err == nil {
			return ErrArtifact
		}
	}
	actual := "sha256:" + hex.EncodeToString(hasher.Sum(nil))
	if actual != model.Digest {
		return ErrArtifact
	}
	return nil
}

func validModelDigest(value string) bool {
	if len(value) != len("sha256:")+64 || value[:len("sha256:")] != "sha256:" {
		return false
	}
	digest := value[len("sha256:"):]
	decoded, err := hex.DecodeString(digest)
	return err == nil && len(decoded) == sha256.Size &&
		hex.EncodeToString(decoded) == digest
}
