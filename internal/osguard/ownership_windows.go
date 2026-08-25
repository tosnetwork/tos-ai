//go:build windows

package osguard

import "os"

// Windows deployments must have their installer verify an owner-only DACL.
// Portable FileInfo exposes neither the owning SID nor link count, so runtime
// code additionally rejects reparse points and rechecks the opened identity.
func CurrentUserOwns(info os.FileInfo) bool {
	return info != nil && info.Mode()&os.ModeSymlink == 0
}

func CurrentUserOwnsSingleLink(info os.FileInfo) bool { return CurrentUserOwns(info) }
