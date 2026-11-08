// Package actionauthoritytest supports tests of the ML-DSA-44 Action
// Authority boundary: an authority-scoped resolver, a private tosctl vault
// whose controllers can sign one message under any signature use (so a test
// can present a signature made under another context), and the refusal
// variants every Action Authority consumer must reject.
//
// Real ML-DSA-44 bytes come only from the pinned helper named by
// TOS_SERVICE_TOSCTL and TOS_SERVICE_TOSCTL_SHA256; a test that needs them
// fails, rather than skips, when the helper is not configured.
package actionauthoritytest

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/tosnetwork/tos-service-protocol/pkg/mldsa44"
	"github.com/tosnetwork/tos-service-protocol/pkg/mldsa44/mldsa44test"
)

// Controller is the vault name of the Action Authority controller.
const Controller = "action-authority"

// Resolver authorizes an Action Authority key only for the authority it is
// listed under and verifies through Verifier.
type Resolver struct {
	Verifier mldsa44.Verifier
	Keys     map[string][][]byte
}

// Verify implements mldsa44.Verifier.
func (r Resolver) Verify(ctx context.Context, use mldsa44.Use, payload, publicKey, signature []byte) error {
	if r.Verifier == nil {
		return mldsa44.ErrUnavailable
	}
	return r.Verifier.Verify(ctx, use, payload, publicKey, signature)
}

// ResolveActionAuthorityKey returns the key listed for authorityID whose key
// ID is keyID.
func (r Resolver) ResolveActionAuthorityKey(authorityID string, keyID [mldsa44.KeyIDSize]byte, _ time.Time) ([]byte, error) {
	for _, key := range r.Keys[authorityID] {
		if id, err := mldsa44.KeyID(key); err == nil && id == keyID {
			return append([]byte(nil), key...), nil
		}
	}
	return nil, errors.New("Action Authority key is not authorized for this authority")
}

// Custody is a private tosctl vault holding named ML-DSA-44 controllers.
type Custody struct {
	Signer     *mldsa44.Tosctl
	PublicKeys map[string][]byte
	binary     string
	configPath string
	vaultURL   string
}

// NewCustody creates an owner-private file vault with one controller per
// name. The vault master key is random, stays in this process and the
// helper's environment, and is removed with the vault when the test ends.
func NewCustody(t testing.TB, names ...string) *Custody {
	t.Helper()
	verifier := mldsa44test.RequireTosctl(t)
	if verifier == nil {
		t.Fatal("tosctl helper is unavailable")
	}
	binary := os.Getenv(mldsa44test.BinaryEnvironment)
	digest := os.Getenv(mldsa44test.SHA256Environment)
	directory, err := os.MkdirTemp("", "tos-ai-action-authority-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(directory) })
	if err := os.Chmod(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	masterKey := make([]byte, 32)
	if _, err := rand.Read(masterKey); err != nil {
		t.Fatal(err)
	}
	custody := &Custody{PublicKeys: make(map[string][]byte, len(names)), binary: binary,
		configPath: filepath.Join(directory, "tosctl-config.json"),
		vaultURL:   "file://" + filepath.Join(directory, "vault.json") + "?master_key=" + hex.EncodeToString(masterKey)}
	custody.run(t, "config", "generate", "--output="+custody.configPath)
	for _, name := range names {
		output := custody.run(t, "agent", "native-registry", "new-controller", "--config", custody.configPath,
			"--name", name, "--format", "json")
		var created struct {
			Name      string `json:"name"`
			Suite     string `json:"suite"`
			KeyID     string `json:"key_id"`
			PublicKey string `json:"public_key"`
		}
		if err := json.Unmarshal(output, &created); err != nil || created.Name != name || created.Suite != "ML-DSA-44" {
			t.Fatalf("unexpected new-controller output for %q", name)
		}
		key, err := hex.DecodeString(created.PublicKey)
		if err != nil || len(key) != mldsa44.PublicKeySize {
			t.Fatalf("new-controller returned a malformed key for %q", name)
		}
		id, err := mldsa44.KeyID(key)
		if err != nil || hex.EncodeToString(id[:]) != created.KeyID {
			t.Fatalf("new-controller key ID for %q does not match its key", name)
		}
		custody.PublicKeys[name] = key
	}
	signer, err := mldsa44.NewTosctl(mldsa44.TosctlConfig{Binary: binary, SHA256: digest,
		ConfigPath: custody.configPath, VaultURL: custody.vaultURL})
	if err != nil {
		t.Fatal(err)
	}
	custody.Signer = signer
	return custody
}

// KeyID returns the text key ID of a named controller.
func (c *Custody) KeyID(t testing.TB, name string) string {
	t.Helper()
	key, ok := c.PublicKeys[name]
	if !ok {
		t.Fatalf("unknown test controller %q", name)
	}
	id, err := mldsa44.KeyID(key)
	if err != nil {
		t.Fatal(err)
	}
	return mldsa44.KeyIDText(id)
}

// SignFor has controller sign a 32-byte message under the helper signature
// use named by use (for example "agent-guarantor" or
// "custody-authorization") and returns the mldsa44: signature text. It lets a
// test present a real signature by the Action Authority key that was made
// under another context.
func (c *Custody) SignFor(t testing.TB, controller, use string, message []byte) string {
	t.Helper()
	if len(message) != 32 {
		t.Fatal("SignFor signs only 32-byte messages")
	}
	output := c.run(t, "agent", "native-registry", "sign", "--config", c.configPath, "--controller", controller,
		"--for", use, "--message-hex", hex.EncodeToString(message), "--format", "json")
	var signed struct {
		KeyID     string `json:"key_id"`
		Signature string `json:"signature"`
	}
	if err := json.Unmarshal(output, &signed); err != nil {
		t.Fatalf("unexpected sign output for use %q", use)
	}
	raw, err := hex.DecodeString(signed.Signature)
	if err != nil || len(raw) != mldsa44.SignatureSize {
		t.Fatalf("sign for use %q returned a malformed signature", use)
	}
	text, err := mldsa44.SignatureText(raw)
	if err != nil {
		t.Fatal(err)
	}
	return text
}

func (c *Custody) run(t testing.TB, args ...string) []byte {
	t.Helper()
	command := exec.Command(c.binary, args...)
	command.Env = []string{"VAULT_URL=" + c.vaultURL}
	command.Dir = "/"
	output, err := command.Output()
	if err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			t.Fatalf("tosctl %s %s: %v: %s", args[0], args[len(args)-1], err, exitErr.Stderr)
		}
		t.Fatalf("tosctl %s: %v", args[0], err)
	}
	return output
}

// Variant is one substituted key ID and signature text.
type Variant struct {
	Name           string
	KeyID          string
	Signature      string
	WantEd25519Err bool
}

// Variants returns the key ID / signature substitutions an Action Authority
// verifier must refuse for an object whose valid key ID is keyID and whose
// 32-byte signed message is message: Ed25519, another context (Agent
// Guarantor and the custody authorization context), non-canonical text, and
// a key ID that names another listed key. other names a second controller in
// custody.
func Variants(t testing.TB, custody *Custody, controller, other, keyID, signature string, message []byte) []Variant {
	t.Helper()
	seed := make([]byte, ed25519.SeedSize)
	for index := range seed {
		seed[index] = 0x5a
	}
	classical := ed25519.NewKeyFromSeed(seed)
	classicalKey := "ed25519:" + hex.EncodeToString(classical.Public().(ed25519.PublicKey))
	classicalProof := "ed25519:" + base64.RawURLEncoding.EncodeToString(ed25519.Sign(classical, message))
	body := strings.TrimPrefix(keyID, mldsa44.KeyIDPrefix)
	raw, err := mldsa44.ParseSignatureText(signature)
	if err != nil {
		t.Fatal(err)
	}
	encodedShort := mldsa44.SignaturePrefix + base64.RawURLEncoding.EncodeToString(raw[:len(raw)-1])
	padded := mldsa44.SignaturePrefix + base64.URLEncoding.EncodeToString(raw)
	standard := mldsa44.SignaturePrefix + base64.RawStdEncoding.EncodeToString(raw)
	if standard == signature {
		t.Fatal("the signature encodes identically in both Base64 alphabets; the standard-alphabet variant would be valid")
	}
	return []Variant{
		{Name: "ed25519 key and proof", KeyID: classicalKey, Signature: classicalProof, WantEd25519Err: true},
		{Name: "ed25519 proof", KeyID: keyID, Signature: classicalProof, WantEd25519Err: true},
		{Name: "agent guarantor context", KeyID: keyID, Signature: custody.SignFor(t, controller, "agent-guarantor", message)},
		{Name: "custody authorization context", KeyID: keyID, Signature: custody.SignFor(t, controller, "custody-authorization", message)},
		{Name: "upper-case key ID", KeyID: mldsa44.KeyIDPrefix + strings.ToUpper(body), Signature: signature},
		{Name: "short key ID", KeyID: mldsa44.KeyIDPrefix + body[:63], Signature: signature},
		{Name: "all-zero key ID", KeyID: mldsa44.KeyIDPrefix + strings.Repeat("0", 64), Signature: signature},
		{Name: "padded proof", KeyID: keyID, Signature: padded},
		{Name: "standard-alphabet proof", KeyID: keyID, Signature: standard},
		{Name: "truncated proof", KeyID: keyID, Signature: encodedShort},
		{Name: "key ID of another listed key", KeyID: custody.KeyID(t, other), Signature: signature},
	}
}
