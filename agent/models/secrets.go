package models

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// Secrets is where a catalog reads credentials: by name, at Build. The
// catalog document names secrets and never carries their values, so the
// same document serves every deployment; what differs is the Secrets
// behind it. A cloud agent reads the directory a Kubernetes Secret is
// mounted as (Dir); a local agent reads what it loaded from its own
// configuration (Static); a host with a vault implements Lookup over it.
type Secrets interface {
	Lookup(ctx context.Context, name string) (string, error)
}

// ErrSecretNotFound reports a name the Secrets do not hold.
var ErrSecretNotFound = errors.New("models: secret not found")

// Dir is Secrets over a directory with one file per secret, the shape a
// Kubernetes Secret volume has: the file's name is the secret's name, its
// content the value. Trailing whitespace is trimmed, as mounted values
// commonly end in a newline. Names are single path elements.
type Dir string

// Lookup is Secrets.
func (d Dir) Lookup(_ context.Context, name string) (string, error) {
	if name == "" || name != filepath.Base(name) || strings.HasPrefix(name, ".") {
		return "", fmt.Errorf("%w: invalid secret name %q", ErrSecretNotFound, name)
	}
	raw, err := os.ReadFile(filepath.Join(string(d), name))
	if err != nil {
		if os.IsNotExist(err) {
			return "", fmt.Errorf("%w: %s", ErrSecretNotFound, name)
		}
		return "", err
	}
	return strings.TrimRight(string(raw), " \t\r\n"), nil
}

// Static is Secrets over values already in memory: what a local agent
// loaded from its configuration, or a test supplies.
type Static map[string]string

// Lookup is Secrets.
func (s Static) Lookup(_ context.Context, name string) (string, error) {
	v, ok := s[name]
	if !ok {
		return "", fmt.Errorf("%w: %s", ErrSecretNotFound, name)
	}
	return v, nil
}
