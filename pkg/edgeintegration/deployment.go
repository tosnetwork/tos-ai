// Package edgeintegration composes the tos-ai vertical with the generic Edge
// Core without giving the Worker public-network or wallet authority.
package edgeintegration

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/binary"
	"errors"
	"fmt"
	"sort"

	"github.com/tosnetwork/tos-ai/pkg/profile/textgeneration"
	edgev1 "github.com/tosnetwork/tos-protocol/gen/tos/edge/v1"
	"github.com/tosnetwork/tos-protocol/pkg/edge"
	"github.com/tosnetwork/tos-protocol/pkg/localrpc"
)

// Deployment is an immutable pairing of one validated private Worker client
// and the exact public profile plan derived from that Worker's fresh external
// capabilities. It does not own or close the client.
type Deployment struct {
	worker           *localrpc.WorkerClient
	plan             *edge.ProfileInvocationPlan
	routeFingerprint [sha256.Size]byte
}

// New probes the private Worker once and fails closed if it has no currently
// available, externally callable tos.ai.text-generation route. Dynamic
// capacity remains enforced again by Quote and Invoke; the startup snapshot is
// used only to install the bounded semantic mapper and cannot grant payment or
// session authority.
func New(ctx context.Context, worker *localrpc.WorkerClient) (*Deployment, error) {
	if ctx == nil {
		return nil, errors.New("nil Edge integration context")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if worker == nil {
		return nil, errors.New("nil Worker client")
	}
	capabilities, err := worker.GetCapabilities(ctx)
	if err != nil {
		return nil, fmt.Errorf("load Worker capabilities for Edge: %w", err)
	}
	plan, err := textgeneration.NewProfilePlanFromWorkerCapabilities(
		capabilities.Capabilities,
	)
	if err != nil {
		return nil, fmt.Errorf("construct Worker profile plan: %w", err)
	}
	fingerprint, err := fingerprintExternalRoutes(capabilities.Capabilities)
	if err != nil {
		return nil, fmt.Errorf("commit Worker profile routes: %w", err)
	}
	return &Deployment{
		worker: worker, plan: plan, routeFingerprint: fingerprint,
	}, nil
}

// Worker returns the validated private client installed in this deployment.
// WorkerClient has no automatic retry behavior.
func (d *Deployment) Worker() (*localrpc.WorkerClient, error) {
	if d == nil || d.worker == nil || d.plan == nil {
		return nil, errors.New("invalid Edge AI deployment")
	}
	return d.worker, nil
}

// ProfilePlan returns the immutable exact-selector plan installed from the
// live Worker snapshot.
func (d *Deployment) ProfilePlan() (*edge.ProfileInvocationPlan, error) {
	if d == nil || d.worker == nil || d.plan == nil {
		return nil, errors.New("invalid Edge AI deployment")
	}
	return d.plan, nil
}

// CheckReady proves that the same private Worker is healthy and still exposes
// the exact externally callable service/model/runtime identities from which
// the immutable Edge profile plan was built. Dynamic capacity may change and
// remains enforced by Quote/Invoke; route or model/runtime identity drift
// requires a deliberate deployment reload instead of failing after payment.
func (d *Deployment) CheckReady(ctx context.Context) error {
	if d == nil || d.worker == nil || d.plan == nil {
		return errors.New("invalid Edge AI deployment")
	}
	if ctx == nil {
		return errors.New("nil Edge AI readiness context")
	}
	health, err := d.worker.Health(ctx)
	if err != nil {
		return fmt.Errorf("check Worker health: %w", err)
	}
	for _, required := range []string{
		"worker", "admission", "resources", "runtimes", "model-binding",
		"task-store",
	} {
		found := false
		for _, component := range health.Readiness {
			if component.Id != required {
				continue
			}
			found = true
			if component.Status != edgev1.ReadinessStatus_READINESS_STATUS_READY {
				return errors.New("Worker is not ready")
			}
			break
		}
		if !found {
			return errors.New("Worker omitted a required readiness component")
		}
	}
	capabilities, err := d.worker.GetCapabilities(ctx)
	if err != nil {
		return fmt.Errorf("refresh Worker capabilities: %w", err)
	}
	fingerprint, err := fingerprintExternalRoutes(capabilities.Capabilities)
	if err != nil {
		return fmt.Errorf("validate current Worker profile routes: %w", err)
	}
	if subtle.ConstantTimeCompare(
		fingerprint[:], d.routeFingerprint[:],
	) != 1 {
		return errors.New("Worker profile routes changed since deployment startup")
	}
	return nil
}

func fingerprintExternalRoutes(
	capabilities []*edgev1.Capability,
) ([sha256.Size]byte, error) {
	if _, err := textgeneration.NewProfilePlanFromWorkerCapabilities(
		capabilities,
	); err != nil {
		return [sha256.Size]byte{}, err
	}
	rows := make([][]byte, 0, len(capabilities))
	for _, capability := range capabilities {
		if capability.Operation != textgeneration.Operation ||
			!hasExternalPriority(capability.AcceptedPriorities) {
			continue
		}
		row := make([]byte, 0, 256)
		for _, field := range []string{
			capability.ServiceId, capability.Operation, capability.Model,
			capability.ModelDigest, capability.Runtime,
			capability.RuntimeRevision,
		} {
			row = appendFramedRouteField(row, field)
		}
		rows = append(rows, row)
	}
	sort.Slice(rows, func(left, right int) bool {
		return bytes.Compare(rows[left], rows[right]) < 0
	})
	hash := sha256.New()
	var length [8]byte
	_, _ = hash.Write([]byte("tos.ai.edge.routes.v1\x00"))
	binary.BigEndian.PutUint64(length[:], uint64(len(rows)))
	_, _ = hash.Write(length[:])
	for _, row := range rows {
		binary.BigEndian.PutUint64(length[:], uint64(len(row)))
		_, _ = hash.Write(length[:])
		_, _ = hash.Write(row)
	}
	var output [sha256.Size]byte
	copy(output[:], hash.Sum(nil))
	return output, nil
}

func appendFramedRouteField(output []byte, value string) []byte {
	var length [8]byte
	binary.BigEndian.PutUint64(length[:], uint64(len(value)))
	output = append(output, length[:]...)
	return append(output, value...)
}

func hasExternalPriority(values []edgev1.Priority) bool {
	for _, value := range values {
		if value == edgev1.Priority_PRIORITY_EXTERNAL_SERVICE {
			return true
		}
	}
	return false
}
