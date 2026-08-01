// Package textgeneration implements the reviewed tos.ai.text-generation
// profile mapper. It translates a bounded public JSON intent into the private
// prompt payload accepted by configured TOS AI Worker adapters.
package textgeneration

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strings"
	"unicode"
	"unicode/utf8"

	airuntime "github.com/tosnetwork/tos-ai/pkg/runtime"
	"github.com/tosnetwork/tos-protocol/pkg/edge"
)

const (
	ProfileID          = "tos.ai.text-generation"
	ProfileVersion     = "0.1.0"
	Operation          = "generate"
	MaxRoutes          = 128
	MaxIntentBytesHard = 16 << 20
	MaxModelBytes      = 256
)

var serviceIDPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9._:-]{2,127}$`)

// Route is an operator-reviewed Worker service/model pair. Public intents may
// select only pairs installed when the immutable mapper is constructed.
type Route struct {
	ServiceID string
	Model     string
}

// Intent is the complete v0.1 semantic request. Sampling controls and runtime
// endpoints are deliberately absent: v0.1 maps one prompt to one fixed model.
type Intent struct {
	Model  string `json:"model"`
	Prompt string `json:"prompt"`
}

// Mapper is immutable after construction and safe for concurrent use.
type Mapper struct {
	routes map[string]struct{}
}

var _ edge.ProfileInvocationMapper = (*Mapper)(nil)

func NewMapper(routes []Route) (*Mapper, error) {
	if len(routes) == 0 || len(routes) > MaxRoutes {
		return nil, fmt.Errorf("text-generation routes must contain 1..%d entries", MaxRoutes)
	}
	allowed := make(map[string]struct{}, len(routes))
	for _, route := range routes {
		if !serviceIDPattern.MatchString(route.ServiceID) || !validModel(route.Model) {
			return nil, errors.New("invalid text-generation route")
		}
		key := routeKey(route.ServiceID, route.Model)
		if _, duplicate := allowed[key]; duplicate {
			return nil, errors.New("duplicate text-generation route")
		}
		allowed[key] = struct{}{}
	}
	return &Mapper{routes: allowed}, nil
}

// NewMapperFromCapabilities derives routes only from validated Worker
// capabilities that implement this profile's operation and explicitly admit
// external-service work. The input scan and resulting immutable map are both
// hard-bounded.
func NewMapperFromCapabilities(
	capabilities []airuntime.Capability,
) (*Mapper, error) {
	if len(capabilities) == 0 || len(capabilities) > MaxRoutes {
		return nil, fmt.Errorf(
			"text-generation capabilities must contain 1..%d entries",
			MaxRoutes,
		)
	}
	routes := make([]Route, 0, len(capabilities))
	for _, capability := range capabilities {
		if err := airuntime.ValidateCapability(capability); err != nil {
			return nil, errors.New("invalid Worker capability for profile routing")
		}
		if capability.Operation != Operation ||
			!acceptsExternalService(capability.AcceptedPriorities) {
			continue
		}
		routes = append(routes, Route{
			ServiceID: capability.ServiceID,
			Model:     capability.Model,
		})
	}
	if len(routes) == 0 {
		return nil, errors.New("no externally callable text-generation capability")
	}
	return NewMapper(routes)
}

// Registration returns the one exact Edge registration implemented by this
// mapper. The profile has no v0.1 critical extensions.
func (m *Mapper) Registration() (edge.ProfileInvocationRegistration, error) {
	if m == nil || len(m.routes) == 0 {
		return edge.ProfileInvocationRegistration{}, errors.New("invalid text-generation mapper")
	}
	return edge.ProfileInvocationRegistration{
		ProfileID: ProfileID, ProfileVersion: ProfileVersion,
		Operation: Operation, Mapper: m,
	}, nil
}

func (m *Mapper) MapInvocation(
	ctx context.Context,
	input edge.ProfileInvocationInput,
) (edge.ProfileInvocationOutput, error) {
	if ctx == nil {
		return edge.ProfileInvocationOutput{}, errors.New("nil text-generation context")
	}
	if err := ctx.Err(); err != nil {
		return edge.ProfileInvocationOutput{}, err
	}
	if m == nil || len(m.routes) == 0 ||
		input.ProfileID != ProfileID || input.ProfileVersion != ProfileVersion ||
		input.Operation != Operation || len(input.ProfileExtensions) != 0 {
		return edge.ProfileInvocationOutput{}, errors.New("unsupported text-generation profile selector")
	}
	if input.MaxInputBytes == 0 || input.MaxOutputBytes == 0 ||
		len(input.Intent) == 0 || len(input.Intent) > MaxIntentBytesHard ||
		uint64(len(input.Intent)) > input.MaxInputBytes {
		return edge.ProfileInvocationOutput{}, errors.New("text-generation intent exceeds bounds")
	}
	intent, err := decodeIntent(input.Intent)
	if err != nil {
		return edge.ProfileInvocationOutput{}, err
	}
	if _, allowed := m.routes[routeKey(input.ServiceID, intent.Model)]; !allowed {
		return edge.ProfileInvocationOutput{}, errors.New("text-generation route is not configured")
	}
	if err := ctx.Err(); err != nil {
		return edge.ProfileInvocationOutput{}, err
	}
	return edge.ProfileInvocationOutput{
		Model: intent.Model, Payload: []byte(intent.Prompt),
	}, nil
}

func decodeIntent(data []byte) (Intent, error) {
	if !utf8.Valid(data) {
		return Intent{}, errors.New("text-generation intent is not UTF-8")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	token, err := decoder.Token()
	if err != nil || token != json.Delim('{') {
		return Intent{}, errors.New("text-generation intent must be an object")
	}
	var output Intent
	seen := make(map[string]struct{}, 2)
	for decoder.More() {
		keyToken, err := decoder.Token()
		if err != nil {
			return Intent{}, errors.New("invalid text-generation intent")
		}
		key, ok := keyToken.(string)
		if !ok {
			return Intent{}, errors.New("invalid text-generation intent key")
		}
		if _, duplicate := seen[key]; duplicate {
			return Intent{}, errors.New("duplicate text-generation intent field")
		}
		seen[key] = struct{}{}
		switch key {
		case "model":
			if err := decoder.Decode(&output.Model); err != nil {
				return Intent{}, errors.New("invalid text-generation model")
			}
		case "prompt":
			if err := decoder.Decode(&output.Prompt); err != nil {
				return Intent{}, errors.New("invalid text-generation prompt")
			}
		default:
			return Intent{}, errors.New("unknown text-generation intent field")
		}
	}
	if token, err = decoder.Token(); err != nil || token != json.Delim('}') {
		return Intent{}, errors.New("invalid text-generation intent")
	}
	if err := ensureJSONEOF(decoder); err != nil {
		return Intent{}, err
	}
	if len(seen) != 2 || !validModel(output.Model) || output.Prompt == "" ||
		strings.IndexByte(output.Prompt, 0) >= 0 ||
		len(output.Prompt) > MaxIntentBytesHard {
		return Intent{}, errors.New("invalid text-generation intent fields")
	}
	return output, nil
}

func ensureJSONEOF(decoder *json.Decoder) error {
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return errors.New("text-generation intent has trailing data")
	}
	return nil
}

func validModel(model string) bool {
	return model != "" && len(model) <= MaxModelBytes && utf8.ValidString(model) &&
		strings.IndexFunc(model, unicode.IsControl) < 0
}

func routeKey(serviceID, model string) string {
	return serviceID + "\x00" + model
}

func acceptsExternalService(priorities []airuntime.Priority) bool {
	for _, priority := range priorities {
		if priority == airuntime.PriorityExternalService {
			return true
		}
	}
	return false
}
