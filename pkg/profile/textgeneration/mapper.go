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
	edgev1 "github.com/tosnetwork/tos-protocol/gen/tos/edge/v1"
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

// NewProfilePlanFromWorkerCapabilities constructs the exact immutable Edge
// deployment plan advertised by a live, already validated Worker capability
// snapshot. Only externally callable text-generation routes enter the plan;
// unrelated operations and owner-only capabilities cannot become public
// routes. Callers should obtain the snapshot through localrpc.WorkerClient so
// freshness, bounds, resource evidence, and private-RPC response validation
// have already been enforced.
func NewProfilePlanFromWorkerCapabilities(
	capabilities []*edgev1.Capability,
) (*edge.ProfileInvocationPlan, error) {
	if len(capabilities) == 0 || len(capabilities) > MaxRoutes {
		return nil, fmt.Errorf(
			"text-generation Worker capabilities must contain 1..%d entries",
			MaxRoutes,
		)
	}
	routes := make([]Route, 0, len(capabilities))
	for _, capability := range capabilities {
		if capability == nil {
			return nil, errors.New("nil Worker capability")
		}
		if capability.Operation != Operation ||
			!wireAcceptsExternalService(capability.AcceptedPriorities) {
			continue
		}
		routes = append(routes, Route{
			ServiceID: capability.ServiceId,
			Model:     capability.Model,
		})
	}
	if len(routes) == 0 {
		return nil, errors.New(
			"no externally callable text-generation Worker capability",
		)
	}
	mapper, err := NewMapper(routes)
	if err != nil {
		return nil, err
	}
	registration, err := mapper.Registration()
	if err != nil {
		return nil, err
	}
	plan, err := edge.NewProfileInvocationPlan(
		[]edge.ProfileInvocationRegistration{registration},
		[]edge.ProfileInvocationRequirement{{
			ProfileID: ProfileID, ProfileVersion: ProfileVersion,
			Operation: Operation,
		}},
	)
	if err != nil {
		return nil, err
	}
	if !plan.Supports(ProfileID, ProfileVersion, nil, Operation) {
		return nil, errors.New("text-generation Worker profile is not enabled")
	}
	return plan, nil
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
	canonical, err := canonicalIntent(output)
	if err != nil || !bytes.Equal(data, canonical) {
		return Intent{}, errors.New("text-generation intent is not canonical JSON")
	}
	return output, nil
}

// canonicalIntent implements the RFC 8785 JSON Canonicalization Scheme for
// this profile's deliberately tiny value domain: one object containing two
// strings in lexicographic member order. Keeping the encoder local makes the
// exact accepted bytes reviewable and prevents permissive JSON decoding (for
// example, lone UTF-16 surrogates replaced with U+FFFD) from changing the
// paid intent after it was committed.
func canonicalIntent(intent Intent) ([]byte, error) {
	if !utf8.ValidString(intent.Model) || !utf8.ValidString(intent.Prompt) {
		return nil, errors.New("text-generation intent contains invalid UTF-8")
	}
	output := make([]byte, 0, len(intent.Model)+len(intent.Prompt)+24)
	output = append(output, `{"model":`...)
	output = appendCanonicalJSONString(output, intent.Model)
	output = append(output, `,"prompt":`...)
	output = appendCanonicalJSONString(output, intent.Prompt)
	output = append(output, '}')
	return output, nil
}

func appendCanonicalJSONString(output []byte, value string) []byte {
	const hexadecimal = "0123456789abcdef"
	output = append(output, '"')
	for _, character := range value {
		switch character {
		case '"', '\\':
			output = append(output, '\\', byte(character))
		case '\b':
			output = append(output, `\b`...)
		case '\t':
			output = append(output, `\t`...)
		case '\n':
			output = append(output, `\n`...)
		case '\f':
			output = append(output, `\f`...)
		case '\r':
			output = append(output, `\r`...)
		default:
			if character < 0x20 {
				output = append(
					output, '\\', 'u', '0', '0',
					hexadecimal[byte(character)>>4],
					hexadecimal[byte(character)&0x0f],
				)
				continue
			}
			output = utf8.AppendRune(output, character)
		}
	}
	return append(output, '"')
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

func wireAcceptsExternalService(priorities []edgev1.Priority) bool {
	for _, priority := range priorities {
		if priority == edgev1.Priority_PRIORITY_EXTERNAL_SERVICE {
			return true
		}
	}
	return false
}
