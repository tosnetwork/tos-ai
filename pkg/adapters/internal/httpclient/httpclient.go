// Package httpclient preserves the adapter-local import surface while the
// shared implementation also serves audited runtime lifecycle backends.
package httpclient

import (
	"net/http"
	"net/url"

	"github.com/tosnetwork/tos-ai/internal/runtimehttp"
)

const (
	MaxConnectionsHard = runtimehttp.MaxConnectionsHard
	MaxHeaderBytesHard = runtimehttp.MaxHeaderBytesHard
	MaxEndpointBytes   = runtimehttp.MaxEndpointBytes
	MaxPlaintextCIDRs  = runtimehttp.MaxPlaintextCIDRs
	MaxResolvedIPsHard = runtimehttp.MaxResolvedIPsHard
)

type Config = runtimehttp.Config

func Build(config Config) (*url.URL, *http.Client, error) {
	return runtimehttp.Build(config)
}

func Endpoint(base *url.URL, suffix string) string {
	return runtimehttp.Endpoint(base, suffix)
}
