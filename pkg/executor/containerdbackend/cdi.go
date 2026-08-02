package containerdbackend

import (
	"context"
	"errors"

	"github.com/containerd/containerd/v2/core/containers"
	"github.com/containerd/containerd/v2/pkg/oci"
	cdiapi "tags.cncf.io/container-device-interface/pkg/cdi"
)

type cdiInjector struct {
	cache *cdiapi.Cache
}

func newCDIInjector(specDirectories []string) (*cdiInjector, error) {
	if len(specDirectories) == 0 {
		return nil, errors.New("CDI spec directories are required")
	}
	cache, err := cdiapi.NewCache(
		cdiapi.WithAutoRefresh(false),
		cdiapi.WithSpecDirs(specDirectories...),
	)
	if err != nil {
		return nil, errors.New("configure CDI registry")
	}
	return &cdiInjector{cache: cache}, nil
}

func (i *cdiInjector) specOpt(devices []string) oci.SpecOpts {
	fixed := append([]string(nil), devices...)
	return func(
		_ context.Context,
		_ oci.Client,
		_ *containers.Container,
		spec *oci.Spec,
	) error {
		if len(fixed) == 0 {
			return nil
		}
		if i == nil || i.cache == nil || spec == nil {
			return errors.New("CDI injection is unavailable")
		}
		if err := i.cache.Refresh(); err != nil {
			return errors.New("refresh CDI registry")
		}
		if _, err := i.cache.InjectDevices(spec, fixed...); err != nil {
			return errors.New("inject CDI devices")
		}
		return nil
	}
}
