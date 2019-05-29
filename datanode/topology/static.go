package topology

import (
	xwatch "github.com/m3db/m3/src/x/watch"
)

type staticInitializer struct {
	opts StaticOptions
}

// NewStaticInitializer creates a static topology initializer.
func NewStaticInitializer(opts StaticOptions) Initializer {
	return staticInitializer{opts}
}

func (i staticInitializer) Init() (Topology, error) {
	if err := i.opts.Validate(); err != nil {
		return nil, err
	}
	return NewStaticTopology(i.opts), nil
}

func (i staticInitializer) TopologyIsSet() (bool, error) {
	// Always has the specified static topology ready.
	return true, nil
}

type staticTopology struct {
	w xwatch.Watchable
}

// NewStaticTopology creates a static topology.
func NewStaticTopology(opts StaticOptions) Topology {
	w := xwatch.NewWatchable()
	w.Update(NewStaticMap(opts))
	return &staticTopology{w: w}
}

func (t *staticTopology) Get() Map {
	return t.w.Get().(Map)
}

func (t *staticTopology) Watch() (MapWatch, error) {
	// Topology is static, the returned watch will not receive any updates.
	_, w, err := t.w.Watch()
	if err != nil {
		return nil, err
	}
	return NewMapWatch(w), nil
}

func (t *staticTopology) Close() {}
