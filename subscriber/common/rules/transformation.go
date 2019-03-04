package rules

const (
	noOp string = "noop"
)

var (
	// Note: when add new transformation,
	// make sure transformFuncs and numSourcesPerTransformations should always have the same set of keys

	// transformation functions per each transformation
	transformFuncs = map[string]func(from interface{}, ctx map[string]string) (interface{}, error){}

	// default value for each transformation
	// if not defined, the default value will be what is defined in the transformation config
	// if not defined again, the default value will be nil
	defaultsPerTransformation = map[string]interface{}{
		noOp: nil,
	}
)

// NoOp is a transformation preserve value
// but it can rename the source value into different alias
func NoOp(from interface{}, ctx map[string]string) (interface{}, error) {
	return from, nil
}

// Transform converts source to destination data
func (t TransformationConfig) Transform(from interface{}) (to interface{}, err error) {
	transformFunc, found := transformFuncs[t.Type]
	if !found {
		transformFunc = NoOp
	}

	to, err = transformFunc(from, t.Context)
	return
}
