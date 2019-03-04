package tools

import (
	"encoding/json"
	"fmt"
	"go/build"
	"path"
)

var srcPkg = "github.com/uber/aresdb/subscriber"

// GetModulePath will return the source path of the given module
// under the github.com/uber/aresdb/subscriber.
// This function uses the default GOPATH/GOENV resolution to find
// the given module
func GetModulePath(module string) string {
	// use the default search path for searching the package, and thus the empty
	// string for the srcDir argument
	pkg, err := build.Import(path.Join(srcPkg, module), "", build.FindOnly)

	if err != nil {
		panic(fmt.Errorf("Could not find source %v", srcPkg))
	}

	// return the source directory of the package
	return pkg.Dir
}

// ToJSON converts input to JSON format
func ToJSON(data interface{}) string {
	rst, err := json.Marshal(data)
	if err != nil {
		return err.Error()
	}
	return string(rst)
}
