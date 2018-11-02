//  Copyright (c) 2017-2018 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package utils

import (
	"reflect"
	"regexp"
	"runtime"
	"strings"
)

var re = regexp.MustCompile("[^0-9a-zA-Z_\\-.]+")

// GetFuncName returns the function name given a function pointer. It will only return
// the last part of the name.
func GetFuncName(f interface{}) string {
	fullFuncName := runtime.FuncForPC(reflect.ValueOf(f).Pointer()).Name()
	paths := strings.Split(fullFuncName, "/")
	parts := strings.SplitN(paths[len(paths)-1], ".", 2)
	return re.ReplaceAllString(parts[1], "")
}
