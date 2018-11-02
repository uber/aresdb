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

import "flag"

// IntFlag creates a int based command option.
func IntFlag(longName, shortName string, defaultValue int, description string) *int {
	res := flag.Int(longName, defaultValue, description)
	flag.IntVar(res, shortName, defaultValue, description)
	return res
}

// BoolFlag creates a boolean based command option.
func BoolFlag(longName, shortName string, defaultValue bool, description string) *bool {
	res := flag.Bool(longName, defaultValue, description)
	flag.BoolVar(res, shortName, defaultValue, description)
	return res
}

// StringFlag creates a string based command option.
func StringFlag(longName, shortName string, defaultValue string, description string) *string {
	res := flag.String(longName, defaultValue, description)
	flag.StringVar(res, shortName, defaultValue, description)
	return res
}
