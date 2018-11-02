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

// IndexOfStr finds the index of a target string inside a slice of strings,
// return -1 if not found
func IndexOfStr(strs []string, target string) int {
	for id, str := range strs {
		if str == target {
			return id
		}
	}
	return -1
}

// IndexOfInt finds the index of a target int inside a slice of ints,
// return -1 if not found
func IndexOfInt(s []int, e int) int {
	for i, a := range s {
		if a == e {
			return i
		}
	}
	return -1
}
