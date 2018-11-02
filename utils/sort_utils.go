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

// Int64Array is the holder object for []int64 and implementations for Sort interfaces.
type Int64Array []int64

func (s Int64Array) Len() int           { return len(s) }
func (s Int64Array) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s Int64Array) Less(i, j int) bool { return s[i] < s[j] }

// Uint32Array is the holder object for []uint32 and implementations for Sort interfaces.
type Uint32Array []uint32

func (s Uint32Array) Len() int           { return len(s) }
func (s Uint32Array) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s Uint32Array) Less(i, j int) bool { return s[i] < s[j] }
