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

const (
	// use lower 14 bits in hash as group
	groupBits = 14
	// max 16 bits for group
	maxGroupBits = 16
)

// ComputeHLLValue compute hll value based on hash value
func ComputeHLLValue(hash uint64) uint32 {
	group := uint32(hash & ((1 << groupBits) - 1))
	var rho uint32
	for {
		h := hash & (1 << (rho + groupBits))
		if rho+groupBits < 64 && h == 0 {
			rho++
		} else {
			break
		}
	}
	return rho<<maxGroupBits | group
}
