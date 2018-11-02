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
	"encoding/hex"
	"strings"
)

// NormalizeUUIDString normailizes a uuid string by applying following rules.
// If it's not a valid uuid str, it will raise an error.
// 1. remove 0x prefix if any
// 2. convert to uppercase
// 3. remove -
func NormalizeUUIDString(uuidStr string) (string, error) {

	normalizedStr := uuidStr
	if strings.HasPrefix(normalizedStr, "0x") {
		normalizedStr = normalizedStr[2:]
	}

	normalizedStr = strings.ToUpper(normalizedStr)
	normalizedStr = strings.Replace(normalizedStr, "-", "", -1)
	if _, err := hex.DecodeString(normalizedStr); err != nil {
		return "", StackError(err, "Invalid uuid string: %s", uuidStr)
	}
	return normalizedStr, nil
}
