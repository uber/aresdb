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

package tree

import "strings"

// QualifiedName is column QualifiedName
type QualifiedName struct {
	// Parts is list of string
	Parts []string
	// OriginalParts is list of string
	OriginalParts []string
}

// NewQualifiedName creates QualifiedName
func NewQualifiedName(originalParts, parts []string) *QualifiedName {
	return &QualifiedName{
		Parts:         parts,
		OriginalParts: originalParts,
	}
}

// String returns string
func (n *QualifiedName) String() string {
	return strings.Join(n.Parts, ".")
}
