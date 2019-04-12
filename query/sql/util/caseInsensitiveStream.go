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

package util

import (
	"unicode"

	"github.com/antlr/antlr4/runtime/Go/antlr"
)

// CaseChangingStream wraps an existing CharStream, but upper cases, or
// lower cases the input before it is tokenized.
type CaseChangingStream struct {
	antlr.CharStream

	upper bool
}

// NewCaseChangingStream returns a new CaseChangingStream that forces
// all tokens read from the underlying stream to be either upper case
// or lower case based on the upper argument.
func NewCaseChangingStream(in antlr.CharStream, upper bool) *CaseChangingStream {
	return &CaseChangingStream{
		in, upper,
	}
}

// LA gets the value of the symbol at offset from the current position
// from the underlying CharStream and converts it to either upper case
// or lower case.
func (is *CaseChangingStream) LA(offset int) int {
	in := is.CharStream.LA(offset)
	if in < 0 {
		// Such as antlr.TokenEOF which is -1
		return in
	}
	if is.upper {
		return int(unicode.ToUpper(rune(in)))
	}
	return int(unicode.ToLower(rune(in)))
}
