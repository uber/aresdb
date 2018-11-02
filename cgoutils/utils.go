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

package cgoutils

// #include <stdlib.h>
import "C"
import (
	"code.uber.internal/data/ares/utils"
	"unsafe"
)

// DoCGoCall is the function wrapper to call a cgo function, check whether there is any exception thrown by the function
// and converted it to a golang error if any.
func DoCGoCall(f func() (uintptr, unsafe.Pointer)) uintptr {
	res, pStrErr := f()
	if pStrErr != nil {
		errMsg := C.GoString((*C.char)(pStrErr))
		C.free(pStrErr)
		panic(utils.StackError(nil, errMsg))
	}
	return res
}
