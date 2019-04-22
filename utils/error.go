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
	"errors"
	"fmt"
	"runtime"
	"strings"
)

// APIError represents APIError with error code
type APIError struct {
	Code    int    `json:"-"`
	Message string `json:"message"`
	Cause   error  `json:"cause"`
}

func (e APIError) Error() string {
	cause := ""
	if e.Cause != nil {
		cause = e.Cause.Error()
	}
	return fmt.Sprintf("%s\n%s", e.Message, cause)
}

// StackedError contains multiple lines of error messages as well as the stack trace.
type StackedError struct {
	Messages []string `json:"messages"`
	Stack    []string `json:"stack"`
}

func (e *StackedError) Error() string {
	var result string
	for i := len(e.Messages) - 1; i >= 0; i-- {
		result += e.Messages[i]
		result += "\n"
	}
	result += strings.Join(e.Stack, "\n")
	return result
}

// StackError adds one more line of message to err.
// It updates err if it's already a StackedError, otherwise creates a new StackedError
// with the message from err and the stack trace of current goroutine.
func StackError(err error, message string, args ...interface{}) *StackedError {
	if err == nil {
		stack := make([]byte, 0x10000)
		runtime.Stack(stack, false)
		e := &StackedError{
			[]string{fmt.Sprintf(message, args...)},
			strings.Split(string(stack), "\n"),
		}
		e.Stack = e.Stack[:len(e.Stack)-1]
		return e
	}

	e, ok := err.(*StackedError)
	if !ok {
		stack := make([]byte, 0x10000)
		runtime.Stack(stack, false)
		e = &StackedError{
			[]string{err.Error()},
			strings.Split(string(stack), "\n"),
		}
		e.Stack = e.Stack[:len(e.Stack)-1]
	}

	if message != "" {
		e.Messages = append(e.Messages, fmt.Sprintf(message, args...))
	}
	return e
}

// RecoverWrap recover all panics inside the passed in func
func RecoverWrap(call func() error) (err error) {
	defer func() {
		if r := recover(); r != nil {
			// find out exactly what the error was and set err
			switch x := r.(type) {
			case string:
				err = errors.New(x)
			case error:
				err = x
			default:
				err = errors.New("Unknown panic")
			}
		}
	}()

	err = call()
	return
}
