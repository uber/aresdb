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

#ifndef CGOUTILS_UTILS_H_
#define CGOUTILS_UTILS_H_

// CGoCallResHandle wraps the return result of a CGo function call along with
// the error string if any.
typedef struct {
  void *res;
  const char *pStrErr;
} CGoCallResHandle;

#endif  // CGOUTILS_UTILS_H_
