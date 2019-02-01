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

package api

import (
	"github.com/uber/aresdb/query"
)

// AQLRequest represents AQL query request. Debug mode will
// run **each batch** in synchronized mode and report time
// for each step.
// swagger:parameters queryAQL
type AQLRequest struct {
	// in: query
	Device int `query:"device,optional" json:"device"`
	// in: query
	Verbose int `query:"verbose,optional" json:"verbose"`
	// in: query
	Debug int `query:"debug,optional" json:"debug"`
	// in: query
	Profiling string `query:"profiling,optional" json:"profiling"`
	// in: query
	Query string `query:"q,optional" json:"q"`
	// in: query
	DeviceChoosingTimeout int `query:"timeout,optional" json:"timeout"`
	// in: header
	Accept string `header:"Accept" json:"accept"`
	// in: header
	Origin string `header:"Rpc-Caller" json:"origin"`
	// in: body
	Body query.AQLRequest `body:""`
}
