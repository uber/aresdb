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
	"net/http"
	"net/http/httptest"

	"github.com/gorilla/mux"
	"github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/uber-go/tally"
	"github.com/uber/aresdb/common"
)

func testHTTPHandlerFunc(w http.ResponseWriter, r *http.Request) {
}

var _ = ginkgo.Describe("http", func() {
	ginkgo.It("NoCache should work", func() {
		httpHandlerFunc := func(w http.ResponseWriter, r *http.Request) {
			// etagHeaders should be deleted.
			for k := range r.Header {
				Ω(etagHeaders).ShouldNot(HaveKey(k))
			}

			for k, v := range noCacheHeaders {
				Ω(w.Header().Get(k)).Should(Equal(v))
			}
		}

		r := httptest.NewRequest(http.MethodGet, "https://localhost/test", nil)
		w := httptest.NewRecorder()
		for _, k := range etagHeaders {
			r.Header.Add(k, "1")
		}

		NoCache(http.HandlerFunc(httpHandlerFunc)).ServeHTTP(w, r)
	})

	ginkgo.It("WithMetricsFunc should work", func() {
		r := httptest.NewRequest(http.MethodGet, "https://localhost/test", nil)
		w := httptest.NewRecorder()
		for _, k := range etagHeaders {
			r.Header.Add(k, "1")
		}
		WithMetricsFunc(testHTTPHandlerFunc).ServeHTTP(w, r)
		testScope := GetRootReporter().GetRootScope().(tally.TestScope)
		Ω(testScope.Snapshot().Counters()).
			Should(HaveKey("test.http.call+component=api,handler=testHTTPHandlerFunc,origin=UNKNOWN,status_code=200"))
		Ω(testScope.Snapshot().Timers()).
			Should(HaveKey("test.http.latency+component=api,handler=testHTTPHandlerFunc,origin=UNKNOWN"))
	})

	ginkgo.It("GetOrigin should work", func() {
		r := &http.Request{}
		Ω(GetOrigin(r)).Should(Equal("UNKNOWN"))

		r.Header = make(http.Header)
		r.Header.Set("X-Uber-Origin", "test1")
		Ω(GetOrigin(r)).Should(Equal("test1"))

		r.Header.Set("RPC-Caller", "test2")
		Ω(GetOrigin(r)).Should(Equal("test2"))
	})

	ginkgo.It("LimitServerImmediateReturn should work", func() {
		r := mux.NewRouter()
		r.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte("Good!"))
		}).Methods("GET")

		cfg := common.HTTPConfig{
			MaxConnections:          10,
			MaxIngestionConnections: 10,
			MaxQueryConnections:     10,
			ReadTimeOutInSeconds:    1,
			WriteTimeOutInSeconds:   1,
		}
		_, server := LimitServeImmediateReturn(9374, r, cfg)
		resp, err := http.Get("http://localhost:9374")
		Ω(err).Should(BeNil())
		Ω(resp.Status).Should(Equal("200 OK"))
		err = server.Close()
		Ω(err).Should(BeNil())
	})
})
