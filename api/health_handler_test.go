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
	"fmt"
	"github.com/uber/aresdb/utils"
	"io/ioutil"
	"net/http"
	"net/http/httptest"

	"github.com/gorilla/mux"
	"github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = ginkgo.Describe("HealthCheck", func() {
	healthCheckHandler := NewHealthCheckHandler()
	var testServer *httptest.Server
	ginkgo.BeforeEach(func() {
		testRouter := mux.NewRouter()
		testRouter.HandleFunc("/health", utils.ApplyHTTPWrappers(healthCheckHandler.HealthCheck))
		testServer = httptest.NewUnstartedServer(WithPanicHandling(testRouter))
		testServer.Start()
	})

	ginkgo.AfterEach(func() {
		testServer.Close()
	})

	ginkgo.It("HealthCheck should work", func() {
		hostPort := testServer.Listener.Addr().String()
		resp, err := http.Post(fmt.Sprintf("http://%s/health", hostPort), "", nil)
		Ω(err).Should(BeNil())
		b, err := ioutil.ReadAll(resp.Body)
		Ω(string(b)).Should(Equal("OK"))
		Ω(err).Should(BeNil())
		Ω(resp.StatusCode).Should(Equal(http.StatusOK))

		healthCheckHandler.disable = true

		resp, err = http.Post(fmt.Sprintf("http://%s/health", hostPort), "", nil)
		Ω(err).Should(BeNil())
		b, err = ioutil.ReadAll(resp.Body)
		Ω(string(b)).Should(Equal("Health check disabled"))
		Ω(err).Should(BeNil())
		Ω(resp.StatusCode).Should(Equal(http.StatusServiceUnavailable))
	})
})
