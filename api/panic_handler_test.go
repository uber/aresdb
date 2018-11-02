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
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"

	"github.com/gorilla/mux"
	"github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

// panicProducer is for test panic handler.
var panicProducer = func(w http.ResponseWriter, r *http.Request) {
	panic(errors.New("IntentionalError"))
}

var _ = ginkgo.Describe("HTTPRouter", func() {

	ginkgo.It("PanicHandler should work", func() {
		testRouter := mux.NewRouter()
		testRouter.HandleFunc("/panic", panicProducer)
		testServer := httptest.NewUnstartedServer(WithPanicHandling(testRouter))
		testServer.Start()

		resp, _ := http.Get(fmt.Sprintf("http://%s/panic", testServer.Listener.Addr().String()))
		Ω(resp.StatusCode).Should(Equal(http.StatusInternalServerError))
		respBody, err := ioutil.ReadAll(resp.Body)
		Ω(err).Should(BeNil())
		Ω(respBody).Should(Equal([]byte("IntentionalError")))

		testServer.Close()
	})
})
