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
package client

import (
	"context"
	"encoding/json"
	"github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	topoMocks "github.com/uber/aresdb/cluster/topology/mocks"
	"github.com/uber/aresdb/query/common"
	"net/http"
	"net/http/httptest"
)

var _ = ginkgo.Describe("datanode query client", func() {
	aqlResult := common.AQLQueryResult{
		"foo": float64(1),
	}
	var server *httptest.Server

	ginkgo.AfterEach(func() {
		if server != nil {
			server.Close()
		}
	})

	ginkgo.It("should work happy path", func() {
		server = httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			aqlResponseGood := aqlRespBody{
				Results: []common.AQLQueryResult{
					aqlResult,
				},
			}
			bs, _ := json.Marshal(aqlResponseGood)
			rw.Write(bs)
		}))
		add := "http://" + server.Listener.Addr().String()
		mockHost := topoMocks.Host{}
		mockHost.On("Address").Return(add)

		client := NewDataNodeQueryClient()
		res, err := client.Query(context.TODO(), "", &mockHost, common.AQLQuery{}, false)
		Ω(err).Should(BeNil())
		Ω(res).Should(Equal(aqlResult))
	})

	ginkgo.It("should fail status code not ok", func() {
		server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			rw.WriteHeader(500)
		}))
		add := "http://" + server.Listener.Addr().String()
		mockHost := topoMocks.Host{}
		mockHost.On("Address").Return(add)

		client := NewDataNodeQueryClient()
		_, err := client.Query(context.TODO(), "", &mockHost, common.AQLQuery{}, false)
		Ω(err.Error()).Should(ContainSubstring("got status code"))
	})

	ginkgo.It("should fail bad body", func() {
		server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			aqlResponseBad := struct {
				RandomField []string `json:"someField"`
			}{
				RandomField: []string{"hello", "world"},
			}
			bs, _ := json.Marshal(aqlResponseBad)
			rw.Write(bs)
		}))
		add := "http://" + server.Listener.Addr().String()
		mockHost := topoMocks.Host{}
		mockHost.On("Address").Return(add)

		client := NewDataNodeQueryClient()
		_, err := client.Query(context.TODO(), "", &mockHost, common.AQLQuery{}, false)
		Ω(err.Error()).Should(ContainSubstring("invalid response from datanode"))
	})

	ginkgo.It("should return expected error on connection failure", func() {
		add := "http://localhost:9999"
		mockHost := topoMocks.Host{}
		mockHost.On("Address").Return(add)

		client := NewDataNodeQueryClient()
		_, err := client.Query(context.TODO(), "", &mockHost, common.AQLQuery{}, false)
		Ω(err).Should(Equal(ErrFailedToConnect))
	})
})
