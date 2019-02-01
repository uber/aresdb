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
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"

	"github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/uber/aresdb/common"
	memCom "github.com/uber/aresdb/memstore/common"
	metaCom "github.com/uber/aresdb/metastore/common"
	"go.uber.org/zap"
)

var _ = ginkgo.Describe("AresDB connector", func() {
	var hostPort string
	var testServer *httptest.Server
	testTableNames := []string{"a"}
	re := regexp.MustCompile("/schema/tables/a/columns/(.+)/enum-cases")
	testTables := map[string]metaCom.Table{
		"a": {
			Name: "a",
			Columns: []metaCom.Column{
				{
					Name: "col0",
					Type: metaCom.Int32,
				},
				{
					Name: "col1",
					Type: metaCom.Int32,
					HLLConfig: metaCom.HLLConfig{
						Suffix: "",
						Mode: metaCom.HLLEnabled,
					},
				},
				{
					Name: "col1_hll",
					Type: metaCom.Uint32,
				},
				{
					Name: "col2",
					Type: metaCom.BigEnum,
				},
				{
					Name: "col3",
					Type: metaCom.Bool,
				},
				{
					Name:              "col4",
					Type:              metaCom.BigEnum,
					DisableAutoExpand: true,
					CaseInsensitive:   true,
				},
				{
					Name:              "col5",
					Type:              metaCom.BigEnum,
					DisableAutoExpand: true,
					CaseInsensitive:   true,
				},
			},
			PrimaryKeyColumns: []int{1},
			IsFactTable:       true,
		},
	}

	// this is the enum cases at first
	initialColumn2EnumCases := map[string][]string{
		"col2": {"1"},
		"col4": {"a"},
		"col5": {"A"},
	}

	// extendedEnumIDs
	column2extendedEnumIDs := []int{2}

	ginkgo.BeforeEach(func() {
		testServer = httptest.NewUnstartedServer(
			http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if strings.HasSuffix(r.URL.Path, "tables") && r.Method == http.MethodGet {
					tableListBytes, _ := json.Marshal(testTableNames)
					w.WriteHeader(http.StatusOK)
					w.Write(tableListBytes)
				} else if strings.HasSuffix(r.URL.Path, "tables/a") && r.Method == http.MethodGet {
					tableBytes, _ := json.Marshal(testTables["a"])
					w.WriteHeader(http.StatusOK)
					w.Write(tableBytes)
				} else if strings.HasSuffix(r.URL.Path, "enum-cases") {
					if r.Method == http.MethodGet {
						column := string(re.FindSubmatch([]byte(r.URL.Path))[1])
						var enumBytes []byte
						if enumCases, ok := initialColumn2EnumCases[column]; ok {
							enumBytes, _ = json.Marshal(enumCases)
						}

						w.WriteHeader(http.StatusOK)
						w.Write(enumBytes)
					} else if r.Method == http.MethodPost {
						enumIDBytes, _ := json.Marshal(column2extendedEnumIDs)
						w.WriteHeader(http.StatusOK)
						w.Write(enumIDBytes)
					}
				} else if strings.Contains(r.URL.Path, "data") && r.Method == http.MethodPost {
					w.WriteHeader(http.StatusOK)
				}
			}))
		testServer.Start()
		hostPort = testServer.Listener.Addr().String()
	})

	ginkgo.AfterEach(func() {
		testServer.Close()
	})

	ginkgo.It("Insert", func() {
		config := ConnectorConfig{
			Address: hostPort,
		}

		logger := zap.NewExample().Sugar()
		rootScope, _, _ := common.NewNoopMetrics().NewRootScope()

		errConfig := ConnectorConfig{
			Address: "localhost:8888",
		}
		connector, err := errConfig.NewConnector(logger, rootScope)
		Ω(err).ShouldNot(BeNil())

		connector, err = config.NewConnector(logger, rootScope)
		Ω(err).Should(BeNil())

		n, err := connector.Insert("a", []string{"col0", "col1", "col2", "col3", "col4", "col5"}, []Row{
			{100, 1, "1", true, "a", "A"},
			{200, int64(2), "2", false, "A", "a"},
			{300, uint32(3), "2", "1", "b", "B"},
			{400, int32(3), "1", "0", nil, nil},
		})
		Ω(err).Should(BeNil())
		Ω(n).Should(Equal(4))

		// update primary key with addition
		n, err = connector.Insert("a", []string{"col0", "col1", "col2", "col3", "col4", "col5"}, []Row{
			{100, 1, "1", true, "a", "A"},
			{200, int64(2), "2", false, "A", "a"},
			{300, uint32(3), "2", "1", "b", "B"},
			{400, int32(3), "1", "0", nil, nil},
		}, 0, memCom.UpdateWithAddition, 0, 0, 0, 0)
		Ω(err).ShouldNot(BeNil())
		Ω(n).Should(Equal(0))

		// empty rows
		n, err = connector.Insert("a", []string{"col0", "col1", "col2", "col3"}, []Row{})
		Ω(err).Should(BeNil())
		Ω(n).Should(Equal(0))

		// empty column names
		n, err = connector.Insert("a", []string{}, []Row{})
		Ω(err).ShouldNot(BeNil())
		Ω(n).Should(Equal(0))

		// non matching length between column names and row
		n, err = connector.Insert("a", []string{"col0", "col1", "col2", "col3"}, []Row{
			{100, 1, "1", true},
			{200, int64(2)},
			{300, uint32(3), "2"},
			{400, int32(3), "1", "0"},
		})
		Ω(err).ShouldNot(BeNil())
		Ω(n).Should(Equal(0))

		// missing primary key columns
		n, err = connector.Insert("a", []string{"col0", "col2", "col3"}, []Row{
			{100, "1", true},
			{200, "1", "0"},
		})
		Ω(err).ShouldNot(BeNil())
		Ω(n).Should(Equal(0))

		// primary key is nil
		n, err = connector.Insert("a", []string{"col0", "col1", "col2", "col3"}, []Row{
			{100, nil, "1", true},
			{200, int64(2), "1", "0"},
		})
		Ω(err).Should(BeNil())
		Ω(n).Should(Equal(1))

		// missing time column
		n, err = connector.Insert("a", []string{"col1", "col2", "col3"}, []Row{
			{1, "1", true},
			{int64(2), "1", "0"},
		})
		Ω(err).ShouldNot(BeNil())
		Ω(n).Should(Equal(0))

		// time column is nil
		n, err = connector.Insert("a", []string{"col0", "col1", "col2", "col3"}, []Row{
			{nil, 1, "1", true},
			{200, int64(2), "1", "0"},
		})
		Ω(err).Should(BeNil())
		Ω(n).Should(Equal(1))

		// having non-string for enum column
		n, err = connector.Insert("a", []string{"col0", "col1", "col2", "col3"}, []Row{
			{100, 2, 1, true},
			{200, int64(2), "1", "0"},
		})
		Ω(err).Should(BeNil())
		Ω(n).Should(Equal(1))
	})
})
