package gateways

import (
	"encoding/json"
	mux "github.com/gorilla/mux"
	"github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/uber/aresdb/metastore/common"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
)

var _ = ginkgo.Describe("Controller", func() {
	var testServer *httptest.Server
	var host string
	var port int

	headers := http.Header{
		"Foo": []string{"bar"},
	}

	tables := []common.Table{
		{
			Version: 0,
			Name:    "test1",
			Columns: []common.Column{
				{
					Name: "col1",
					Type: "int32",
				},
			},
		},
	}

	ginkgo.BeforeEach(func() {
		testRouter := mux.NewRouter()
		testServer = httptest.NewUnstartedServer(testRouter)
		testRouter.HandleFunc("/schema/ns1/tables", func(w http.ResponseWriter, r *http.Request) {
			b, _ := json.Marshal(tables)
			w.Write(b)
		})
		testRouter.HandleFunc("/schema/ns_baddata/tables", func(w http.ResponseWriter, r *http.Request) {
			w.Write([]byte(`"bad data`))
		})
		testRouter.HandleFunc("/schema/ns1/hash", func(w http.ResponseWriter, r *http.Request) {
			w.Write([]byte("123"))
		})
		testServer.Start()
		hostPort := testServer.Listener.Addr().String()
		comps := strings.SplitN(hostPort, ":", 2)
		host = comps[0]
		port, _ = strconv.Atoi(comps[1])
	})

	ginkgo.AfterEach(func() {
		testServer.Close()
	})

	ginkgo.It("NewControllerHTTPClient should work", func() {
		c := NewControllerHTTPClient(host, port, headers)
		Ω(c.controllerPort).Should(Equal(port))
		Ω(c.controllerHost).Should(Equal(host))
		Ω(c.headers).Should(Equal(headers))

		hash, err := c.GetSchemaHash("ns1")
		Ω(err).Should(BeNil())
		Ω(hash).Should(Equal("123"))

		tablesGot, err := c.GetAllSchema("ns1")
		Ω(err).Should(BeNil())
		Ω(tablesGot).Should(Equal(tables))
	})

	ginkgo.It("should fail with errors", func() {
		c := NewControllerHTTPClient(host, port, headers)
		_, err := c.GetSchemaHash("bad_ns")
		Ω(err).ShouldNot(BeNil())
		tablesGot, err := c.GetAllSchema("bad_ns")
		Ω(err).ShouldNot(BeNil())
		Ω(tablesGot).Should(BeNil())

		_, err = c.GetAllSchema("ns_baddata")
		Ω(err).ShouldNot(BeNil())
	})
})
