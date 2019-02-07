package api

import (
	"github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"net/http"
	"bytes"
)

var _ = ginkgo.Describe("ReadRequest function", func() {

	ginkgo.It("it should work for api request", func() {
		query := `
	{
      "queries":[
		{
          "measures": [
            {
              "sqlExpression": "hll(driver_uuid_hll)"
            }
          ],
          "rowFilters": [
            "trips.status = 'completed'"
          ],
          "table": "trips",
          "timeFilter": {
            "column": "trips.request_at",
            "from": "2018-09-18",
            "to": "2018-09-24"
          },
          "dimensions": [
            {
              "sqlExpression": "request_at",
              "timeBucketizer": "day",
              "timeUnit": "second"
            }
          ]
        }
      ]
	}
	`
		bts := []byte(query)
		r, err := http.NewRequest(http.MethodPost, "localhost:19374", bytes.NewBuffer(bts))
		Ω(err).Should(BeNil())
		r.Header.Set("Accept", "application/hll")
		aqlR := AQLRequest{}
		err = ReadRequest(r, &aqlR)
		Ω(err).Should(BeNil())
		Ω(aqlR.Accept).Should(Equal(ContentTypeHyperLogLog))
	})

	ginkgo.It("it should work for schema requests", func() {
		query := `
	{
        "name": "some_table",
        "columns": [
            {
                "name": "event_time_field",
                "type": "Uint32",
                "config": {
                    "preloadingDays": 52
                }
            },
            {
                "name": "uuid_field",
                "type": "UUID",
                "config": {}
            }
        ],
        "primaryKeyColumns": [
            1
        ],
        "isFactTable": true,
        "archivingSortColumns": [],
        "version": 0
    }
	`
		bts := []byte(query)
		r, err := http.NewRequest(http.MethodPost, "localhost:19374", bytes.NewBuffer(bts))
		Ω(err).Should(BeNil())
		var addTableRequest AddTableRequest
		err = ReadRequest(r, &addTableRequest)
		Ω(err).Should(BeNil())
		Ω(addTableRequest.Body.Config).Should(BeNil())
	})

})
