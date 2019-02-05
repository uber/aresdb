package api

import (
	"github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"net/http"
	"bytes"
)

var _ = ginkgo.Describe("api request", func() {

	ginkgo.It("ReadRequest should work", func() {
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
		Ω(aqlR.Verbose).Should(Equal(1))
	})

})
