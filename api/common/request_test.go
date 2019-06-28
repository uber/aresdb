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

package common

import (
	"bytes"
	"github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/uber/aresdb/utils"
	"net/http"
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
		Ω(aqlR.Accept).Should(Equal(utils.HTTPContentTypeHyperLogLog))
	})

})
