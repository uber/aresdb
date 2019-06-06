package broker

import (
	"encoding/json"
	"github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/uber/aresdb/query/common"
)

var _ = ginkgo.Describe("resultMerge", func() {
	ginkgo.It("sum should work same shape", func() {
		runTests([]resultMergeTestCase{
			{
				lhsBytes: []byte(`{
					"1234": {
						"foo": 123,
						"bar": 2
					}
				}`),
				rhsBytes: []byte(`{
					"1234": {
						"foo": 1,
						"bar": 1
					}
				}`),
				agg: Sum,
				expected: []byte(`{
					"1234": {
						"foo": 124,
						"bar": 3
					}
				}`),
			},
			{
				lhsBytes: []byte(`{}`),
				rhsBytes: []byte(`{}`),
				agg: Sum,
				expected: []byte(`{}`),
			},
		})
	})

	ginkgo.It("sum should work different shape", func() {
		runTests([]resultMergeTestCase{
			{
				lhsBytes: []byte(`{
					"1234": {
						"foo": 123
					}
				}`),
				rhsBytes: []byte(`{
					"1234": {
						"foo": 1,
						"bar": 1
					}
				}`),
				agg: Sum,
				expected: []byte(`{
					"1234": {
						"foo": 124,
						"bar": 1
					}
				}`),
			},
			{
				lhsBytes: []byte(`{}`),
				rhsBytes: []byte(`{
					"1234": {
						"foo": 1,
						"bar": 1
					}
				}`),
				agg: Sum,
				expected: []byte(`{
					"1234": {
						"foo": 1,
						"bar": 1
					}
				}`),
			},
			{
				lhsBytes: []byte(`{
					"1234": {
						"foo": 123
					}
				}`),
				rhsBytes: []byte(`{}`),
				agg: Sum,
				expected: []byte(`{
					"1234": {
						"foo": 123
					}
				}`),
			},
		})
	})

	ginkgo.It("count should work same shape", func() {
		runTests([]resultMergeTestCase{
			{
				lhsBytes: []byte(`{
					"1234": {
						"foo": 123,
						"bar": 2
					}
				}`),
				rhsBytes: []byte(`{
					"1234": {
						"foo": 1,
						"bar": 1
					}
				}`),
				agg: Count,
				expected: []byte(`{
					"1234": {
						"foo": 124,
						"bar": 3
					}
				}`),
			},
			{
				lhsBytes: []byte(`{}`),
				rhsBytes: []byte(`{}`),
				agg: Count,
				expected: []byte(`{}`),
			},
		})
	})

	ginkgo.It("count should work different shape", func() {
		runTests([]resultMergeTestCase{
			{
				lhsBytes: []byte(`{
					"1234": {
						"foo": 123
					}
				}`),
				rhsBytes: []byte(`{
					"1234": {
						"foo": 1,
						"bar": 1
					}
				}`),
				agg: Count,
				expected: []byte(`{
					"1234": {
						"foo": 124,
						"bar": 1
					}
				}`),
			},
			{
				lhsBytes: []byte(`{}`),
				rhsBytes: []byte(`{
					"1234": {
						"foo": 1,
						"bar": 1
					}
				}`),
				agg: Count,
				expected: []byte(`{
					"1234": {
						"foo": 1,
						"bar": 1
					}
				}`),
			},
			{
				lhsBytes: []byte(`{
					"1234": {
						"foo": 123
					}
				}`),
				rhsBytes: []byte(`{}`),
				agg: Count,
				expected: []byte(`{
					"1234": {
						"foo": 123
					}
				}`),
			},
		})
	})

	ginkgo.It("max should work same shape", func() {
		runTests([]resultMergeTestCase{
			{
				lhsBytes: []byte(`{
					"1234": {
						"foo": 2,
						"bar": 1
					}
				}`),
				rhsBytes: []byte(`{
					"1234": {
						"foo": 1,
						"bar": 2
					}
				}`),
				agg: Max,
				expected: []byte(`{
					"1234": {
						"foo": 2,
						"bar": 2
					}
				}`),
			},
			{
				lhsBytes: []byte(`{}`),
				rhsBytes: []byte(`{}`),
				agg: Max,
				expected: []byte(`{}`),
			},
		})
	})

	ginkgo.It("max should work different shape", func() {
		runTests([]resultMergeTestCase{
			{
				lhsBytes: []byte(`{
					"1234": {
						"foo": 2
					}
				}`),
				rhsBytes: []byte(`{
					"1234": {
						"foo": 1,
						"bar": 1
					}
				}`),
				agg: Max,
				expected: []byte(`{
					"1234": {
						"foo": 2,
						"bar": 1
					}
				}`),
			},
			{
				lhsBytes: []byte(`{}`),
				rhsBytes: []byte(`{
					"1234": {
						"foo": 1,
						"bar": 1
					}
				}`),
				agg: Max,
				expected: []byte(`{
					"1234": {
						"foo": 1,
						"bar": 1
					}
				}`),
			},
			{
				lhsBytes: []byte(`{
					"1234": {
						"foo": 123
					}
				}`),
				rhsBytes: []byte(`{}`),
				agg: Max,
				expected: []byte(`{
					"1234": {
						"foo": 123
					}
				}`),
			},
		})
	})

	ginkgo.It("min should work same shape", func() {
		runTests([]resultMergeTestCase{
			{
				lhsBytes: []byte(`{
					"1234": {
						"foo": 2,
						"bar": 1
					}
				}`),
				rhsBytes: []byte(`{
					"1234": {
						"foo": 1,
						"bar": 2
					}
				}`),
				agg: Min,
				expected: []byte(`{
					"1234": {
						"foo": 1,
						"bar": 1
					}
				}`),
			},
			{
				lhsBytes: []byte(`{}`),
				rhsBytes: []byte(`{}`),
				agg: Min,
				expected: []byte(`{}`),
			},
		})
	})

	ginkgo.It("min should work different shape", func() {
		runTests([]resultMergeTestCase{
			{
				lhsBytes: []byte(`{
					"1234": {
						"foo": 2
					}
				}`),
				rhsBytes: []byte(`{
					"1234": {
						"foo": 1,
						"bar": 1
					}
				}`),
				agg: Min,
				expected: []byte(`{
					"1234": {
						"foo": 1,
						"bar": 1
					}
				}`),
			},
			{
				lhsBytes: []byte(`{}`),
				rhsBytes: []byte(`{
					"1234": {
						"foo": 1,
						"bar": 1
					}
				}`),
				agg: Min,
				expected: []byte(`{
					"1234": {
						"foo": 1,
						"bar": 1
					}
				}`),
			},
			{
				lhsBytes: []byte(`{
					"1234": {
						"foo": 123
					}
				}`),
				rhsBytes: []byte(`{}`),
				agg: Min,
				expected: []byte(`{
					"1234": {
						"foo": 123
					}
				}`),
			},
		})
	})

	ginkgo.It("avg should work same shape", func() {
		runTests([]resultMergeTestCase{
			{
				lhsBytes: []byte(`{
					"1234": {
						"foo": 2,
						"bar": 1
					}
				}`),
				rhsBytes: []byte(`{
					"1234": {
						"foo": 1,
						"bar": 2
					}
				}`),
				agg: Avg,
				expected: []byte(`{
					"1234": {
						"foo": 2,
						"bar": 0.5
					}
				}`),
			},
			{
				lhsBytes: []byte(`{}`),
				rhsBytes: []byte(`{}`),
				agg: Avg,
				expected: []byte(`{}`),
			},
		})
	})

	ginkgo.It("avg should error different shape", func() {
		runTests([]resultMergeTestCase{
			{
				lhsBytes: []byte(`{
					"1234": {
						"foo": 2
					}
				}`),
				rhsBytes: []byte(`{
					"1234": {
						"foo": 1,
						"bar": 1
					}
				}`),
				agg: Avg,
				errPattern: "error calculating avg",
			},
			{
				lhsBytes: []byte(`{}`),
				rhsBytes: []byte(`{
					"1234": {
						"foo": 1,
						"bar": 1
					}
				}`),
				agg: Avg,
				errPattern: "error calculating avg",
			},
			{
				lhsBytes: []byte(`{
					"1234": {
						"foo": 123
					}
				}`),
				rhsBytes: []byte(`{}`),
				agg: Avg,
				errPattern: "error calculating avg",
			},
		})
	})


})

type resultMergeTestCase struct {
	lhsBytes []byte
	rhsBytes []byte
	agg AggType
	expected []byte
	errPattern string
}

func runTests(cases []resultMergeTestCase) {
	for _, tc := range cases {
		var lhs, rhs common.AQLQueryResult
		json.Unmarshal(tc.lhsBytes, &lhs)
		json.Unmarshal(tc.rhsBytes, &rhs)
		ctx := newResultMergeContext(tc.agg)
		result := ctx.run(lhs, rhs)
		if "" == tc.errPattern {
			Ω(ctx.err).Should(BeNil())
			bs, err := json.Marshal(result)
			Ω(err).Should(BeNil())
			Ω(bs).Should(MatchJSON(tc.expected))
		} else {
			Ω(ctx.err.Error()).Should(ContainSubstring(tc.errPattern))
		}
	}
}