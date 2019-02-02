package utils

import (
	"github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = ginkgo.Describe("hyperloglog utils", func() {
	ginkgo.It("ComputeHLLValue should work", func() {
		tests := [][]interface{}{
			{uint64(0), uint32(3276800)},
			{uint64(0xffffffffffffffff), uint32(16383)},
			{uint64(0xf0f0f0f0f0f0f0f0), uint32(12528)},
			{uint64(0x0f0f0f0f0f0f0f0f), uint32(134927)},
			{uint64(8849112093580131862), uint32(15894)},
			{uint64(720734999560851427), uint32(266211)},
			{uint64(506097522914230528 ^ 1084818905618843912), uint32(329736)},
		}

		for _, test := range tests {
			input := test[0].(uint64)
			expected := test[1].(uint32)
			out := ComputeHLLValue(input)
			Ω(out).Should(Equal(expected))
		}
	})
})
