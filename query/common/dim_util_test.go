package common

import (
	"github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = ginkgo.Describe("dim utils", func() {
	ginkgo.It("DimValResVectorSize should work", func() {
		Ω(DimValResVectorSize(3, DimCountsPerDimWidth{0, 0, 1, 1, 1})).Should(Equal(30))
		Ω(DimValResVectorSize(3, DimCountsPerDimWidth{0, 0, 2, 1, 1})).Should(Equal(45))
		Ω(DimValResVectorSize(3, DimCountsPerDimWidth{0, 0, 1, 0, 0})).Should(Equal(15))
		Ω(DimValResVectorSize(3, DimCountsPerDimWidth{0, 0, 1, 1, 0})).Should(Equal(24))
		Ω(DimValResVectorSize(3, DimCountsPerDimWidth{0, 0, 1, 0, 1})).Should(Equal(21))
		Ω(DimValResVectorSize(3, DimCountsPerDimWidth{0, 0, 0, 1, 1})).Should(Equal(15))
		Ω(DimValResVectorSize(3, DimCountsPerDimWidth{0, 0, 0, 1, 0})).Should(Equal(9))
		Ω(DimValResVectorSize(3, DimCountsPerDimWidth{0, 0, 0, 0, 1})).Should(Equal(6))
		Ω(DimValResVectorSize(0, DimCountsPerDimWidth{0, 0, 1, 1, 1})).Should(Equal(0))
	})
})
