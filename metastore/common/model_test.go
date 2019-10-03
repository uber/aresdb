package common

import (
	"github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = ginkgo.Describe("model tests", func() {
	ginkgo.It("enum type check should work", func() {
		c := Column{
			Type: Int8,
		}
		Ω(c.IsEnumColumn()).Should(BeFalse())
		c.Type = SmallEnum
		Ω(c.IsEnumColumn()).Should(BeTrue())
		Ω(c.IsEnumBasedColumn()).Should(BeTrue())
		Ω(c.IsEnumArrayColumn()).Should(BeFalse())

		c.Type = BigEnum
		Ω(c.IsEnumColumn()).Should(BeTrue())
		Ω(c.IsEnumBasedColumn()).Should(BeTrue())
		Ω(c.IsEnumArrayColumn()).Should(BeFalse())

		c.Type = ArraySmallEnum
		Ω(c.IsEnumColumn()).Should(BeFalse())
		Ω(c.IsEnumBasedColumn()).Should(BeTrue())
		Ω(c.IsEnumArrayColumn()).Should(BeTrue())

		c.Type = ArrayBigEnum
		Ω(c.IsEnumColumn()).Should(BeFalse())
		Ω(c.IsEnumBasedColumn()).Should(BeTrue())
		Ω(c.IsEnumArrayColumn()).Should(BeTrue())
	})

	ginkgo.It("EnumCardinality should work", func() {
		Ω(EnumCardinality(SmallEnum)).Should(Equal(256))
		Ω(EnumCardinality(BigEnum)).Should(Equal(65536))
		Ω(EnumCardinality(UUID)).Should(Equal(0))
	})
})
