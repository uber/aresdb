package tools

import (
	"strings"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("util", func() {
	It("GetModulePath", func() {
		dir := GetModulePath("")
		rst := strings.HasSuffix(dir, srcPkg)
		Ω(rst).Should(Equal(true))

	})

	It("ToJSON", func() {
		type Item struct {
			Data string
		}
		item := Item{
			Data: "test data",
		}
		itemJSON := ToJSON(item)
		Ω(itemJSON).Should(Equal("{\"Data\":\"test data\"}"))
	})
})
