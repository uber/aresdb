package rules

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Transformation", func() {
	tranformationCfg := TransformationConfig{
		Type:    "SmallEnum",
		Source:  "status",
		Default: "ACTIVE",
		Context: map[string]string{},
	}
	It("transform", func() {
		from := "START"
		to, err := tranformationCfg.Transform(from)
		Ω(err).Should(BeNil())
		Ω(to).Should(Equal("START"))
	})
	It("transform with default", func() {
		to, err := tranformationCfg.Transform(nil)
		Ω(err).Should(BeNil())
		Ω(to).Should(BeNil())
	})
})
