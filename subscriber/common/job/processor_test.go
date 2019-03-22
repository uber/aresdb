package job

import (
	"errors"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("driver", func() {
	p := ProcessorError{
		1,
		int64(13434),
		errors.New("test"),
	}

	It("ErrorToJSON", func() {
		j := p.ErrorToJSON()
		Ω(j).ShouldNot(BeEmpty())
	})
})
