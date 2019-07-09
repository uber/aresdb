package bootstrap

import (
	"github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = ginkgo.Describe("bootstrap details", func() {

	ginkgo.It("bootstrap details should work", func() {
		numColumns := 10
		details := NewBootstrapDetails()
		details.SetNumColumns(10)
		jb, err := details.MarshalJSON()
		Ω(err).Should(BeNil())
		Ω(string(jb)).Should(MatchJSON(`{"stage":"waiting","numColumns":10,"batches":{}}`))

		for columnID := 0; columnID < numColumns; columnID++ {
			details.SetBootstrapStage(PeerCopy)
			details.AddVPToCopy(0, uint32(columnID))
		}

		jb, err = details.MarshalJSON()
		Ω(err).Should(BeNil())
		Ω(string(jb)).Should(MatchJSON(`{"stage":"peercopy","numColumns":10,"batches":{"0":[1,1,1,1,1,1,1,1,1,1]}}`))

		for columnID := 0; columnID < numColumns; columnID++ {
			details.MarkVPFinished(0, uint32(columnID))
		}
		details.SetBootstrapStage(Finished)

		jb, err = details.MarshalJSON()
		Ω(err).Should(BeNil())
		Ω(string(jb)).Should(MatchJSON(`{"stage":"finished","numColumns":10,"batches":{"0":[2,2,2,2,2,2,2,2,2,2]}}`))

		details.Clear()
		jb, err = details.MarshalJSON()
		Ω(err).Should(BeNil())
		Ω(string(jb)).Should(MatchJSON(`{"stage":"waiting","numColumns":0,"batches":{}}`))
	})
})
