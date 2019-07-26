package bootstrap

import (
	"github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/uber/aresdb/utils"
	"time"
)

var _ = ginkgo.Describe("bootstrap details", func() {

	ginkgo.It("bootstrap details should work", func() {
		utils.SetClockImplementation(func() time.Time {
			return time.Unix(10, 0)
		})

		numColumns := 10
		details := NewBootstrapDetails()
		details.SetNumColumns(10)
		jb, err := details.MarshalJSON()
		Ω(err).Should(BeNil())
		Ω(string(jb)).Should(MatchJSON(`{"stage":"waiting", "source": "", "startedAt": 0, "numColumns":10,"batches":{}}`))

		details.SetBootstrapStage(PeerCopy)
		details.SetSource("test")
		for columnID := 0; columnID < numColumns; columnID++ {
			details.AddVPToCopy(0, uint32(columnID))
		}

		jb, err = details.MarshalJSON()
		Ω(err).Should(BeNil())
		Ω(string(jb)).Should(MatchJSON(`{"stage":"peercopy", "source": "test", "startedAt": 10, "numColumns":10,"batches":{"0":[1,1,1,1,1,1,1,1,1,1]}}`))

		for columnID := 0; columnID < numColumns; columnID++ {
			details.MarkVPFinished(0, uint32(columnID))
		}
		details.SetBootstrapStage(Finished)

		jb, err = details.MarshalJSON()
		Ω(err).Should(BeNil())
		Ω(string(jb)).Should(MatchJSON(`{"stage":"finished", "source": "test", "startedAt": 10, "numColumns":10,"batches":{"0":[2,2,2,2,2,2,2,2,2,2]}}`))

		details.Clear()
		jb, err = details.MarshalJSON()
		Ω(err).Should(BeNil())
		Ω(string(jb)).Should(MatchJSON(`{"stage":"waiting", "source": "test", "startedAt": 10, "numColumns":0,"batches":{}}`))

		utils.ResetClockImplementation()
	})
})
