package sink_test

import (
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestSink(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Sink Suite")
}
