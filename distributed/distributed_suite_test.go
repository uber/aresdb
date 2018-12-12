package distributed_test

import (
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestDistributed(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Distributed Suite")
}
