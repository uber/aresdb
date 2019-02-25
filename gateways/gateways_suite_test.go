package gateways_test

import (
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestGateways(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Gateways Suite")
}
