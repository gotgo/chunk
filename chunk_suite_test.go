package chunk_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"testing"
)

func TestChunk(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Chunk Suite")
}
