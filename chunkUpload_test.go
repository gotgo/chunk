package chunk_test

import (
	"io"

	. "github.com/gotgo/chunk"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

type MockDestination struct {
	fileSize int64
}

func (d *MockDestination) Writer(subfolder string) FolderDestination {
	return d
}
func (d *MockDestination) Reader(subfolder string) FolderSource {
	return d
}

func (d *MockDestination) Write(p []byte) (n int, err error) {
	return len(p), nil
}
func (d *MockDestination) Close() error {
	return nil
}
func (d *MockDestination) Create(filename string) (io.WriteCloser, error) {
	return d, nil
}
func (d *MockDestination) Delete(filename string) error {
	return nil
}
func (d *MockDestination) Uri(filename string) string {
	return filename
}
func (d *MockDestination) Size(filename string) int64 {
	return d.fileSize
}
func (d *MockDestination) Files() ([]FileSource, error) {
	s := make([]FileSource, 1)
	s[0] = &MockFile{size: d.fileSize}
	return s, nil
}
func (d *MockDestination) Remove() error {
	return nil
}

type MockFile struct {
	size int64
}

func (m *MockFile) Name() string {
	return "name"
}
func (m *MockFile) Uri() string {
	return "name"
}
func (m *MockFile) Size() int64 {
	return m.size
}
func (m *MockFile) Open() (io.ReadCloser, error) {
	panic("not implemented")
}

type MockSource struct {
	size int
	err  error
}

func (s *MockSource) Read(p []byte) (n int, err error) {
	sz := s.size
	if err != nil {
		return 0, s.err
	}

	if len(p) >= s.size {
		s.size = 0
		return sz, io.EOF
	} else {
		s.size -= len(p)
		return len(p), nil
	}
}

var _ = Describe("ChunkUpload", func() {

	It("should work", func() {
		c := &ChunkUpload{
			CurrentChunkNumber: 1,
			CurrentChunkSize:   512,
			ChunkSize:          512,
			TotalSize:          1024,
			TotalChunks:        2,
			Identifier:         "abcdefg",
			Filename:           "myfile.txt",
			RelativePath:       "myfile.txt",
		}

		c.Destination = &MockDestination{fileSize: int64(c.ChunkSize)}

		folder, err := c.UploadChunk(&MockSource{size: c.ChunkSize})
		Expect(folder.Filename).To(Equal(c.Identifier))
		Expect(err).To(BeNil())
		Expect(folder.IsComplete()).To(BeFalse())

		c.TotalSize = 512

		folder, err = c.UploadChunk(&MockSource{size: c.ChunkSize})
		Expect(folder.Filename).To(Equal(c.Identifier))
		Expect(err).To(BeNil())
		Expect(folder.IsComplete()).To(BeTrue())
	})

})
