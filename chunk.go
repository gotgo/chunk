package chunk

import (
	"io"
	"os"
	"strconv"
)

const uploadFolder = "incomplete"
const bufferSize = 1024*1024 + 4096 //1MB for the file being uploaded, 4096 for the rest of the payload
const maxFileKeySize = 256
const assemblerCount = 2
const delim = "_"

// folder to assemble
type ChunkFolder struct {
	FolderSource

	//final final name
	Filename string

	isComplete bool
}

func (f *ChunkFolder) IsComplete() bool {
	return f.isComplete
}

type UploadOutcome struct {
	Uri  string
	Err  error
	Data interface{}
}

//type Complete struct {
//	Callback func(*UploadOutcome)
//	Data     interface{}
//}

type AssembleFolder struct {
	Callback    func(*UploadOutcome)
	Data        interface{}
	Source      *ChunkFolder
	Destination FolderDestination
	uri         string
	err         error
}

func (o *AssembleFolder) Notify() {
	if c := o.Callback; c != nil {
		c(&UploadOutcome{
			Uri:  o.uri,
			Err:  o.err,
			Data: o.Data,
		})
	}
}

type FileSource interface {
	Name() string
	Uri() string
	Size() int64
	Open() (io.ReadCloser, error)
}

// FOLDER
type FolderSource interface {
	//returns chunks in sorted order
	Files() ([]FileSource, error)
	Remove() error

	//FolderUri() string
}

type FolderDestination interface {
	//Folder Destination
	Create(filename string) (io.WriteCloser, error)
	Delete(filename string) error
	Size(filename string) int64

	//
	Uri(filename string) string
}

type Destination interface {
	FolderDestination
	FolderSource
}

type ByChunk []os.FileInfo

func (a ByChunk) Len() int      { return len(a) }
func (a ByChunk) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a ByChunk) Less(i, j int) bool {
	ai, _ := strconv.Atoi(a[i].Name())
	aj, _ := strconv.Atoi(a[j].Name())
	return ai < aj
}
