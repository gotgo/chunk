package chunk

import (
	"io"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"sort"

	"github.com/gotgo/fw/me"
	"github.com/gotgo/fw/util"
)

//TODO: Consider setting the file and not need to pass it every time?

type FileDestination struct {
	FolderRoot string
	subfolder  string
}

func (f *FileDestination) Writer(subfolder string) FolderDestination {
	return f.createCopy(subfolder)
}

func (f *FileDestination) Reader(subfolder string) FolderSource {
	return f.createCopy(subfolder)
}

func (f *FileDestination) createCopy(subfolder string) *FileDestination {
	return &FileDestination{FolderRoot: f.FolderRoot, subfolder: subfolder}
}

func (f *FileDestination) getFolder() string {
	return filepath.Join(util.NotEmpty(f.FolderRoot, "/tmp"), f.subfolder)
}

func (f *FileDestination) getDestinationFile(filePath string) string {
	return filepath.Join(f.getFolder(), filePath)
}

// Size() - Gets the file's size. If file doesn't exist, value is less than zero
func (d *FileDestination) Size(filePath string) int64 {
	path := d.getDestinationFile(filePath)
	if fi, err := os.Stat(path); err == nil {
		return fi.Size()
	}
	return -1
}

func (d *FileDestination) Delete(filePath string) error {
	path := d.getDestinationFile(filePath)
	if path == "/" {
		//ensure we are not deleting the whole system '/'
		panic("wont delete whole filesystem")
	}
	return os.Remove(path)
}

func (fd *FileDestination) Uri(filename string) string {
	return fd.getDestinationFile(filename)
}

func (fd *FileDestination) Create(filename string) (io.WriteCloser, error) {
	folderRoot := path.Dir(fd.getDestinationFile(filename))
	if folderRoot != "" {
		os.MkdirAll(folderRoot, 0774)
	}
	filePath := fd.Uri(filename)

	file, err := os.Create(filePath)
	if err != nil {
		return nil, me.Err(err, "create destination fail", &me.KV{"folderPath", filename}, &me.KV{"filePath", filePath})
	}
	return &FileFlusher{file}, nil
}

// Folder Source

func (f *FileDestination) Remove() error {
	return os.RemoveAll(f.getFolder())
}

func (f *FileDestination) Files() ([]FileSource, error) {
	folderPath := f.getFolder()

	fileInfos, err := ioutil.ReadDir(folderPath)
	if err != nil {
		return nil, me.Err(err, "read folder of chunk files fail", &me.KV{"folderPath", folderPath})
	}

	sort.Sort(ByChunk(fileInfos)) //sort the file names in the correct order for assembly

	source := make([]FileSource, len(fileInfos))
	for i, fi := range fileInfos {
		filePath := path.Join(folderPath, fi.Name())
		source[i] = &FileSystemFile{
			Path: filePath,
		}
	}
	return source, nil
}

////////////////////////////

type FileSystemFile struct {
	Path string
}

func (f *FileSystemFile) Size() int64 {
	if fi, err := os.Stat(f.Path); err == nil {
		return fi.Size()
	}
	return -1
}

// do we need this?
func (f *FileSystemFile) Name() string {
	return filepath.Base(f.Path)
}

func (f *FileSystemFile) Uri() string {
	return f.Path
}

func (f *FileSystemFile) Open() (io.ReadCloser, error) {
	src, err := os.Open(f.Path)
	src.Seek(0, 0)
	if err != nil {
		return nil, me.Err(err, "open fileChunk file failed", &me.KV{"file", f.Path})
	}
	return src, nil
}

////////////////////////////

type FileFlusher struct {
	file *os.File
}

func (fd *FileFlusher) Write(b []byte) (int, error) {
	if fd.file == nil {
		panic("no file to write to")
	}
	return fd.file.Write(b)
}

func (fd *FileFlusher) Close() error {
	file := fd.file
	if file == nil {
		panic("no file to close")
	}

	if err := file.Sync(); err != nil {
		return me.Err(err, "failed to flush destination to disk", &me.KV{"file", file.Name()})
	}

	if err := file.Close(); err != nil {
		return me.Err(err, "failed to close destination file", &me.KV{"file", file.Name()})
	}

	return nil
}

////////////////////////////
