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
	file       *os.File
}

func (f *FileDestination) getPathRoot() string {
	return util.NotEmpty(f.FolderRoot, "/tmp")
}

func (fd *FileDestination) getDestinationFile(filePath string) string {
	return filepath.Clean(path.Join(fd.getPathRoot(), filePath))
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
	if fd.FolderRoot != "" {
		os.MkdirAll(fd.FolderRoot, 0774)
	}
	filePath := fd.Uri(filename)

	file, err := os.Create(filePath)
	fd.file = file
	if err != nil {
		return nil, me.Err(err, "create destination fail", &me.KV{"folderPath", filename}, &me.KV{"filePath", filePath})
	}
	return fd, nil
}

func (fd *FileDestination) Write(b []byte) (int, error) {
	if fd.file == nil {
		panic("no file to write to")
	}
	return fd.file.Write(b)
}

func (fd *FileDestination) Close() error {
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

// Folder Source

func (f *FileDestination) Remove() error {
	return os.RemoveAll(f.FolderRoot)
}

func (f *FileDestination) Files() ([]FileSource, error) {
	folderPath := f.FolderRoot

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
