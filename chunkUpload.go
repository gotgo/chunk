package chunk

import (
	"io"
	"strconv"

	"github.com/gotgo/fw/me"
)

//Need Server Affinity for saving chunks to the local file system.

type ChunkUpload struct {
	CurrentChunkNumber int //flowChunkNumber
	CurrentChunkSize   int
	ChunkSize          int
	TotalSize          int64
	Identifier         string
	Filename           string
	RelativePath       string
	TotalChunks        int
	Destination        Destination
}

func (u *ChunkUpload) chunkFolderName() string {
	return u.Identifier
}

func (u *ChunkUpload) filename() string {
	return strconv.Itoa(u.CurrentChunkNumber)
}

func (u *ChunkUpload) ChunkAlreadyUploaded() bool {
	d := u.Destination.Writer(u.chunkFolderName())
	filePath := u.filename()
	return int64(u.CurrentChunkSize) == d.Size(filePath)
}

func (u *ChunkUpload) UploadChunk(src io.Reader) (*ChunkFolder, error) {
	d := u.Destination.Writer(u.chunkFolderName())
	dstPath := u.filename()
	dst, err := d.Create(dstPath)
	if err != nil {
		return nil, me.Err(err, "failed to create file for chunk")
	}

	var copied int64
	if copied, err = io.Copy(dst, src); err != nil {
		_ = dst.Close()
		_ = d.Delete(dstPath) //remove tainted file
		return nil, me.Err(err, "failed to copy source file to destinationfile", &me.KV{"dest", dst}, &me.KV{"source", "http multi part"})
	}

	if copied != int64(u.CurrentChunkSize) {
		_ = dst.Close()
		_ = d.Delete(dstPath)
		return nil, me.NewErr("actual chunk size not the same as the advertised CurrentChunkSize",
			&me.KV{"CurrentChunkSize", u.CurrentChunkSize},
			&me.KV{"copied", copied})
	}

	if err = dst.Close(); err != nil {
		_ = d.Delete(dstPath) //remove possibly tainted file
		return nil, me.Err(err, "failed to close destination")
	}

	//sum of uploaded files
	//get list of uploaded file chunks
	s := u.Destination.Reader(u.chunkFolderName())
	sum, err := sumSizes(s.Files()) //sizes
	if err != nil {
		return nil, me.Err(err, "unable to get list of uploaded chunk files", &me.KV{"path", dstPath})
	}

	filename := u.chunkFolderName()
	folder := &ChunkFolder{
		Filename: filename,
	}

	folder.FolderSource = s
	if sum == u.TotalSize {
		folder.isComplete = true
	}
	return folder, nil
}

// sumSizes - given a list of file infos, what is the sum of the file size across all files
func sumSizes(fileInfos []FileSource, err error) (int64, error) {
	if err != nil {
		return 0, err
	}

	var sum int64
	for _, fi := range fileInfos {
		sum += fi.Size()
	}
	return sum, nil
}
