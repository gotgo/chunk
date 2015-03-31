package chunk

import (
	"io"
	"path"
	"strconv"

	"github.com/gotgo/fw/me"
)

//Need Server Affinity for saving chunks to the local file system.

type ChunkUpload struct {
	CurrentChunkNumber int //flowChunkNumber
	ChunkSize          int
	CurrentChunkSize   int
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

func (u *ChunkUpload) chunkFilePath() string {
	return path.Join(u.chunkFolderName(), strconv.Itoa(u.CurrentChunkNumber))
}

func (u *ChunkUpload) ChunkAlreadyUploaded() bool {
	filePath := u.chunkFilePath()
	return int64(u.CurrentChunkSize) == u.Destination.Size(filePath)
}

func (u *ChunkUpload) UploadChunk(src io.Reader) (*ChunkFolder, error) {
	dstPath := u.chunkFilePath()
	dst, err := u.Destination.Create(dstPath)
	if err != nil {
		return nil, me.Err(err, "failed to create file for chunk")
	}

	// count??
	if _, err = io.Copy(dst, src); err != nil {
		_ = dst.Close()
		_ = u.Destination.Delete(dstPath) //remove tainted file
		return nil, me.Err(err, "failed to copy source file to destinationfile", &me.KV{"dest", dst}, &me.KV{"source", "http multi part"})
	}

	if err = dst.Close(); err != nil {
		_ = u.Destination.Delete(dstPath) //remove possibly tainted file
		return nil, me.Err(err, "failed to close destination")
	}

	//sum of uploaded files
	//get list of uploaded file chunks
	sum, err := sumSizes(u.Destination.Files())
	if err != nil {
		return nil, me.Err(err, "unable to get list of uploaded chunk files", &me.KV{"path", dstPath})
	}

	filename := u.chunkFolderName()
	folder := &ChunkFolder{
		Filename: filename,
	}

	folder.FolderSource = u.Destination
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
