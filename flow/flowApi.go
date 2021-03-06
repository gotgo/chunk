package flow

import (
	"fmt"
	"net/http"
	"strconv"

	"github.com/gotgo/chunk"
	"github.com/gotgo/fw/me"
	"github.com/gotgo/fw/util"
)

const formFileKey = "file"

//flowChunkNumber
//flowChunkSize
//flowCurrentChunkSize
//flowTotalSize
//flowIdentifier
//flowFilename
//flowRelativePath
//flowTotalChunks

const bufferSize = 1024*1024 + 4096

func ChunkAlreadyUploaded(r *http.Request, d chunk.Destination) (bool, int, string) {
	ul, missingField := FlowParse(r)
	if missingField != "" {
		return false, 400, "bad request - missing data " + missingField
	}

	ul.Destination = d

	exists := ul.ChunkAlreadyUploaded()
	if !exists {
		return false, 404, "not found"
	} else {
		return true, 200, "OK"
	}
}

func UploadChunk(r *http.Request, d chunk.Destination) (*chunk.ChunkFolder, int, string, error) {
	u, missingField := FlowParse(r)
	if missingField != "" {
		return nil, 400, "bad request - missing data " + missingField, nil
	}

	u.Destination = d

	r.ParseMultipartForm(bufferSize)

	if r.MultipartForm == nil {
		return nil, 400, "bad request - no multipart form", nil
	}

	if r.MultipartForm.File == nil {
		return nil, 400, "no files in multipart form", nil
	}

	files := r.MultipartForm.File[formFileKey]

	if len(files) > 1 {
		return nil, 400, "more than 1 file present for key " + formFileKey, nil
	} else if len(files) == 0 {
		return nil, 400, "no file found at multipart key:" + formFileKey, nil
	}

	f, err := files[0].Open()
	if err != nil {
		return nil, 500, "failed to open the submitted file", err
	}
	defer f.Close()
	folder, err := u.UploadChunk(f)

	if err != nil {
		return nil, 500, "failed to upload file", err
	}

	return folder, 200, "OK", nil
}

func FlowParse(r *http.Request) (*chunk.ChunkUpload, string) {
	u := new(chunk.ChunkUpload)
	var err error

	u.ChunkSize, err = requireIntValue(r, "flowChunkSize")
	if err != nil {
		return nil, "flowChunkSize"
	}

	u.CurrentChunkNumber, err = requireIntValue(r, "flowChunkNumber")
	if err != nil {
		return nil, "flowchunkNumber"
	}

	u.CurrentChunkSize, err = requireIntValue(r, "flowCurrentChunkSize")
	if err != nil {
		return nil, "flowCurrentChunkSize"
	}

	u.TotalSize, err = requireInt64Value(r, "flowTotalSize")
	if err != nil {
		return nil, "flowTotalSize"
	}

	flowIdentifier := r.FormValue("flowIdentifier")

	safe, index := util.IsPathSafe(flowIdentifier)
	if !safe {
		return nil, fmt.Sprintf("flowIdentifier invalid character at index %d", index)
	}

	u.Identifier = flowIdentifier
	if u.Identifier == "" {
		return nil, "flowIdentifier"
	}

	u.Filename = r.FormValue("flowFilename")
	u.RelativePath = r.FormValue("flowRelativePath")

	u.TotalChunks, err = requireIntValue(r, "flowTotalChunks")
	if err != nil {
		return nil, "flowTotalChunks"
	}
	return u, ""
}

//http util
func requireIntValue(r *http.Request, name string) (int, error) {
	val := r.FormValue(name)
	if val == "" {
		return 0, me.NewErr(name + " missing")
	}

	v, e := strconv.ParseInt(val, 10, 0)
	return int(v), e
}

func requireInt64Value(r *http.Request, name string) (int64, error) {
	val := r.FormValue(name)
	if val == "" {
		return 0, me.NewErr(name + " missing")
	}

	return strconv.ParseInt(val, 10, 64)
}
