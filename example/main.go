package main

import (
	"fmt"
	"net/http"
	"os"
	"runtime"

	"github.com/gorilla/handlers"
	"github.com/gotgo/goflow"
	"github.com/gotgo/goflow/flow"
)

var assembler *chunk.FileAssembler
var dest chunk.FolderDestination

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())

	assembler = &chunk.FileAssembler{}
	assembler.Start()
	defer assembler.Stop()

	dest = &chunk.FileDestination{FolderRoot: "/tmp/uploads/complete"}

	m := http.NewServeMux()
	m.HandleFunc("/upload", uploadHandler)
	handler := handlers.LoggingHandler(os.Stdout, m)
	http.ListenAndServe(":3002", handler)
}

func uploadHandler(w http.ResponseWriter, r *http.Request) {
	localFolder := &chunk.FileDestination{FolderRoot: "/tmp/uploads/incomplete"}

	var code int
	var msg string
	var pieces *chunk.ChunkFolder

	if r.Method == "POST" {
		pieces, code, msg = flow.UploadChunk(r, localFolder)
		if pieces != nil && pieces.IsComplete() {
			assembler.Post(&chunk.AssembleFolder{Source: pieces, Destination: dest, Callback: completed, Data: nil})
			uri := dest.Uri(pieces.Filename)
			w.Write([]byte(uri))
		}
	} else if r.Method == "GET" {
		_, code, msg = flow.ChunkAlreadyUploaded(r, localFolder)
	} else {
		panic("unknown method")
	}
	w.WriteHeader(code)
	w.Write([]byte(msg))

}

func completed(outcome *chunk.UploadOutcome) {
	fmt.Printf("complete")
}

func getErrorMessage(e interface{}) string {
	var msg string
	if err, ok := e.(error); ok {
		msg = err.Error()
	} else if str, ok := e.(string); ok {
		msg = str
	} else if _, ok := e.(runtime.Error); ok {
		msg = err.Error()
	} else {
		msg = ""
	}
	return msg
}

type streamHandler func(http.ResponseWriter, *http.Request) error
