package chunk

import (
	"io"

	"sync"

	"github.com/gotgo/fw/logging"
	"github.com/gotgo/fw/me"
)

// FileAssembler - assembles file chunks into files
type FileAssembler struct {
	Log logging.Logger `inject:""`

	// running - true if running
	running bool
	// toAssemble - path to folder of files to merge
	toAssemble chan *AssembleFolder
	// assembled - path to completely assembled file
	assembled chan *AssembleFolder
	// assembledOnce - used to close the channels
	closeAssembledOnce *sync.Once
	// toAssembleOnce - used to close the channels
	closeToAssembleOnce *sync.Once
	// mu - synchronize access to Start() and Stop()
	mu sync.Mutex
}

// Start - Start Threads to assemble files.
func (fa *FileAssembler) Start() {
	fa.mu.Lock()
	defer fa.mu.Unlock()
	if fa.running {
		return
	}

	fa.closeToAssembleOnce = &sync.Once{}
	fa.closeAssembledOnce = &sync.Once{}
	fa.toAssemble = make(chan *AssembleFolder, 100)
	fa.assembled = make(chan *AssembleFolder, 100)

	//scatter gather - multiple threads writing to the completed channel
	for i := 0; i < assemblerCount; i++ {
		go fa.runAssembler()
	}

	go fa.completer()

	fa.running = true
}

// Stop = stop all new file assembly
func (fa *FileAssembler) Stop() {
	fa.mu.Lock()
	defer fa.mu.Unlock()

	if !fa.running {
		return
	}

	once := fa.closeToAssembleOnce
	once.Do(func() {
		close(fa.toAssemble)
	})

	fa.running = false
}

func (fa *FileAssembler) completer() {
	for outcome := range fa.assembled {
		outcome.Notify()
	}
}

func (fa *FileAssembler) Post(folder *AssembleFolder) {
	fa.toAssemble <- folder
}

func (fa *FileAssembler) runAssembler() {
	for a := range fa.toAssemble {
		a.uri, a.err = fa.doAssemble(a.Source, a.Destination)

		fa.assembled <- a

		//in either case: fail or succeed - delete everything so we can start fresh
		err := a.Source.Remove()
		if err != nil {
			me.LogError(fa.Log, "failed to remove chunk source", err, &logging.KV{"source", a.uri})
		}
	}

	fa.closeAssembledOnce.Do(func() {
		close(fa.assembled)
	})
}

func (fa *FileAssembler) doAssemble(folder *ChunkFolder, destination FolderDestination) (string, error) {
	source, filename := folder, folder.Filename

	writer, err := destination.Create(filename)

	if err != nil || writer == nil {
		return "", me.Err(err, "failed to create destination writer", &me.KV{"filename", filename})
	}

	if err = fa.assemble(source, writer); err != nil {
		destination.Delete(filename) //cleanup
		return "", err
	}

	if err = writer.Close(); err != nil {
		destination.Delete(filename) //delete on error
		return "", me.Err(err, "failed to close writer", &me.KV{"filename", filename})
	}

	//we are only removing on success, so we can see what failed? or should we always cleanup no matter what?
	source.Remove()
	return destination.Uri(filename), nil
}

// assemble - folderPath: the folder of files to make into one file, returns: the file path of the completed file
func (fa *FileAssembler) assemble(source *ChunkFolder, dst io.WriteCloser) error {
	files, err := source.Files()
	if err != nil {
		return err
	}

	//join multiple files into 1 file
	for _, file := range files {
		path := file.Uri()
		src, err := file.Open()
		if err != nil {
			return me.Err(err, "Failed to open file", &me.KV{"file", path})
		}
		defer src.Close()

		bts, err := io.Copy(dst, src)
		if err != nil {
			return me.Err(err, "copy fileChunk into destination stream failed", &me.KV{"fileChunkFile", path})
		}

		if bts == 0 {
			return me.NewErr("no bytes copied", &me.KV{"fileChunkFile", path})
		}
	}

	return nil
}
