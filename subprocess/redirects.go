package subprocess

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os"
	"sync"
	"time"
)

type Redirect struct {
	Mode     RedirectMode
	Filename string
	Pipe     *os.File
	Data     []byte

	MaxOutputSize int64
}

const MAX_MEM_OUTPUT = 1024 * 1024 * 1024

type PipeResultRecorder interface {
	Record(direction int, numBytes int64, err error)
}

type PipeRecordEntry struct {
	Direction int
	Timestamp time.Time
	Bytes     int64
	Error     error
}

type OrderedRecorder struct {
	mu      sync.RWMutex
	entries []PipeRecordEntry
}

func (s *OrderedRecorder) Record(direction int, numBytes int64, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.entries = append(s.entries, PipeRecordEntry{
		Direction: direction,
		Timestamp: time.Now(),
		Bytes:     numBytes,
		Error:     err,
	})
}

func (s *OrderedRecorder) GetEntries() []PipeRecordEntry {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.entries
}

func (d *SubprocessData) SetupOutputMemory(b *bytes.Buffer, maxOutputSize int64) (*os.File, error) {
	reader, writer, e := os.Pipe()
	if e != nil {
		return nil, fmt.Errorf("SetupOutputMemory: os.Pipe: %w", e)
	}

	d.closeAfterStart = append(d.closeAfterStart, writer)

	if maxOutputSize <= 0 {
		maxOutputSize = MAX_MEM_OUTPUT
	}

	d.startAfterStart = append(d.startAfterStart, func() error {
		_, err := io.Copy(b, io.LimitReader(reader, maxOutputSize))
		reader.Close()
		return err
	})

	d.cleanupIfFailed = append(d.cleanupIfFailed, func() {
		reader.Close()
	})
	return writer, nil
}

func (d *SubprocessData) SetupFile(filename string, read bool, maxOutputSize int64, isStdErr bool) (*os.File, error) {
	writer, e := OpenFileForRedirect(filename, read)
	if e != nil {
		return nil, e
	}

	d.closeAfterStart = append(d.closeAfterStart, writer)

	if maxOutputSize < 0 || read {
		return writer, nil
	}

	if maxOutputSize == 0 {
		maxOutputSize = MAX_MEM_OUTPUT
	}

	wcheck, err := OpenFileForCheck(filename)
	if err != nil {
		writer.Close()
		return nil, fmt.Errorf("opening %q for size check: %w", filename, err)
	}

	cw := &outputRedirectCheck{
		n:       filename,
		f:       wcheck,
		maxSize: maxOutputSize,
	}

	if isStdErr {
		d.errCheck = cw
	} else {
		d.outCheck = cw
	}

	return writer, nil
}

func (d *SubprocessData) SetupPipe(f *os.File) (*os.File, error) {
	d.closeAfterStart = append(d.closeAfterStart, f)
	return f, nil
}

func (d *SubprocessData) SetupOutput(w *Redirect, b *bytes.Buffer, isStdErr bool) (*os.File, error) {
	if w == nil {
		return WriterDefault()
	}

	switch w.Mode {
	case REDIRECT_MEMORY:
		return d.SetupOutputMemory(b, w.MaxOutputSize)
	case REDIRECT_FILE:
		return d.SetupFile(w.Filename, false, w.MaxOutputSize, isStdErr)
	case REDIRECT_PIPE:
		return d.SetupPipe(w.Pipe)
	}
	return WriterDefault()
}

func (d *SubprocessData) SetupInputMemory(b []byte) (*os.File, error) {
	reader, writer, e := os.Pipe()
	if e != nil {
		return nil, fmt.Errorf("SetupInputMemory: os.Pipe: %w", e)
	}
	d.closeAfterStart = append(d.closeAfterStart, reader)
	d.startAfterStart = append(d.startAfterStart, func() error {
		_, err := io.Copy(writer, bytes.NewBuffer(b))
		if err1 := writer.Close(); err == nil {
			err = err1
		}
		return err
	})
	return reader, nil
}

func (d *SubprocessData) SetupInputRemote(r io.ReadCloser) (*os.File, error) {
	reader, writer, e := os.Pipe()
	if e != nil {
		return nil, fmt.Errorf("SetupInputRemote: os.Pipe: %w", e)
	}

	// TODO: rewrite resource management to not leak on startup failure.
	d.closeAfterStart = append(d.closeAfterStart, reader)
	d.startAfterStart = append(d.startAfterStart, func() error {
		defer r.Close()
		_, err := io.Copy(writer, r)
		if err1 := writer.Close(); err == nil {
			err = err1
		}
		return err
	})
	d.cleanupIfFailed = append(d.cleanupIfFailed, func() {
		r.Close()
		writer.Close()
	})
	return reader, nil
}

func (d *SubprocessData) SetupInput(w *Redirect) (*os.File, error) {
	if w == nil {
		return ReaderDefault()
	}

	switch w.Mode {
	case REDIRECT_MEMORY:
		return d.SetupInputMemory(w.Data)
	case REDIRECT_PIPE:
		return d.SetupPipe(w.Pipe)
	case REDIRECT_FILE:
		return d.SetupFile(w.Filename, true, 0, false)
	}
	return ReaderDefault()
}

func recordingTee(w io.WriteCloser, r io.ReadCloser, t io.Writer, recorder func(int64, error), wg *sync.WaitGroup) {
	defer r.Close()
	defer w.Close()

	var wc io.Writer = w

	if t != nil {
		wc = io.MultiWriter(t, w) // we want to prioritize t for interaction log
	}
	n, err := copyPipeWithTap(w, r, wc)
	if recorder != nil {
		recorder(n, err)
	}
	if wg != nil {
		wg.Done()
	}
}

func RecordingPipe(d io.Writer, recorder func(int64, error), wg *sync.WaitGroup) (*os.File, *os.File, error) {
	if d == nil && recorder == nil {
		return hackPipe()
	}

	r1, w1, e := hackPipe()
	if e != nil {
		return nil, nil, fmt.Errorf("RecordingPipe: first hackPipe: %w", e)
	}

	r2, w2, e := hackPipe()
	if e != nil {
		r1.Close()
		w1.Close()
		return nil, nil, fmt.Errorf("RecordingPipe: second hackPipe: %w", e)
	}

	var t io.Writer
	if d != nil {
		t = d
	}

	go recordingTee(w1, r2, t, recorder, wg)

	return r1, w2, nil
}

func recordDirection(recorder PipeResultRecorder, direction int) func(int64, error) {
	if recorder == nil {
		return nil
	}
	return func(n int64, err error) {
		recorder.Record(direction, n, err)
	}
}

const maxInteractionLogBytes int64 = 8196 + 1

type InteractionLog struct {
	writer               *bufio.Writer
	mutex                sync.RWMutex
	hadEol               bool
	currentLineDirection int
	written              int64
	maxBytes             int64
}

func (w *InteractionLog) writeBytes(p []byte) (n int, complete bool, err error) {
	if len(p) == 0 {
		return 0, true, nil
	}

	complete = true
	if w.maxBytes > 0 {
		remaining := w.maxBytes - w.written
		if remaining <= 0 {
			return 0, false, nil
		}
		if int64(len(p)) > remaining {
			p = p[:int(remaining)]
			complete = false
		}
	}

	n, err = w.writer.Write(p)
	w.written += int64(n)
	if err != nil {
		return n, false, err
	}
	if n != len(p) {
		return n, false, io.ErrShortWrite
	}
	return n, complete, nil
}

func (w *InteractionLog) write(direction int, p []byte) (n int, err error) {
	if len(p) == 0 {
		return 0, nil
	}

	w.mutex.Lock()
	defer w.mutex.Unlock()
	if w.maxBytes > 0 && w.written >= w.maxBytes {
		return len(p), nil
	}

	eol := []byte("\n")
	// When direction switches without eol written
	switch_without_eol := []byte("\n~")

	var prefix []byte
	if direction == 0 {
		prefix = []byte("< ")
	} else {
		prefix = []byte("> ")
	}

	n = 0
	lines := bytes.Split(p, eol)
	for i, line := range lines {
		var wn int
		var complete bool

		if i+1 >= len(lines) && len(line) == 0 {
			break
		}

		if direction != w.currentLineDirection {
			if !w.hadEol {
				_, complete, err = w.writeBytes(switch_without_eol)
				if err != nil {
					return
				}
				if !complete {
					return len(p), nil
				}
			}

			w.currentLineDirection = direction
			_, complete, err = w.writeBytes(prefix)
			if err != nil {
				return
			}
			if !complete {
				return len(p), nil
			}
		}

		wn, complete, err = w.writeBytes(line)
		n += wn
		if err != nil {
			return
		}
		if !complete {
			return len(p), nil
		}
		if i+1 < len(lines) {
			wn, complete, err = w.writeBytes(eol)
			n += wn
			if err != nil {
				return
			}
			if !complete {
				return len(p), nil
			}
			w.hadEol = true
			w.currentLineDirection = -1
		} else {
			w.hadEol = false
		}
	}
	return n, nil
}

type InteractionLogWriter struct {
	interactionLog *InteractionLog
	direction      int
	d              *os.File
}

func (w *InteractionLogWriter) Write(p []byte) (n int, err error) {
	if w.d != nil {
		n, err = w.d.Write(p)
		if err != nil {
			return
		}
		if n != len(p) {
			err = io.ErrShortWrite
			return
		}
	}
	return w.interactionLog.write(w.direction, p)
}

func Interconnect(s1, s2 *Subprocess, d1, d2, interactionLogFile *os.File, recorder PipeResultRecorder) error {
	var w1 io.Writer
	if d1 != nil {
		w1 = d1
	}
	var w2 io.Writer
	if d2 != nil {
		w2 = d2
	}

	var wg sync.WaitGroup
	wg.Add(2)

	if interactionLogFile != nil {
		writer := bufio.NewWriterSize(interactionLogFile, int(maxInteractionLogBytes))
		interactionLog := &InteractionLog{
			writer:               writer,
			hadEol:               true,
			currentLineDirection: -1,
			maxBytes:             maxInteractionLogBytes,
		}
		go func() {
			wg.Wait()
			err := writer.Flush()
			_ = err // TODO ???
		}()
		w1 = &InteractionLogWriter{
			interactionLog: interactionLog,
			direction:      0,
			d:              d1,
		}
		w2 = &InteractionLogWriter{
			interactionLog: interactionLog,
			direction:      1,
			d:              d2,
		}
	}

	read1, write1, err := RecordingPipe(w1, recordDirection(recorder, 0), &wg)
	if err != nil {
		return err
	}

	read2, write2, err := RecordingPipe(w2, recordDirection(recorder, 1), &wg)
	if err != nil {
		read1.Close()
		write1.Close()
		return err
	}

	s1.StdIn = &Redirect{
		Mode: REDIRECT_PIPE,
		Pipe: read1,
	}
	s2.StdOut = &Redirect{
		Mode: REDIRECT_PIPE,
		Pipe: write1,
	}
	s1.StdOut = &Redirect{
		Mode: REDIRECT_PIPE,
		Pipe: write2,
	}
	s2.StdIn = &Redirect{
		Mode: REDIRECT_PIPE,
		Pipe: read2,
	}
	return nil
}
