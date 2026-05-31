package subprocess

import (
	"bufio"
	"bytes"
	"io"
	"os"
	"testing"
)

func TestInteractionLogMaxBytes(t *testing.T) {
	var buf bytes.Buffer
	log := &InteractionLog{
		writer:               bufio.NewWriter(&buf),
		hadEol:               true,
		currentLineDirection: -1,
		maxBytes:             5,
	}

	input := []byte("abcdef\n")
	n, err := log.write(1, input)
	if err != nil {
		t.Fatal(err)
	}
	if n != len(input) {
		t.Fatalf("write returned %d, want %d", n, len(input))
	}
	if err := log.writer.Flush(); err != nil {
		t.Fatal(err)
	}
	if buf.Len() != 5 {
		t.Fatalf("log length = %d, want 5", buf.Len())
	}

	n, err = log.write(0, []byte("xyz"))
	if err != nil {
		t.Fatal(err)
	}
	if n != 3 {
		t.Fatalf("write after cap returned %d, want 3", n)
	}
	if err := log.writer.Flush(); err != nil {
		t.Fatal(err)
	}
	if buf.Len() != 5 {
		t.Fatalf("log length after cap = %d, want 5", buf.Len())
	}
}

func TestCopyPipeWithTap(t *testing.T) {
	srcR, srcW, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	dstR, dstW, err := os.Pipe()
	if err != nil {
		srcR.Close()
		srcW.Close()
		t.Fatal(err)
	}
	defer dstR.Close()

	type copyResult struct {
		n   int64
		err error
	}
	done := make(chan copyResult, 1)
	go func() {
		n, err := copyPipeWithTap(dstW, srcR, dstW)
		dstW.Close()
		srcR.Close()
		done <- copyResult{n: n, err: err}
	}()

	payload := []byte("pipe relay payload\n")
	if _, err := srcW.Write(payload); err != nil {
		t.Fatal(err)
	}
	if err := srcW.Close(); err != nil {
		t.Fatal(err)
	}

	out, err := io.ReadAll(dstR)
	if err != nil {
		t.Fatal(err)
	}
	result := <-done
	if result.err != nil {
		t.Fatal(result.err)
	}
	if result.n != int64(len(payload)) {
		t.Fatalf("copied %d bytes, want %d", result.n, len(payload))
	}
	if !bytes.Equal(out, payload) {
		t.Fatalf("copied payload = %q, want %q", out, payload)
	}
}
