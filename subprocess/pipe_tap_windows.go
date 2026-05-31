package subprocess

import (
	"io"
	"os"
	"runtime"
	"syscall"
	"unsafe"
)

const (
	pipeNowait  = 1
	errorNoData = syscall.Errno(232)

	// The test interaction is dominated by thousands of tiny flushed messages.
	// Yielding every read miss keeps the relay around the old 3.4s walltime;
	// this interval keeps the fast path below 3s while still letting other Go
	// goroutines run regularly.
	pipeSpinYieldEvery = 256
)

var setNamedPipeHandleState = syscall.NewLazyDLL("kernel32.dll").NewProc("SetNamedPipeHandleState")

func copyPipeWithTap(dst io.Writer, r io.Reader, wc io.Writer) (int64, error) {
	// dst is the original pipe destination used only to verify that this is the
	// usual pipe-to-pipe relay. Data is still written to wc, which may also tap
	// the stream into an interaction log or recorder.
	if _, ok := dst.(*os.File); !ok {
		return io.Copy(wc, r)
	}
	rf, ok := r.(*os.File)
	if !ok {
		return io.Copy(wc, r)
	}

	if err := setPipeNowait(syscall.Handle(rf.Fd())); err != nil {
		return io.Copy(wc, r)
	}
	return copyPipeNowait(rf, wc)
}

func setPipeNowait(h syscall.Handle) error {
	mode := uint32(pipeNowait)
	r, _, e := setNamedPipeHandleState.Call(
		uintptr(h),
		uintptr(unsafe.Pointer(&mode)),
		0,
		0,
	)
	if r == 0 {
		if e != syscall.Errno(0) {
			return e
		}
		return syscall.EINVAL
	}
	return nil
}

func copyPipeNowait(r *os.File, wc io.Writer) (written int64, err error) {
	buf := make([]byte, 32*1024)
	rh := syscall.Handle(r.Fd())
	idle := 0
	for {
		var nr uint32
		e := syscall.ReadFile(rh, buf, &nr, nil)
		if nr > 0 {
			idle = 0
			nw, ew := wc.Write(buf[:nr])
			if nw > 0 {
				written += int64(nw)
			}
			if ew != nil {
				return written, ew
			}
			if nw != int(nr) {
				return written, io.ErrShortWrite
			}
		}
		if e != nil {
			if e == errorNoData {
				idle++
				if idle%pipeSpinYieldEvery == 0 {
					runtime.Gosched()
				}
				continue
			}
			if e == syscall.ERROR_BROKEN_PIPE || e == syscall.ERROR_HANDLE_EOF {
				return written, nil
			}
			return written, os.NewSyscallError("ReadFile", e)
		}
		if nr == 0 {
			return written, nil
		}
	}
}
