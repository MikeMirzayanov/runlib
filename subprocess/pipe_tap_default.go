//go:build !windows

package subprocess

import "io"

func copyPipeWithTap(_ io.Writer, r io.Reader, wc io.Writer) (int64, error) {
	return io.Copy(wc, r)
}
