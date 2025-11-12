package proc

import (
	"archive/tar"
	"archive/zip"
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

func isGzip(b []byte) bool {
	return len(b) >= 2 && b[0] == 0x1f && b[1] == 0x8b
}
func isZip(b []byte) bool {
	return len(b) >= 4 && bytes.Equal(b[:4], []byte{0x50, 0x4b, 0x03, 0x04})
}

func writeTarGzInto(r io.Reader, dest string) error {
	gzr, zerr := gzip.NewReader(r)
	if zerr != nil {
		return zerr
	}
	defer func() { _ = gzr.Close() }()
	tr := tar.NewReader(gzr)
	for {
		err := func() error {
			h, err := tr.Next()
			if err == io.EOF {
				return nil
			}
			if err != nil {
				return err
			}
			target := filepath.Join(dest, h.Name)

			if !strings.HasPrefix(target, filepath.Clean(dest)+string(os.PathSeparator)) {
				return fmt.Errorf("illegal file path: %s", h.Name)
			}

			switch h.Typeflag {
			case tar.TypeDir:
				if err = os.MkdirAll(target, os.FileMode(h.Mode)); err != nil {
					return err
				}
			case tar.TypeReg:
				if err = os.MkdirAll(filepath.Dir(target), 0700); err != nil {
					return err
				}
				f, e := os.OpenFile(target, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, os.FileMode(h.Mode))
				if e != nil {
					return e
				}
				defer func() { _ = f.Close() }()

				if _, err = io.Copy(f, tr); err != nil {
					return err
				}
			default:
				// ?
			}
			return nil
		}()
		if err != nil {
			return err
		}
	}
}

func writeZipInto(ra io.ReaderAt, size int64, dest string) error {
	r, zerr := zip.NewReader(ra, size)
	if zerr != nil {
		return zerr
	}
	for _, f := range r.File {
		err := func() error {
			path := filepath.Join(dest, f.Name)
			if !strings.HasPrefix(path, filepath.Clean(dest)+string(os.PathSeparator)) {
				return fmt.Errorf("illegal file path: %s", f.Name)
			}
			if f.FileInfo().IsDir() {
				if e := os.MkdirAll(path, f.Mode()); e != nil {
					return e
				}
				return nil
			}
			if e := os.MkdirAll(filepath.Dir(path), 0700); e != nil {
				return e
			}

			rc, e := f.Open()
			if e != nil {
				return e
			}
			defer func() { _ = rc.Close() }()

			out, e := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, f.Mode())
			if e != nil {
				return e
			}
			defer func() { _ = out.Close() }()

			_, e = io.Copy(out, rc)
			return e
		}()
		if err != nil {
			return err
		}
	}
	return nil
}
