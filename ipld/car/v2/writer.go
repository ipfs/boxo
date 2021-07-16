package car

import (
	"io"
	"os"

	internalio "github.com/ipld/go-car/v2/internal/io"

	"github.com/ipld/go-car/v2/index"
)

// WrapV1File is a wrapper around WrapV1 that takes filesystem paths.
// The source path is assumed to exist, and the destination path is overwritten.
// Note that the destination path might still be created even if an error
// occurred.
func WrapV1File(srcPath, dstPath string) error {
	src, err := os.Open(srcPath)
	if err != nil {
		return err
	}
	defer src.Close()

	dst, err := os.Create(dstPath)
	if err != nil {
		return err
	}
	defer dst.Close()

	if err := WrapV1(src, dst); err != nil {
		return err
	}

	// Check the close error, since we're writing to dst.
	// Note that we also do a "defer dst.Close()" above,
	// to make sure that the earlier error returns don't leak the file.
	if err := dst.Close(); err != nil {
		return err
	}
	return nil
}

// WrapV1 takes a CARv1 file and wraps it as a CARv2 file with an index.
// The resulting CARv2 file's inner CARv1 payload is left unmodified,
// and does not use any padding before the innner CARv1 or index.
func WrapV1(src io.ReadSeeker, dst io.Writer) error {
	// TODO: verify src is indeed a CARv1 to prevent misuse.
	// GenerateIndex should probably be in charge of that.

	idx, err := GenerateIndex(src)
	if err != nil {
		return err
	}

	// Use Seek to learn the size of the CARv1 before reading it.
	v1Size, err := src.Seek(0, io.SeekEnd)
	if err != nil {
		return err
	}
	if _, err := src.Seek(0, io.SeekStart); err != nil {
		return err
	}

	// Similar to the writer API, write all components of a CARv2 to the
	// destination file: Pragma, Header, CARv1, Index.
	v2Header := NewHeader(uint64(v1Size))
	if _, err := dst.Write(Pragma); err != nil {
		return err
	}
	if _, err := v2Header.WriteTo(dst); err != nil {
		return err
	}
	if _, err := io.Copy(dst, src); err != nil {
		return err
	}
	if err := index.WriteTo(idx, dst); err != nil {
		return err
	}

	return nil
}

// AttachIndex attaches a given index to an existing car v2 file at given path and offset.
func AttachIndex(path string, idx index.Index, offset uint64) error {
	// TODO: instead of offset, maybe take padding?
	// TODO: check that the given path is indeed a CAR v2.
	// TODO: update CAR v2 header according to the offset at which index is written out.
	out, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o640)
	if err != nil {
		return err
	}
	defer out.Close()
	indexWriter := internalio.NewOffsetWriter(out, int64(offset))
	return index.WriteTo(idx, indexWriter)
}
