package car

import (
	"errors"
	"fmt"
	"io"
	"os"

	internalio "github.com/ipld/go-car/v2/internal/io"

	"github.com/ipld/go-car/v2/index"
)

// ErrAlreadyV1 signals that the given payload is already in CARv1 format.
var ErrAlreadyV1 = errors.New("already a CARv1")

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

// ExtractV1File takes a CARv2 file and extracts its CARv1 data payload, unmodified.
// The resulting CARv1 file will not include any data payload padding that may be present in the
// CARv2 srcPath.
// If srcPath represents a CARv1 ErrAlreadyV1 error is returned.
// The srcPath is assumed to exist, and the destination path is created if not exist.
// Note that the destination path might still be created even if an error
// occurred.
// If srcPath and dstPath are the same, then the dstPath is converted, in-place, to CARv1.
func ExtractV1File(srcPath, dstPath string) (err error) {
	src, err := os.Open(srcPath)
	if err != nil {
		return err
	}

	// Ignore close error since only reading from src.
	defer src.Close()

	// Detect CAR version.
	version, err := ReadVersion(src)
	if err != nil {
		return err
	}
	if version == 1 {
		return ErrAlreadyV1
	}
	if version != 2 {
		return fmt.Errorf("invalid source version: %v", version)
	}

	// Read CARv2 header to locate data payload.
	var v2h Header
	if _, err := v2h.ReadFrom(src); err != nil {
		return err
	}

	// TODO consider extracting this into Header.Validate since it is also implemented in BlockReader.
	// Validate header
	dataOffset := int64(v2h.DataOffset)
	if dataOffset < PragmaSize+HeaderSize {
		return fmt.Errorf("invalid data payload offset: %v", dataOffset)
	}
	dataSize := int64(v2h.DataSize)
	if dataSize <= 0 {
		return fmt.Errorf("invalid data payload size: %v", dataSize)
	}

	// Seek to the point where the data payload starts
	if _, err := src.Seek(dataOffset, io.SeekStart); err != nil {
		return err
	}

	// Open destination as late as possible to minimise unintended file creation in case an error
	// occurs earlier.
	// Note, we explicitly do not use os.O_TRUNC here so that we can support in-place extraction.
	// Otherwise, truncation of an existing file will wipe the data we would be reading from if
	// source and destination paths are the same.
	// Later, we do truncate the file to the right size to assert there are no tailing extra bytes.
	dst, err := os.OpenFile(dstPath, os.O_CREATE|os.O_WRONLY, 0o666)
	if err != nil {
		return err
	}

	defer func() {
		// Close destination and override return error type if it is nil.
		cerr := dst.Close()
		if err == nil {
			err = cerr
		}
	}()

	// Copy data payload over, expecting to write exactly the right number of bytes.
	// Note that we explicitly use io.CopyN using file descriptors to leverage the SDK's efficient
	// byte copy which should stay out of userland.
	// There are two benchmarks to measure this: BenchmarkExtractV1File vs. BenchmarkExtractV1UsingReader
	written, err := io.CopyN(dst, src, dataSize)
	if err != nil {
		return err
	}
	if written != dataSize {
		return fmt.Errorf("expected to write exactly %v but wrote %v", dataSize, written)
	}

	// Check that the size destination file matches expected size.
	// If bigger truncate.
	// Note, we need to truncate:
	// - if file is changed in-place, i.e. src and dst paths are the same then index or padding
	//   could be present after the data payload.
	// - if an existing file is passed as destination which is different from source and is larger
	//   than the data payload size.
	// In general, we want to guarantee that this function produces correct CARv2 payload in
	// destination.
	stat, err := dst.Stat()
	if err != nil {
		return err
	}
	if stat.Size() > dataSize {
		// Truncate to the expected size to assure the resulting file is a correctly sized CARv1.
		err = dst.Truncate(written)
	}

	return err
}

// AttachIndex attaches a given index to an existing CARv2 file at given path and offset.
func AttachIndex(path string, idx index.Index, offset uint64) error {
	// TODO: instead of offset, maybe take padding?
	// TODO: check that the given path is indeed a CARv2.
	// TODO: update CARv2 header according to the offset at which index is written out.
	out, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o640)
	if err != nil {
		return err
	}
	defer out.Close()
	indexWriter := internalio.NewOffsetWriter(out, int64(offset))
	return index.WriteTo(idx, indexWriter)
}
