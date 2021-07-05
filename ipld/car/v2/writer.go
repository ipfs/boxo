package car

import (
	"bytes"
	"context"
	"io"
	"os"

	"github.com/ipfs/go-cid"
	format "github.com/ipfs/go-ipld-format"
	"github.com/ipld/go-car/v2/index"
	"github.com/ipld/go-car/v2/internal/carv1"
)

const bulkPaddingBytesSize = 1024

var bulkPadding = make([]byte, bulkPaddingBytesSize)

type (
	// padding represents the number of padding bytes.
	padding uint64
	// Writer writes CAR v2 into a give io.Writer.
	Writer struct {
		NodeGetter   format.NodeGetter
		CarV1Padding uint64
		IndexPadding uint64

		ctx          context.Context
		roots        []cid.Cid
		encodedCarV1 *bytes.Buffer
	}
	WriteOption func(*Writer)
)

// WriteTo writes this padding to the given writer as default value bytes.
func (p padding) WriteTo(w io.Writer) (n int64, err error) {
	var reminder int64
	if p > bulkPaddingBytesSize {
		reminder = int64(p % bulkPaddingBytesSize)
		iter := int(p / bulkPaddingBytesSize)
		for i := 0; i < iter; i++ {
			if _, err = w.Write(bulkPadding); err != nil {
				return
			}
			n += bulkPaddingBytesSize
		}
	} else {
		reminder = int64(p)
	}

	paddingBytes := make([]byte, reminder)
	_, err = w.Write(paddingBytes)
	n += reminder
	return
}

// NewWriter instantiates a new CAR v2 writer.
func NewWriter(ctx context.Context, ng format.NodeGetter, roots []cid.Cid) *Writer {
	return &Writer{
		NodeGetter:   ng,
		ctx:          ctx,
		roots:        roots,
		encodedCarV1: new(bytes.Buffer),
	}
}

// WriteTo writes the given root CIDs according to CAR v2 specification.
func (w *Writer) WriteTo(writer io.Writer) (n int64, err error) {
	_, err = writer.Write(Pragma)
	if err != nil {
		return
	}
	n += int64(PragmaSize)
	// We read the entire car into memory because index.Generate takes a reader.
	// TODO Future PRs will make this more efficient by exposing necessary interfaces in index pacakge so that
	// this can be done in an streaming manner.
	if err = carv1.WriteCar(w.ctx, w.NodeGetter, w.roots, w.encodedCarV1); err != nil {
		return
	}
	carV1Len := w.encodedCarV1.Len()

	wn, err := w.writeHeader(writer, carV1Len)
	if err != nil {
		return
	}
	n += wn

	wn, err = padding(w.CarV1Padding).WriteTo(writer)
	if err != nil {
		return
	}
	n += wn

	carV1Bytes := w.encodedCarV1.Bytes()
	wwn, err := writer.Write(carV1Bytes)
	if err != nil {
		return
	}
	n += int64(wwn)

	wn, err = padding(w.IndexPadding).WriteTo(writer)
	if err != nil {
		return
	}
	n += wn

	wn, err = w.writeIndex(writer, carV1Bytes)
	if err == nil {
		n += wn
	}
	return
}

func (w *Writer) writeHeader(writer io.Writer, carV1Len int) (int64, error) {
	header := NewHeader(uint64(carV1Len)).
		WithCarV1Padding(w.CarV1Padding).
		WithIndexPadding(w.IndexPadding)
	return header.WriteTo(writer)
}

func (w *Writer) writeIndex(writer io.Writer, carV1 []byte) (int64, error) {
	// TODO avoid recopying the bytes by refactoring index once it is integrated here.
	// Right now we copy the bytes since index takes a writer.
	// Consider refactoring index to make this process more efficient.
	// We should avoid reading the entire car into memory since it can be large.
	reader := bytes.NewReader(carV1)
	idx, err := index.Generate(reader)
	if err != nil {
		return 0, err
	}
	// FIXME refactor index to expose the number of bytes written.
	return 0, index.WriteTo(idx, writer)
}

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
	// index.Generate should probably be in charge of that.

	idx, err := index.Generate(src)
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

	// Similar to the Writer API, write all components of a CARv2 to the
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
