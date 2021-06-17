package carbon

import "io"

//lint:ignore U1000 The entire carbon package will be reviewed; this is temporary
type unatreader struct {
	io.ReaderAt
	at int64
}

//lint:ignore U1000 The entire carbon package will be reviewed; this is temporary
func (u *unatreader) Read(p []byte) (n int, err error) {
	n, err = u.ReadAt(p, u.at)
	u.at = u.at + int64(n)
	return
}

//lint:ignore U1000 The entire carbon package will be reviewed; this is temporary
func (u *unatreader) ReadByte() (byte, error) {
	b := []byte{0}
	_, err := u.Read(b)
	return b[0], err
}
