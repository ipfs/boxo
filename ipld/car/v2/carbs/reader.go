package carbs

import "io"

type unatreader struct {
	io.ReaderAt
	at int64
}

func (u *unatreader) Read(p []byte) (n int, err error) {
	n, err = u.ReadAt(p, u.at)
	u.at = u.at + int64(n)
	return
}

func (u *unatreader) ReadByte() (byte, error) {
	b := []byte{0}
	_, err := u.Read(b)
	return b[0], err
}
