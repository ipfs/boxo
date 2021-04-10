package carbon

import "io"

type poswriter struct {
	io.Writer
	at uint64
}

func (p *poswriter) Write(b []byte) (n int, err error) {
	n, err = p.Writer.Write(b)
	p.at += uint64(n)
	return
}
