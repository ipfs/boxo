package io

import "io"

func FullReadAt(r io.ReaderAt, b []byte, off int64) (sum int64, err error) {
	for int64(len(b)) > sum {
		n, err := r.ReadAt(b[sum:], off+sum)
		sum += int64(n)
		if err != nil {
			if err == io.EOF {
				if sum < int64(len(b)) {
					return sum, io.ErrUnexpectedEOF
				}
				return sum, nil
			}
			return sum, err
		}
	}
	return sum, nil
}
