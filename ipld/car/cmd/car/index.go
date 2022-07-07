package main

import (
	"bufio"
	"fmt"
	"io"
	"os"

	"github.com/ipfs/go-cid"
	carv1 "github.com/ipld/go-car"
	carv2 "github.com/ipld/go-car/v2"
	"github.com/ipld/go-car/v2/index"
	"github.com/multiformats/go-multicodec"
	"github.com/multiformats/go-varint"
	"github.com/urfave/cli/v2"
)

// IndexCar is a command to add an index to a car
func IndexCar(c *cli.Context) error {
	r, err := carv2.OpenReader(c.Args().Get(0))
	if err != nil {
		return err
	}
	defer r.Close()

	if c.Int("version") == 1 {
		if c.IsSet("codec") && c.String("codec") != "none" {
			return fmt.Errorf("'none' is the only supported codec for a v1 car")
		}
		outStream := os.Stdout
		if c.Args().Len() >= 2 {
			outStream, err = os.Create(c.Args().Get(1))
			if err != nil {
				return err
			}
		}
		defer outStream.Close()

		dr, err := r.DataReader()
		if err != nil {
			return err
		}
		_, err = io.Copy(outStream, dr)
		return err
	}

	if c.Int("version") != 2 {
		return fmt.Errorf("invalid CAR version %d", c.Int("version"))
	}

	var idx index.Index
	if c.String("codec") != "none" {
		var mc multicodec.Code
		if err := mc.Set(c.String("codec")); err != nil {
			return err
		}
		idx, err = index.New(mc)
		if err != nil {
			return err
		}
	}

	outStream := os.Stdout
	if c.Args().Len() >= 2 {
		outStream, err = os.Create(c.Args().Get(1))
		if err != nil {
			return err
		}
	}
	defer outStream.Close()

	v1r, err := r.DataReader()
	if err != nil {
		return err
	}

	if r.Version == 1 {
		fi, err := os.Stat(c.Args().Get(0))
		if err != nil {
			return err
		}
		r.Header.DataSize = uint64(fi.Size())
	}
	v2Header := carv2.NewHeader(r.Header.DataSize)
	if c.String("codec") == "none" {
		v2Header.IndexOffset = 0
		if _, err := outStream.Write(carv2.Pragma); err != nil {
			return err
		}
		if _, err := v2Header.WriteTo(outStream); err != nil {
			return err
		}
		if _, err := io.Copy(outStream, v1r); err != nil {
			return err
		}
		return nil
	}

	if _, err := outStream.Write(carv2.Pragma); err != nil {
		return err
	}
	if _, err := v2Header.WriteTo(outStream); err != nil {
		return err
	}

	// collect records as we go through the v1r
	br := bufio.NewReader(v1r)
	hdr, err := carv1.ReadHeader(br)
	if err != nil {
		return fmt.Errorf("error reading car header: %w", err)
	}
	if err := carv1.WriteHeader(hdr, outStream); err != nil {
		return err
	}

	records := make([]index.Record, 0)
	var sectionOffset int64
	if sectionOffset, err = v1r.Seek(0, io.SeekCurrent); err != nil {
		return err
	}
	sectionOffset -= int64(br.Buffered())

	for {
		// Read the section's length.
		sectionLen, err := varint.ReadUvarint(br)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		if _, err := outStream.Write(varint.ToUvarint(sectionLen)); err != nil {
			return err
		}

		// Null padding; by default it's an error.
		// TODO: integrate corresponding ReadOption
		if sectionLen == 0 {
			// TODO: pad writer to expected length.
			break
		}

		// Read the CID.
		cidLen, c, err := cid.CidFromReader(br)
		if err != nil {
			return err
		}
		records = append(records, index.Record{Cid: c, Offset: uint64(sectionOffset)})
		if _, err := c.WriteBytes(outStream); err != nil {
			return err
		}

		// Seek to the next section by skipping the block.
		// The section length includes the CID, so subtract it.
		remainingSectionLen := int64(sectionLen) - int64(cidLen)
		if _, err := io.CopyN(outStream, br, remainingSectionLen); err != nil {
			return err
		}
		sectionOffset += int64(sectionLen) + int64(varint.UvarintSize(sectionLen))
	}

	if err := idx.Load(records); err != nil {
		return err
	}

	_, err = index.WriteTo(idx, outStream)
	return err
}
