package main

import (
	"bytes"
	"fmt"
	"os"
	"sort"
	"strings"

	carv2 "github.com/ipld/go-car/v2"
	"github.com/multiformats/go-multicodec"
	"github.com/urfave/cli/v2"
)

// InspectCar verifies a CAR and prints a basic report about its contents
func InspectCar(c *cli.Context) (err error) {
	inStream := os.Stdin
	if c.Args().Len() >= 1 {
		inStream, err = os.Open(c.Args().First())
		if err != nil {
			return err
		}
	}

	rd, err := carv2.NewReader(inStream)
	if err != nil {
		return err
	}
	stats, err := rd.Inspect(c.IsSet("full"))
	if err != nil {
		return err
	}

	var v2s string
	if stats.Version == 2 {
		idx := "(none)"
		if stats.IndexCodec != 0 {
			idx = stats.IndexCodec.String()
		}
		var buf bytes.Buffer
		stats.Header.Characteristics.WriteTo(&buf)
		v2s = fmt.Sprintf(`Characteristics: %x
Data offset: %d
Data (payload) length: %d
Index offset: %d
Index type: %s
`, buf.Bytes(), stats.Header.DataOffset, stats.Header.DataSize, stats.Header.IndexOffset, idx)
	}

	var roots strings.Builder
	switch len(stats.Roots) {
	case 0:
		roots.WriteString(" (none)")
	case 1:
		roots.WriteString(" ")
		roots.WriteString(stats.Roots[0].String())
	default:
		for _, r := range stats.Roots {
			roots.WriteString("\n\t")
			roots.WriteString(r.String())
		}
	}

	var codecs strings.Builder
	{
		keys := make([]int, len(stats.CodecCounts))
		i := 0
		for codec := range stats.CodecCounts {
			keys[i] = int(codec)
			i++
		}
		sort.Ints(keys)
		for _, code := range keys {
			codec := multicodec.Code(code)
			codecs.WriteString(fmt.Sprintf("\n\t%s: %d", codec, stats.CodecCounts[codec]))
		}
	}

	var hashers strings.Builder
	{
		keys := make([]int, len(stats.MhTypeCounts))
		i := 0
		for codec := range stats.MhTypeCounts {
			keys[i] = int(codec)
			i++
		}
		sort.Ints(keys)
		for _, code := range keys {
			codec := multicodec.Code(code)
			hashers.WriteString(fmt.Sprintf("\n\t%s: %d", codec, stats.MhTypeCounts[codec]))
		}
	}

	rp := "No"
	if stats.RootsPresent {
		rp = "Yes"
	}

	pfmt := `Version: %d
%sRoots:%s
Root blocks present in data: %s
Block count: %d
Min / average / max block length (bytes): %d / %d / %d
Min / average / max CID length (bytes): %d / %d / %d
Block count per codec:%s
CID count per multihash:%s
`

	fmt.Printf(
		pfmt,
		stats.Version,
		v2s,
		roots.String(),
		rp,
		stats.BlockCount,
		stats.MinBlockLength,
		stats.AvgBlockLength,
		stats.MaxBlockLength,
		stats.MinCidLength,
		stats.AvgCidLength,
		stats.MaxCidLength,
		codecs.String(),
		hashers.String(),
	)

	return nil
}
