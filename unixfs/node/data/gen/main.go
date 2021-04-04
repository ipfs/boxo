package main

import (
	"fmt"
	"os"

	"github.com/ipld/go-ipld-prime/schema"
	gengo "github.com/ipld/go-ipld-prime/schema/gen/go"
)

func main() {
	ts := schema.TypeSystem{}
	ts.Init()
	adjCfg := &gengo.AdjunctCfg{}

	pkgName := "data"

	ts.Accumulate(schema.SpawnString("String"))
	ts.Accumulate(schema.SpawnInt("Int"))
	ts.Accumulate(schema.SpawnBytes("Bytes"))

	ts.Accumulate(schema.SpawnList("BlockSizes", "Int", false))

	/*
		type UnixTime struct {
			seconds Int
			fractionalNanoseconds Int
		}
	*/
	ts.Accumulate(schema.SpawnStruct("UnixTime",
		[]schema.StructField{
			schema.SpawnStructField("Seconds", "Int", false, false),
			schema.SpawnStructField("FractionalNanoseconds", "Int", true, false),
		},
		schema.SpawnStructRepresentationMap(nil),
	))

	/*
		type UnixFSData struct {
			dataType Int
			data optional Bytes
			filesize optional Int;
			blocksizes [Int]

			hashType optional Int
			fanout optional Int
			mode optional Int
			mtime optional UnixTime
		} representation map
	*/

	ts.Accumulate(schema.SpawnStruct("UnixFSData",
		[]schema.StructField{
			schema.SpawnStructField("DataType", "Int", false, false),
			schema.SpawnStructField("Data", "Bytes", true, false),
			schema.SpawnStructField("FileSize", "Int", true, false),
			schema.SpawnStructField("BlockSizes", "BlockSizes", false, false),
			schema.SpawnStructField("HashType", "Int", true, false),
			schema.SpawnStructField("Fanout", "Int", true, false),
			schema.SpawnStructField("Mode", "Int", true, false),
			schema.SpawnStructField("Mtime", "UnixTime", true, false),
		},
		schema.SpawnStructRepresentationMap(nil),
	))

	/*
		type UnixFSMetadata struct {
			mimeType optional String
		} representation map
	*/

	ts.Accumulate(schema.SpawnStruct("UnixFSMetadata",
		[]schema.StructField{
			schema.SpawnStructField("MimeType", "String", true, false),
		},
		schema.SpawnStructRepresentationMap(nil),
	))

	if errs := ts.ValidateGraph(); errs != nil {
		for _, err := range errs {
			fmt.Printf("- %s\n", err)
		}
		os.Exit(1)
	}

	gengo.Generate(".", pkgName, ts, adjCfg)
}
