package main

import (
	"fmt"
	"github.com/ipld/go-car/v2/internal/index"
	"golang.org/x/exp/mmap"
	"os"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Printf("Usage: hydrate <file.car> [codec]\n")
		return
	}
	db := os.Args[1]
	codec := index.IndexSorted
	if len(os.Args) == 3 {
		if os.Args[2] == "Hash" {
			codec = index.IndexHashed
		} else if os.Args[2] == "GobHash" {
			codec = index.IndexGobHashed
		}
	}

	dbBacking, err := mmap.Open(db)
	if err != nil {
		fmt.Printf("Error Opening car for hydration: %v\n", err)
		return
	}

	dbstat, err := os.Stat(db)
	if err != nil {
		fmt.Printf("Error statting car for hydration: %v\n", err)
		return
	}

	idx, err := index.GenerateIndex(dbBacking, dbstat.Size(), codec)
	if err != nil {
		fmt.Printf("Error generating index: %v\n", err)
		return
	}

	fmt.Printf("Saving...\n")

	if err := index.Save(idx, db); err != nil {
		fmt.Printf("Error saving : %v\n", err)
	}
}
