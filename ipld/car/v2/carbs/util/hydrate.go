package main

import (
	"fmt"
	"os"

	"github.com/willscott/carbs"
	"golang.org/x/exp/mmap"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Printf("Usage: hydrate <file.car> [codec]\n")
		return
	}
	db := os.Args[1]
	codec := carbs.IndexSorted
	if len(os.Args) == 3 {
		if os.Args[2] == "Hash" {
			codec = carbs.IndexHashed
		} else if os.Args[2] == "GobHash" {
			codec = carbs.IndexGobHashed
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

	idx, err := carbs.GenerateIndex(dbBacking, dbstat.Size(), codec, true)
	if err != nil {
		fmt.Printf("Error generating index: %v\n", err)
		return
	}

	fmt.Printf("Saving...\n")

	if err := carbs.Save(idx, db); err != nil {
		fmt.Printf("Error saving : %v\n", err)
		return
	}
	return
}
