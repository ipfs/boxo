package main

import (
	"fmt"
	"os"

	carv2 "github.com/ipld/go-car/v2"

	"github.com/ipld/go-car/v2/index"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Printf("Usage: hydrate <file.car>\n")
		return
	}
	db := os.Args[1]

	idx, err := carv2.GenerateIndexFromFile(db)
	if err != nil {
		fmt.Printf("Error generating index: %v\n", err)
		return
	}

	fmt.Printf("Saving...\n")

	if err := index.Save(idx, db); err != nil {
		fmt.Printf("Error saving : %v\n", err)
	}
}
