package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"io"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/urfave/cli/v2"
)

var importChanges = map[string]string{
	"github.com/ipfs/go-bitswap":                     "github.com/ipfs/go-libipfs/bitswap",
	"github.com/ipfs/go-ipfs-files":                  "github.com/ipfs/go-libipfs/files",
	"github.com/ipfs/tar-utils":                      "github.com/ipfs/go-libipfs/tar",
	"gihtub.com/ipfs/go-block-format":                "github.com/ipfs/go-libipfs/blocks",
	"github.com/ipfs/interface-go-ipfs-core":         "github.com/ipfs/go-libipfs/coreiface",
	"github.com/ipfs/go-unixfs":                      "github.com/ipfs/go-libipfs/unixfs",
	"github.com/ipfs/go-pinning-service-http-client": "github.com/ipfs/go-libipfs/pinning/remote/client",
	"github.com/ipfs/go-path":                        "github.com/ipfs/go-libipfs/path",
	"github.com/ipfs/go-namesys":                     "github.com/ipfs/go-libipfs/namesys",
	"github.com/ipfs/go-mfs":                         "github.com/ipfs/go-libipfs/mfs",
	"github.com/ipfs/go-ipfs-provider":               "github.com/ipfs/go-libipfs/provider",
	"github.com/ipfs/go-ipfs-pinner":                 "github.com/ipfs/go-libipfs/pinning/pinner",
	"github.com/ipfs/go-ipfs-keystore":               "github.com/ipfs/go-libipfs/keystore",
	"github.com/ipfs/go-filestore":                   "github.com/ipfs/go-libipfs/filestore",
	"github.com/ipfs/go-ipns":                        "github.com/ipfs/go-libipfs/ipns",
	"github.com/ipfs/go-blockservice":                "github.com/ipfs/go-libipfs/blockservice",
	"github.com/ipfs/go-ipfs-chunker":                "github.com/ipfs/go-libipfs/chunker",
	"github.com/ipfs/go-fetcher":                     "github.com/ipfs/go-libipfs/fetcher",
	"github.com/ipfs/go-ipfs-blockstore":             "github.com/ipfs/go-libipfs/blockstore",
	"github.com/ipfs/go-ipfs-posinfo":                "github.com/ipfs/go-libipfs/filestore/posinfo",
	"github.com/ipfs/go-ipfs-util":                   "github.com/ipfs/go-libipfs/util",
	"github.com/ipfs/go-ipfs-ds-help":                "github.com/ipfs/go-libipfs/datastore/dshelp",
	"github.com/ipfs/go-verifcid":                    "github.com/ipfs/go-libipfs/verifcid",
	"github.com/ipfs/go-ipfs-exchange-offline":       "github.com/ipfs/go-libipfs/exchange/offline",
	"github.com/ipfs/go-ipfs-routing":                "github.com/ipfs/go-libipfs/routing",
	"github.com/ipfs/go-ipfs-exchange-interface":     "github.com/ipfs/go-libipfs/exchange",
}

type pkgJSON struct {
	Dir            string
	GoFiles        []string
	IgnoredGoFiles []string
	TestGoFiles    []string
	CgoFiles       []string
}

func (p *pkgJSON) allSourceFiles() []string {
	var files []string
	lists := [][]string{p.GoFiles, p.IgnoredGoFiles, p.TestGoFiles, p.CgoFiles}
	for _, l := range lists {
		for _, f := range l {
			files = append(files, filepath.Join(p.Dir, f))
		}
	}
	return files
}

func updateImports(filePath string, dryRun bool) error {
	fset := token.NewFileSet()
	astFile, err := parser.ParseFile(fset, filePath, nil, parser.ParseComments)
	if err != nil {
		return fmt.Errorf("parsing %q: %w", filePath, err)
	}

	var fileChanged bool

	ast.Inspect(astFile, func(n ast.Node) bool {
		switch x := n.(type) {
		case *ast.ImportSpec:
			val := strings.Trim(x.Path.Value, `"`)
			// we take the first matching prefix, so you need to make sure you don't have ambiguous mappings
			for from, to := range importChanges {
				if strings.HasPrefix(val, from) {
					suffix := strings.TrimPrefix(val, from)
					newVal := to + suffix
					fmt.Printf("changing %s => %s in %s\n", x.Path.Value, newVal, filePath)
					if !dryRun {
						x.Path.Value = fmt.Sprintf(`"%s"`, newVal)
						fileChanged = true
					}
				}
			}
		}
		return true
	})

	if !fileChanged {
		return nil
	}

	f, err := os.Create(filePath)
	if err != nil {
		return err
	}
	err = format.Node(f, fset, astFile)
	if err != nil {
		f.Close()
		return fmt.Errorf("formatting %q: %w", filePath, err)
	}
	err = f.Close()
	if err != nil {
		return fmt.Errorf("closing %q: %w", filePath, err)
	}

	return nil
}

func readMappings(mappingsFile string) (map[string]string, error) {
	f, err := os.Open(mappingsFile)
	if err != nil {
		return nil, fmt.Errorf("opening mappings file: %w", err)
	}
	defer f.Close()
	mappings := map[string]string{}
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		vals := strings.Split(line, " ")
		from := strings.TrimSpace(vals[0])
		to := strings.TrimSpace(vals[1])
		mappings[from] = to
	}
	return mappings, nil
}

func main() {
	app := &cli.App{
		Name:  "migrate",
		Usage: "migrates a repo to libipfs by rewriting import paths, operating on the current directory",
		Flags: []cli.Flag{
			&cli.BoolFlag{
				Name: "dryrun",
			},
			&cli.StringFlag{
				Name:  "mappings",
				Usage: "a file with import path mappings, each line containing two space-separated values like 'github.com/ipfs/from github.com/ipfs/to'",
			},
		},
		Action: func(clictx *cli.Context) error {
			dryrun := clictx.Bool("dryrun")
			mappingsFile := clictx.String("mappings")

			if mappingsFile != "" {
				mappings, err := readMappings(mappingsFile)
				if err != nil {
					return err
				}
				importChanges = mappings
			}

			stdout := &bytes.Buffer{}
			stderr := &bytes.Buffer{}
			cmd := exec.Command("go", "list", "-json", "./...")
			cmd.Stdout = stdout
			cmd.Stderr = stderr
			err := cmd.Run()
			if err != nil {
				return fmt.Errorf("running 'go list': %w\nstderr:\n%s", err, stderr)
			}

			dec := json.NewDecoder(stdout)

			for {
				var pkg pkgJSON
				err = dec.Decode(&pkg)
				if err == io.EOF {
					return nil
				}
				if err != nil {
					return fmt.Errorf("decoding JSON: %w", err)
				}
				for _, filePath := range pkg.allSourceFiles() {
					if err := updateImports(filePath, dryrun); err != nil {
						return fmt.Errorf("updating file %q: %w", filePath, err)
					}
				}
			}
		},
	}
	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}
}
