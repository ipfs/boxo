package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"go/parser"
	"go/token"
	"io"
	"io/fs"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/dave/dst"
	"github.com/dave/dst/decorator"
	"github.com/urfave/cli/v2"
)

func main() {
	app := &cli.App{
		Name:  "deprecator",
		Usage: "Adds deprecation comments to all exported types in the module, with pointers to a new location. You should run this in your module root.",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:     "path",
				Usage:    "the new package path that the deprecated message should point users to",
				Required: true,
			},
		},
		Action: func(ctx *cli.Context) error {
			newPkgPath := ctx.String("path")
			wd, err := os.Getwd()
			if err != nil {
				return fmt.Errorf("getting working dir: %w", err)
			}
			fileToPackage, err := buildFileToPackage(wd)
			if err != nil {
				return fmt.Errorf("building mapping of files to packages: %w", err)
			}

			modPath, err := getModulePath(wd)
			if err != nil {
				return fmt.Errorf("finding current module path: %w", err)
			}

			fset := token.NewFileSet()
			return filepath.Walk(wd, func(path string, info fs.FileInfo, err error) error {
				if err != nil {
					return err
				}
				if info.IsDir() || filepath.Ext(path) != ".go" {
					return nil
				}

				addComment := func(name string, decs *dst.Decorations) {
					oldPkg := fileToPackage[path]
					newPkg := strings.Replace(oldPkg, modPath, newPkgPath, 1)
					newSym := newPkg + "." + name
					comment := fmt.Sprintf("// Deprecated: use %s", newSym)
					if len(decs.All()) > 0 {
						decs.Append("//")
					}
					decs.Append(comment)
				}

				file, err := decorator.ParseFile(fset, path, nil, parser.ParseComments)
				if err != nil {
					return fmt.Errorf("parsing %s: %w", path, err)
				}

				if _, ok := fileToPackage[path]; !ok {
					// this happens in the case of e.g. test files, which we want to skip
					return nil
				}

				// process the AST, adding comments where necessary
				dst.Inspect(file, func(n dst.Node) bool { return inspectASTNode(addComment, n) })

				outFile, err := os.Create(path)
				if err != nil {
					return fmt.Errorf("creating %s to write: %w", path, err)
				}
				defer outFile.Close()
				err = decorator.Fprint(outFile, file)
				if err != nil {
					return fmt.Errorf("writing %s: %w", path, err)
				}

				return nil
			})
		},
	}
	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}
}

type pkg struct {
	Dir        string
	ImportPath string
	GoFiles    []string
}

func inspectASTNode(addComment func(string, *dst.Decorations), n dst.Node) bool {
	switch x := n.(type) {
	case *dst.GenDecl:
		if x.Tok == token.CONST || x.Tok == token.VAR || x.Tok == token.TYPE {
			for _, spec := range x.Specs {
				switch s := spec.(type) {
				case *dst.ValueSpec:
					// if parenthesized, put a comment above each exported type in the group
					if x.Lparen {
						for _, name := range s.Names {
							if !name.IsExported() {
								continue
							}
							addComment(name.Name, &s.Decs.Start)
						}
					} else {
						name := s.Names[0]
						if !name.IsExported() {
							continue
						}
						addComment(name.Name, &x.Decs.Start)
					}
				case *dst.TypeSpec:
					name := s.Name
					if !name.IsExported() {
						continue
					}
					addComment(name.Name, &x.Decs.Start)
				}
			}
		}
	case *dst.FuncDecl:
		// don't add notices to methods
		if x.Name.IsExported() && x.Recv == nil {
			addComment(x.Name.Name, &x.Decs.Start)
		}
	}
	return true
}

func getModulePath(dir string) (string, error) {
	cmd := exec.Command("go", "list", "-m")
	cmd.Dir = dir
	stdout := &bytes.Buffer{}
	cmd.Stdout = stdout
	err := cmd.Run()
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(stdout.String()), nil
}

func buildFileToPackage(dir string) (map[string]string, error) {
	cmd := exec.Command("go", "list", "-json", "./...")
	cmd.Dir = dir
	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}
	cmd.Stdout = stdout
	cmd.Stderr = stderr
	err := cmd.Run()
	if err != nil {
		return nil, err
	}
	dec := json.NewDecoder(stdout)
	fileToPackage := map[string]string{}
	for {
		var p pkg
		err := dec.Decode(&p)
		if err == io.EOF {
			return fileToPackage, nil
		}
		if err != nil {
			return nil, err
		}
		for _, f := range p.GoFiles {
			fileToPackage[filepath.Join(p.Dir, f)] = p.ImportPath
		}
	}
}
