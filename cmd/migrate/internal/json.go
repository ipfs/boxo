package migrate

import "path/filepath"

type pkgJSON struct {
	Dir            string
	GoFiles        []string
	IgnoredGoFiles []string
	TestGoFiles    []string
	XTestGoFiles   []string
	CgoFiles       []string
}

func (p *pkgJSON) allSourceFiles() []string {
	var files []string
	lists := [...][]string{p.GoFiles, p.IgnoredGoFiles, p.TestGoFiles, p.CgoFiles, p.XTestGoFiles}
	for _, l := range lists {
		for _, f := range l {
			files = append(files, filepath.Join(p.Dir, f))
		}
	}
	return files
}
