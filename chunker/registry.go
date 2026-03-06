package chunk

import (
	"io"
	"sync"
)

type CtorFunc func(r io.Reader, chunker string) (Splitter, error)

var (
	splittersMu sync.RWMutex
	splitters   = map[string]CtorFunc{}
)

// init registers the default splitters
func init() {
	splitters["size"] = parseSizeString
	splitters["rabin"] = parseRabinString
	splitters["buzhash"] = parseBuzhashString
}

// Register allows users to register custom chunkers that can be instantiated by
// [FromString]. The string passed to [FromString] is used to select the
// chunker. Everything before the first dash is considered the "name" of the
// chunker. For example, "rabin-{min}-{avg}-{max}" will select the "rabin"
// chunker.
func Register(name string, ctor CtorFunc) {
	splittersMu.Lock()
	defer splittersMu.Unlock()
	splitters[name] = ctor
}
