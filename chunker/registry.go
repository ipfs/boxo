package chunk

import (
	"io"
	"strings"
	"sync"
)

type SplitterFunc func(r io.Reader, chunker string) (Splitter, error)

var (
	splittersMu sync.RWMutex
	splitters   = map[string]SplitterFunc{}
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
//
// Register panics if name is empty, contains a dash, ctor is nil, or a
// chunker with the same name is already registered. This follows the
// convention established by [database/sql.Register].
//
// Register is safe for concurrent use.
func Register(name string, ctor SplitterFunc) {
	splittersMu.Lock()
	defer splittersMu.Unlock()
	if name == "" {
		panic("chunk: Register name is empty")
	}
	if strings.Contains(name, "-") {
		panic("chunk: Register name must not contain a dash: " + name)
	}
	if ctor == nil {
		panic("chunk: Register ctor is nil")
	}
	if _, dup := splitters[name]; dup {
		panic("chunk: Register called twice for chunker " + name)
	}
	splitters[name] = ctor
}
