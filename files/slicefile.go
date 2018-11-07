package files

type fileEntry struct {
	name string
	file File
}

func (e fileEntry) Name() string {
	return e.name
}

func (e fileEntry) File() File {
	return e.file
}

func (e fileEntry) Regular() Regular {
	return castRegular(e.file)
}

func (e fileEntry) Dir() Directory {
	return castDir(e.file)
}

func FileEntry(name string, file File) DirEntry {
	return fileEntry{
		name: name,
		file: file,
	}
}

type sliceIterator struct {
	files []DirEntry
	n     int
}

func (it *sliceIterator) Name() string {
	return it.files[it.n].Name()
}

func (it *sliceIterator) File() File {
	return it.files[it.n].File()
}

func (it *sliceIterator) Regular() Regular {
	return it.files[it.n].Regular()
}

func (it *sliceIterator) Dir() Directory {
	return it.files[it.n].Dir()
}

func (it *sliceIterator) Next() bool {
	it.n++
	return it.n < len(it.files)
}

func (it *sliceIterator) Err() error {
	return nil
}

// SliceFile implements File, and provides simple directory handling.
// It contains children files, and is created from a `[]File`.
// SliceFiles are always directories, and can't be read from or closed.
type SliceFile struct {
	files []DirEntry
}

func NewSliceFile(files []DirEntry) Directory {
	return &SliceFile{files}
}

func (f *SliceFile) Entries() (DirIterator, error) {
	return &sliceIterator{files: f.files, n: -1}, nil
}

func (f *SliceFile) Close() error {
	return nil
}

func (f *SliceFile) Length() int {
	return len(f.files)
}

func (f *SliceFile) Size() (int64, error) {
	var size int64

	for _, file := range f.files {
		s, err := file.File().Size()
		if err != nil {
			return 0, err
		}
		size += s
	}

	return size, nil
}

var _ Directory = &SliceFile{}
var _ DirEntry = fileEntry{}
