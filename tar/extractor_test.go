package tar

import (
	"archive/tar"
	"bytes"
	"fmt"
	"io"
	"os"
	fp "path/filepath"
	"runtime"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

var symlinksEnabled bool
var symlinksEnabledErr error

func init() {
	// check if the platform supports symlinks
	// inspired by https://github.com/golang/go/blob/770f1de8c54256d5b17447028e47b201ba8e62c8/src/internal/testenv/testenv_windows.go#L17

	tmpdir, err := os.MkdirTemp("", "platformsymtest")
	if err != nil {
		panic("failed to create temp directory: " + err.Error())
	}
	defer os.RemoveAll(tmpdir)

	symlinksEnabledErr = os.Symlink("target", fp.Join(tmpdir, "symlink"))
	symlinksEnabled = symlinksEnabledErr == nil

	if !symlinksEnabled {
		// for now assume symlinks only fail on Windows, Android and Plan9
		// taken from https://github.com/golang/go/blob/770f1de8c54256d5b17447028e47b201ba8e62c8/src/internal/testenv/testenv_notwin.go#L14
		// and https://github.com/golang/go/blob/770f1de8c54256d5b17447028e47b201ba8e62c8/src/internal/testenv/testenv_windows.go#L34
		switch runtime.GOOS {
		case "windows", "android", "plan9":
		default:
			panic(fmt.Errorf("attempted symlink creation failed: %w", symlinksEnabledErr))
		}
	}
}

func TestSingleFile(t *testing.T) {
	fileName := "file..ext"
	fileData := "file data"

	testTarExtraction(t, nil, []tarEntry{
		&fileTarEntry{fileName, []byte(fileData)},
	},
		func(t *testing.T, extractDir string) {
			f, err := os.Open(fp.Join(extractDir, fileName))
			assert.NoError(t, err)
			data, err := io.ReadAll(f)
			assert.NoError(t, err)
			assert.Equal(t, fileData, string(data))
			assert.NoError(t, f.Close())
		},
		nil,
	)
}

func TestSingleDirectory(t *testing.T) {
	dirName := "dir..sfx"

	testTarExtraction(t, nil, []tarEntry{
		&dirTarEntry{dirName},
	},
		func(t *testing.T, extractDir string) {
			f, err := os.Open(extractDir)
			if err != nil {
				t.Fatal(err)
			}
			objs, err := f.Readdir(1)
			if err == io.EOF && len(objs) == 0 {
				return
			}
			t.Fatalf("expected an empty directory")
		},
		nil,
	)
}

func TestDirectoryFollowSymlinkToNothing(t *testing.T) {
	dirName := "dir"
	childName := "child"

	entries := []tarEntry{
		&dirTarEntry{dirName},
		&dirTarEntry{dirName + "/" + childName},
	}

	testTarExtraction(t, func(t *testing.T, rootDir string) {
		target := fp.Join(rootDir, tarOutRoot)
		if err := os.Symlink(fp.Join(target, "foo"), fp.Join(target, childName)); err != nil {
			t.Fatal(err)
		}
	}, entries, nil,
		os.ErrExist,
	)
}

func TestDirectoryFollowSymlinkToFile(t *testing.T) {
	dirName := "dir"
	childName := "child"

	entries := []tarEntry{
		&dirTarEntry{dirName},
		&dirTarEntry{dirName + "/" + childName},
	}

	testTarExtraction(t, func(t *testing.T, rootDir string) {
		target := fp.Join(rootDir, tarOutRoot)
		symlinkTarget := fp.Join(target, "foo")
		if err := os.WriteFile(symlinkTarget, []byte("original data"), os.ModePerm); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(symlinkTarget, fp.Join(target, childName)); err != nil {
			t.Fatal(err)
		}
	}, entries, nil,
		syscall.ENOTDIR,
	)
}

func TestDirectoryFollowSymlinkToDirectory(t *testing.T) {
	dirName := "dir"
	childName := "child"

	entries := []tarEntry{
		&dirTarEntry{dirName},
		&dirTarEntry{dirName + "/" + childName},
	}

	testTarExtraction(t, func(t *testing.T, rootDir string) {
		target := fp.Join(rootDir, tarOutRoot)
		symlinkTarget := fp.Join(target, "foo")
		if err := os.Mkdir(symlinkTarget, os.ModePerm); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(symlinkTarget, fp.Join(target, childName)); err != nil {
			t.Fatal(err)
		}
	}, entries, nil,
		errExtractedDirToSymlink,
	)
}

func TestSingleSymlink(t *testing.T) {
	if !symlinksEnabled {
		t.Skip("symlinks disabled on this platform", symlinksEnabledErr)
	}

	targetName := "file"
	symlinkName := "symlink"

	testTarExtraction(t, nil, []tarEntry{
		&symlinkTarEntry{targetName, symlinkName},
	}, func(t *testing.T, extractDir string) {
		symlinkPath := fp.Join(extractDir, symlinkName)
		fi, err := os.Lstat(symlinkPath)
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, fi.Mode()&os.ModeSymlink != 0, true, "expected to be a symlink")
		targetPath, err := os.Readlink(symlinkPath)
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, targetName, targetPath)
	}, nil)
}

func TestMultipleRoots(t *testing.T) {
	testTarExtraction(t, nil, []tarEntry{
		&dirTarEntry{"root"},
		&dirTarEntry{"sibling"},
	}, nil, errInvalidRoot)
}

func TestMultipleRootsNested(t *testing.T) {
	testTarExtraction(t, nil, []tarEntry{
		&dirTarEntry{"root/child1"},
		&dirTarEntry{"root/child2"},
	}, nil, errInvalidRoot)
}

func TestOutOfOrderRoot(t *testing.T) {
	testTarExtraction(t, nil, []tarEntry{
		&dirTarEntry{"root/child"},
		&dirTarEntry{"root"},
	}, nil, errInvalidRoot)
}

func TestOutOfOrder(t *testing.T) {
	testTarExtraction(t, nil, []tarEntry{
		&dirTarEntry{"root/child/grandchild"},
		&dirTarEntry{"root/child"},
	}, nil, errInvalidRoot)
}

func TestNestedDirectories(t *testing.T) {
	testTarExtraction(t, nil, []tarEntry{
		&dirTarEntry{"root"},
		&dirTarEntry{"root/child"},
		&dirTarEntry{"root/child/grandchild"},
	}, func(t *testing.T, extractDir string) {
		walkIndex := 0
		err := fp.Walk(extractDir,
			func(path string, info os.FileInfo, err error) error {
				if err != nil {
					return err
				}
				switch walkIndex {
				case 0:
					assert.Equal(t, info.Name(), tarOutRoot)
				case 1:
					assert.Equal(t, info.Name(), "child")
				case 2:
					assert.Equal(t, info.Name(), "grandchild")
				default:
					assert.Fail(t, "has more than 3 entries", path)
				}
				walkIndex++
				return nil
			})
		assert.NoError(t, err)
	}, nil)
}

func TestRootDirectoryHasSubpath(t *testing.T) {
	testTarExtraction(t, nil, []tarEntry{
		&dirTarEntry{"root/child"},
		&dirTarEntry{"root/child/grandchild"},
	}, nil, errInvalidRoot)
}

func TestFilesAndFolders(t *testing.T) {
	testTarExtraction(t, nil, []tarEntry{
		&dirTarEntry{"root"},
		&dirTarEntry{"root/childdir"},
		&fileTarEntry{"root/childdir/file1", []byte("some data")},
	}, nil, nil)
}

func TestInternalSymlinkTraverse(t *testing.T) {
	if !symlinksEnabled {
		t.Skip("symlinks disabled on this platform", symlinksEnabledErr)
	}
	testTarExtraction(t, nil, []tarEntry{
		// FIXME: We are ignoring the first element in the path check so
		//  we add a directory at the start to bypass this.
		&dirTarEntry{"root"},
		&dirTarEntry{"root/child"},
		&symlinkTarEntry{"child", "root/symlink-dir"},
		&fileTarEntry{"root/symlink-dir/file", []byte("file")},
	},
		nil,
		errTraverseSymlink,
	)
}

func TestExternalSymlinkTraverse(t *testing.T) {
	if !symlinksEnabled {
		t.Skip("symlinks disabled on this platform", symlinksEnabledErr)
	}
	testTarExtraction(t, nil, []tarEntry{
		// FIXME: We are ignoring the first element in the path check so
		//  we add a directory at the start to bypass this.
		&dirTarEntry{"inner"},
		&symlinkTarEntry{"..", "inner/symlink-dir"},
		&fileTarEntry{"inner/symlink-dir/file", []byte("overwrite content")},
	},
		nil,
		errTraverseSymlink,
	)
}

func TestLastElementOverwrite(t *testing.T) {
	if !symlinksEnabled {
		t.Skip("symlinks disabled on this platform", symlinksEnabledErr)
	}
	const originalData = "original"
	testTarExtraction(t, func(t *testing.T, rootDir string) {
		// Create an outside target that will try to be overwritten.
		// This file will reside outside of the extraction directory root.
		f, err := os.Create(fp.Join(rootDir, "outside-ref"))
		assert.NoError(t, err)
		n, err := f.WriteString(originalData)
		assert.NoError(t, err)
		assert.Equal(t, len(originalData), n)
	},
		[]tarEntry{
			&dirTarEntry{"root"},
			&symlinkTarEntry{"../outside-ref", "root/symlink"},
			&fileTarEntry{"root/symlink", []byte("overwrite content")},
		},
		func(t *testing.T, extractDir string) {
			// Check that outside-ref still exists but has not been
			// overwritten or truncated (still size the same).
			info, err := os.Stat(fp.Join(extractDir, "..", "outside-ref"))
			assert.NoError(t, err)

			assert.Equal(t, len(originalData), int(info.Size()), "outside reference has been overwritten")
		},
		nil,
	)
}

const tarOutRoot = "tar-out-root"

func testTarExtraction(t *testing.T, setup func(t *testing.T, rootDir string), tarEntries []tarEntry, check func(t *testing.T, extractDir string), extractError error) {
	var err error

	// Directory structure.
	// FIXME: We can't easily work on a MemFS since we would need to replace
	//  all the `os` calls in the extractor so using a temporary dir.
	rootDir, err := os.MkdirTemp("", "tar-extraction-test")
	assert.NoError(t, err)
	extractDir := fp.Join(rootDir, tarOutRoot)
	err = os.MkdirAll(extractDir, 0755)
	assert.NoError(t, err)

	// Generated TAR file.
	tarFilename := fp.Join(rootDir, "generated.tar")
	tarFile, err := os.Create(tarFilename)
	assert.NoError(t, err)
	defer tarFile.Close()
	tw := tar.NewWriter(tarFile)
	defer tw.Close()

	if setup != nil {
		setup(t, rootDir)
	}

	writeTarFile(t, tarFilename, tarEntries)

	testExtract(t, tarFilename, extractDir, extractError)

	if check != nil {
		check(t, extractDir)
	}
}

func testExtract(t *testing.T, tarFile string, extractDir string, expectedError error) {
	var err error

	tarReader, err := os.Open(tarFile)
	assert.NoError(t, err)

	extractor := &Extractor{Path: extractDir}
	err = extractor.Extract(tarReader)

	assert.ErrorIs(t, err, expectedError)
}

// Based on the `writeXXXHeader` family of functions in
// github.com/ipfs/go-ipfs-files@v0.0.8/tarwriter.go.
func writeTarFile(t *testing.T, path string, entries []tarEntry) {
	tarFile, err := os.Create(path)
	assert.NoError(t, err)
	defer tarFile.Close()

	tw := tar.NewWriter(tarFile)
	defer tw.Close()

	for _, e := range entries {
		err = e.write(tw)
		assert.NoError(t, err)
	}
}

type tarEntry interface {
	write(tw *tar.Writer) error
}

var _ tarEntry = (*fileTarEntry)(nil)
var _ tarEntry = (*dirTarEntry)(nil)
var _ tarEntry = (*symlinkTarEntry)(nil)

type fileTarEntry struct {
	path string
	buf  []byte
}

func (e *fileTarEntry) write(tw *tar.Writer) error {
	if err := writeFileHeader(tw, e.path, uint64(len(e.buf))); err != nil {
		return err
	}

	if _, err := io.Copy(tw, bytes.NewReader(e.buf)); err != nil {
		return err
	}

	tw.Flush()
	return nil
}
func writeFileHeader(w *tar.Writer, fpath string, size uint64) error {
	return w.WriteHeader(&tar.Header{
		Name:     fpath,
		Size:     int64(size),
		Typeflag: tar.TypeReg,
		Mode:     0644,
		ModTime:  time.Now(),
		// TODO: set mode, dates, etc. when added to unixFS
	})
}

type dirTarEntry struct {
	path string
}

func (e *dirTarEntry) write(tw *tar.Writer) error {
	return tw.WriteHeader(&tar.Header{
		Name:     e.path,
		Typeflag: tar.TypeDir,
		Mode:     0777,
		ModTime:  time.Now(),
		// TODO: set mode, dates, etc. when added to unixFS
	})
}

type symlinkTarEntry struct {
	target string
	path   string
}

func (e *symlinkTarEntry) write(w *tar.Writer) error {
	return w.WriteHeader(&tar.Header{
		Name:     e.path,
		Linkname: e.target,
		Mode:     0777,
		Typeflag: tar.TypeSymlink,
	})
}
