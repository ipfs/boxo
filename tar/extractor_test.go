package tar

import (
	"archive/tar"
	"bytes"
	"fmt"
	"io"
	"io/fs"
	"os"
	fp "path/filepath"
	"runtime"
	"syscall"
	"testing"
	"time"

	"github.com/ipfs/boxo/files"
	"github.com/stretchr/testify/assert"
)

var (
	symlinksEnabled    bool
	symlinksEnabledErr error
)

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
		&fileTarEntry{path: fileName, buf: []byte(fileData)},
	},
		func(t *testing.T, extractDir string) {
			f, err := os.Open(fp.Join(extractDir, fileName))
			assert.NoError(t, err)
			t.Cleanup(func() {
				assert.NoError(t, f.Close())
			})
			data, err := io.ReadAll(f)
			assert.NoError(t, err)
			assert.Equal(t, fileData, string(data))
		},
		nil,
	)
}

func TestSingleFileWithMeta(t *testing.T) {
	fileName := "file2..ext"
	fileData := "file2 data"
	mode := 0o654
	mtime := time.Now().Round(time.Second)

	testTarExtraction(t, nil, []tarEntry{
		&fileTarEntry{path: fileName, buf: []byte(fileData), mode: mode, mtime: mtime},
	},
		func(t *testing.T, extractDir string) {
			path := fp.Join(extractDir, fileName)
			testMeta(t, path, mode, mtime)
			f, err := os.Open(path)
			assert.NoError(t, err)
			t.Cleanup(func() {
				assert.NoError(t, f.Close())
			})
			data, err := io.ReadAll(f)
			assert.NoError(t, err)
			assert.Equal(t, fileData, string(data))
		},
		nil,
	)
}

func TestSingleDirectory(t *testing.T) {
	dirName := "dir..sfx"

	testTarExtraction(t, nil, []tarEntry{
		&dirTarEntry{path: dirName},
	},
		func(t *testing.T, extractDir string) {
			f, err := os.Open(extractDir)
			assert.NoError(t, err)
			t.Cleanup(func() {
				f.Close()
			})
			objs, err := f.Readdir(1)
			if err == io.EOF && len(objs) == 0 {
				return
			}
			t.Fatalf("expected an empty directory")
		},
		nil,
	)
}

func TestSingleDirectoryWithMeta(t *testing.T) {
	dirName := "dir2..sfx"
	mode := 0o765
	mtime := time.Now().Round(time.Second)

	testTarExtraction(t, nil, []tarEntry{
		&dirTarEntry{path: dirName, mode: mode, mtime: mtime},
	},
		func(t *testing.T, extractDir string) {
			testMeta(t, extractDir, mode, mtime)
			f, err := os.Open(extractDir)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				f.Close()
			})
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
		&dirTarEntry{path: dirName},
		&dirTarEntry{path: dirName + "/" + childName},
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
		&dirTarEntry{path: dirName},
		&dirTarEntry{path: dirName + "/" + childName},
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
		&dirTarEntry{path: dirName},
		&dirTarEntry{path: dirName + "/" + childName},
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
		&symlinkTarEntry{target: targetName, path: symlinkName},
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
		&dirTarEntry{path: "root"},
		&dirTarEntry{path: "sibling"},
	}, nil, errInvalidRoot)
}

func TestMultipleRootsNested(t *testing.T) {
	testTarExtraction(t, nil, []tarEntry{
		&dirTarEntry{path: "root/child1"},
		&dirTarEntry{path: "root/child2"},
	}, nil, errInvalidRoot)
}

func TestOutOfOrderRoot(t *testing.T) {
	testTarExtraction(t, nil, []tarEntry{
		&dirTarEntry{path: "root/child"},
		&dirTarEntry{path: "root"},
	}, nil, errInvalidRoot)
}

func TestOutOfOrder(t *testing.T) {
	testTarExtraction(t, nil, []tarEntry{
		&dirTarEntry{path: "root/child/grandchild"},
		&dirTarEntry{path: "root/child"},
	}, nil, errInvalidRoot)
}

func TestNestedDirectories(t *testing.T) {
	testTarExtraction(t, nil, []tarEntry{
		&dirTarEntry{path: "root"},
		&dirTarEntry{path: "root/child"},
		&dirTarEntry{path: "root/child/grandchild"},
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
		&dirTarEntry{path: "root/child"},
		&dirTarEntry{path: "root/child/grandchild"},
	}, nil, errInvalidRoot)
}

func TestFilesAndFolders(t *testing.T) {
	testTarExtraction(t, nil, []tarEntry{
		&dirTarEntry{path: "root"},
		&dirTarEntry{path: "root/childdir"},
		&fileTarEntry{path: "root/childdir/file1", buf: []byte("some data")},
	}, nil, nil)
}

func TestFilesAndFoldersWithMetadata(t *testing.T) {
	tm := time.Unix(660000000, 0)

	entries := []tarEntry{
		&dirTarEntry{path: "root", mtime: tm.Add(5 * time.Second)},
		&dirTarEntry{path: "root/childdir", mode: 0o3775},
		&fileTarEntry{
			path: "root/childdir/file1", buf: []byte("some data"), mode: 0o4744,
			mtime: tm.Add(10 * time.Second),
		},
		&fileTarEntry{
			path: "root/childdir/file2", buf: []byte("some data"), mode: 0o560,
			mtime: tm.Add(10 * time.Second),
		},
		&fileTarEntry{
			path: "root/childdir/file3", buf: []byte("some data"), mode: 0o6540,
			mtime: tm.Add(10 * time.Second),
		},
	}

	testTarExtraction(t, nil, entries, func(t *testing.T, extractDir string) {
		walkIndex := 0
		err := fp.Walk(extractDir,
			func(path string, fi os.FileInfo, err error) error {
				if err != nil {
					return err
				}
				switch walkIndex {
				case 0: // root
					assert.Equal(t, tm.Add(5*time.Second), fi.ModTime())
				case 1: // childdir
					if runtime.GOOS != "windows" {
						assert.Equal(t, 0o775, int(fi.Mode()&0xFFF))
						assert.Equal(t, os.ModeSetgid, fi.Mode()&os.ModeSetgid)
						assert.Equal(t, os.ModeSticky, fi.Mode()&os.ModeSticky)
					} else {
						assert.Equal(t, 0o777, int(fi.Mode()&0xFFF))
					}
				case 2: // file1
					assert.Equal(t, tm.Add(10*time.Second), fi.ModTime())
					if runtime.GOOS != "windows" {
						assert.Equal(t, 0o744, int(fi.Mode()&0xFFF))
						assert.Equal(t, os.ModeSetuid, fi.Mode()&os.ModeSetuid)
					} else {
						assert.Equal(t, 0o666, int(fi.Mode()&0xFFF))
					}
				case 3: // file2
					assert.Equal(t, tm.Add(10*time.Second), fi.ModTime())
					if runtime.GOOS != "windows" {
						assert.Equal(t, 0o560, int(fi.Mode()&0xFFF))
						assert.Equal(t, 0, int(fi.Mode()&os.ModeSetuid))
					} else {
						assert.Equal(t, 0o666, int(fi.Mode()&0xFFF))
					}
				case 4: // file3
					assert.Equal(t, tm.Add(10*time.Second), fi.ModTime())
					if runtime.GOOS != "windows" {
						assert.Equal(t, 0o540, int(fi.Mode()&0xFFF))
						assert.Equal(t, os.ModeSetgid, fi.Mode()&os.ModeSetgid)
						assert.Equal(t, os.ModeSetuid, fi.Mode()&os.ModeSetuid)
					} else {
						assert.Equal(t, 0o444, int(fi.Mode()&0xFFF))
					}
				default:
					assert.Fail(t, "has more than 5 entries", path)
				}
				walkIndex++
				return nil
			})
		assert.NoError(t, err)
	},
		nil)
}

func TestSymlinkWithModTime(t *testing.T) {
	if !symlinksEnabled {
		t.Skip("symlinks disabled on this platform", symlinksEnabledErr)
	}
	if runtime.GOOS == "darwin" {
		t.Skip("changing symlink modification time is not currently supported on darwin")
	}
	tm := time.Unix(660000000, 0)
	add5 := func() time.Time {
		tm = tm.Add(5 * time.Second)
		return tm
	}

	entries := []tarEntry{
		&dirTarEntry{path: "root"},
		&symlinkTarEntry{target: "child", path: "root/a", mtime: add5()},
		&dirTarEntry{path: "root/child"},
		&fileTarEntry{path: "root/child/file1", buf: []byte("data")},
		&symlinkTarEntry{target: "child/file1", path: "root/file1-sl", mtime: add5()},
	}

	testTarExtraction(t, nil, entries, func(t *testing.T, extractDir string) {
		tm = time.Unix(660000000, 0)

		fi, err := os.Lstat(fp.Join(extractDir, "a"))
		assert.NoError(t, err)
		add5()
		if runtime.GOOS != "windows" {
			assert.Equal(t, tm, fi.ModTime())
		}

		fi, err = os.Lstat(fp.Join(extractDir, "file1-sl"))
		assert.NoError(t, err)
		add5()
		if runtime.GOOS != "windows" {
			assert.Equal(t, tm, fi.ModTime())
		}
	},
		nil)
}

func TestDeferredUpdate(t *testing.T) {
	tm := time.Unix(660000000, 0)
	add5 := func() time.Time {
		tm = tm.Add(5 * time.Second)
		return tm
	}

	// must be in lexical order
	entries := []tarEntry{
		&dirTarEntry{path: "root", mtime: add5()},
		&dirTarEntry{path: "root/a", mtime: add5()},
		&dirTarEntry{path: "root/a/beta", mtime: add5(), mode: 0o500},
		&dirTarEntry{path: "root/a/beta/centauri", mtime: add5()},
		&dirTarEntry{path: "root/a/beta/lima", mtime: add5()},
		&dirTarEntry{path: "root/a/beta/papa", mtime: add5()},
		&dirTarEntry{path: "root/a/beta/xanadu", mtime: add5()},
		&dirTarEntry{path: "root/a/beta/z", mtime: add5()},
		&dirTarEntry{path: "root/a/delta", mtime: add5()},
		&dirTarEntry{path: "root/iota", mtime: add5()},
		&dirTarEntry{path: "root/q", mtime: add5()},
	}

	testTarExtraction(t, nil, entries, func(t *testing.T, extractDir string) {
		tm = time.Unix(660000000, 0)
		err := fp.Walk(extractDir,
			func(path string, fi os.FileInfo, err error) error {
				if err != nil {
					return err
				}

				assert.Equal(t, add5(), fi.ModTime())
				return nil
			})
		assert.NoError(t, err)
	},
		nil)
}

func TestInternalSymlinkTraverse(t *testing.T) {
	if !symlinksEnabled {
		t.Skip("symlinks disabled on this platform", symlinksEnabledErr)
	}
	testTarExtraction(t, nil, []tarEntry{
		// FIXME: We are ignoring the first element in the path check so
		//  we add a directory at the start to bypass this.
		&dirTarEntry{path: "root"},
		&dirTarEntry{path: "root/child"},
		&symlinkTarEntry{target: "child", path: "root/symlink-dir"},
		&fileTarEntry{path: "root/symlink-dir/file", buf: []byte("file")},
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
		&dirTarEntry{path: "inner"},
		&symlinkTarEntry{target: "..", path: "inner/symlink-dir"},
		&fileTarEntry{path: "inner/symlink-dir/file", buf: []byte("overwrite content")},
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
		t.Cleanup(func() {
			f.Close()
		})
		n, err := f.WriteString(originalData)
		assert.NoError(t, err)
		assert.Equal(t, len(originalData), n)
	},
		[]tarEntry{
			&dirTarEntry{path: "root"},
			&symlinkTarEntry{target: "../outside-ref", path: "root/symlink"},
			&fileTarEntry{path: "root/symlink", buf: []byte("overwrite content")},
		},
		func(t *testing.T, extractDir string) {
			// Check that outside-ref still exists but has not been overwritten
			// or truncated (still size the same). The symlink itself have been
			// overwritten by the extracted file.
			info, err := os.Stat(fp.Join(extractDir, "..", "outside-ref"))
			assert.NoError(t, err)

			assert.Equal(t, len(originalData), int(info.Size()), "outside reference has been overwritten")
		},
		nil,
	)
}

const tarOutRoot = "tar-out-root"

func testTarExtraction(t *testing.T, setup func(t *testing.T, rootDir string), tarEntries []tarEntry, check func(t *testing.T, extractDir string), extractError error) {
	// Directory structure.
	// FIXME: We can't easily work on a MemFS since we would need to replace
	//  all the `os` calls in the extractor so using a temporary dir.
	rootDir := t.TempDir()
	t.Cleanup(func() {
		chmodRecursive(t, rootDir)
	})
	extractDir := fp.Join(rootDir, tarOutRoot)
	err := os.MkdirAll(extractDir, 0o755)
	assert.NoError(t, err)

	if setup != nil {
		setup(t, rootDir)
	}

	// Generated TAR file.
	tarFilename := fp.Join(rootDir, "generated.tar")
	writeTarFile(t, tarFilename, tarEntries)

	testExtract(t, tarFilename, extractDir, extractError)

	if check != nil {
		check(t, extractDir)
	}
}

func chmodRecursive(t *testing.T, path string) {
	t.Helper()
	err := fp.WalkDir(path, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		return os.Chmod(path, fs.FileMode(0700))
	})
	if err != nil {
		t.Log("ERROR:", err)
	}
}

func testExtract(t *testing.T, tarFile string, extractDir string, expectedError error) {
	tarReader, err := os.Open(tarFile)
	assert.NoError(t, err)
	defer tarReader.Close()

	extractor := &Extractor{Path: extractDir}
	err = extractor.Extract(tarReader)

	assert.ErrorIs(t, err, expectedError)
}

func testMeta(t *testing.T, path string, mode int, now time.Time) {
	fi, err := os.Lstat(path)
	assert.NoError(t, err)
	m := files.ModePermsToUnixPerms(fi.Mode())
	if runtime.GOOS == "windows" {
		if fi.IsDir() {
			mode = 0o777
		} else if mode&0o220 != 0 {
			mode = 0o666
		} else if mode&0o440 != 0 {
			mode = 0o444
		}
	}
	assert.Equal(t, mode, int(m))
	assert.Equal(t, now.Unix(), fi.ModTime().Unix())
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

var (
	_ tarEntry = (*fileTarEntry)(nil)
	_ tarEntry = (*dirTarEntry)(nil)
	_ tarEntry = (*symlinkTarEntry)(nil)
)

type fileTarEntry struct {
	path  string
	buf   []byte
	mode  int
	mtime time.Time
}

func (e *fileTarEntry) write(tw *tar.Writer) error {
	err := writeFileHeader(tw, e.path, uint64(len(e.buf)), e.mode, e.mtime)
	if err != nil {
		return err
	}

	if _, err = io.Copy(tw, bytes.NewReader(e.buf)); err != nil {
		return err
	}

	tw.Flush()
	return nil
}

func writeFileHeader(w *tar.Writer, fpath string, size uint64, mode int, mtime time.Time) error {
	return w.WriteHeader(&tar.Header{
		Name:     fpath,
		Size:     int64(size),
		Typeflag: tar.TypeReg,
		Mode:     int64(mode),
		ModTime:  mtime,
	})
}

type dirTarEntry struct {
	path  string
	mode  int
	mtime time.Time
}

func (e *dirTarEntry) write(tw *tar.Writer) error {
	return tw.WriteHeader(&tar.Header{
		Name:     e.path,
		Typeflag: tar.TypeDir,
		Mode:     int64(e.mode),
		ModTime:  e.mtime,
	})
}

type symlinkTarEntry struct {
	target string
	path   string
	mtime  time.Time
}

func (e *symlinkTarEntry) write(w *tar.Writer) error {
	return w.WriteHeader(&tar.Header{
		Name:     e.path,
		Linkname: e.target,
		Mode:     0o777,
		ModTime:  e.mtime,
		Typeflag: tar.TypeSymlink,
	})
}
