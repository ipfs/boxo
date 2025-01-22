package files

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"testing"
	"time"
)

func TestWebFile(t *testing.T) {
	const content = "Hello world!"
	const mode = 0o644
	mtime := time.Unix(16043205005, 0)

	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Add(LastModifiedHeaderName, mtime.Format(time.RFC1123))
		w.Header().Add(FileModeHeaderName, strconv.FormatUint(uint64(mode), 8))
		fmt.Fprint(w, content)
	}))
	defer s.Close()

	u, err := url.Parse(s.URL)
	if err != nil {
		t.Fatal(err)
	}
	wf := NewWebFile(u)
	body, err := io.ReadAll(wf)
	if err != nil {
		t.Fatal(err)
	}
	if string(body) != content {
		t.Fatalf("expected %q but got %q", content, string(body))
	}
	if actual := wf.Mode(); actual != mode {
		t.Fatalf("expected file mode %q but got 0%q", mode, strconv.FormatUint(uint64(actual), 8))
	}
	if actual := wf.ModTime(); !actual.Equal(mtime) {
		t.Fatalf("expected last modified time %q but got %q", mtime, actual)
	}
}

func TestWebFile_notFound(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "File not found.", http.StatusNotFound)
	}))
	defer s.Close()

	u, err := url.Parse(s.URL)
	if err != nil {
		t.Fatal(err)
	}
	wf := NewWebFile(u)
	_, err = io.ReadAll(wf)
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestWebFileSize(t *testing.T) {
	body := "Hello world!"
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, body)
	}))
	defer s.Close()

	u, err := url.Parse(s.URL)
	if err != nil {
		t.Fatal(err)
	}

	// Read size before reading file.

	wf1 := NewWebFile(u)
	if size, err := wf1.Size(); err != nil {
		t.Error(err)
	} else if int(size) != len(body) {
		t.Errorf("expected size to be %d, got %d", len(body), size)
	}

	actual, err := io.ReadAll(wf1)
	if err != nil {
		t.Fatal(err)
	}
	if string(actual) != body {
		t.Fatal("should have read the web file")
	}

	wf1.Close()

	// Read size after reading file.

	wf2 := NewWebFile(u)
	actual, err = io.ReadAll(wf2)
	if err != nil {
		t.Fatal(err)
	}
	if string(actual) != body {
		t.Fatal("should have read the web file")
	}

	if size, err := wf2.Size(); err != nil {
		t.Error(err)
	} else if int(size) != len(body) {
		t.Errorf("expected size to be %d, got %d", len(body), size)
	}
}
