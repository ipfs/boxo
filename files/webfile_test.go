package files

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
)

func TestWebFile(t *testing.T) {
	const content = "Hello world!"
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, content)
	}))
	defer s.Close()

	u, err := url.Parse(s.URL)
	if err != nil {
		t.Fatal(err)
	}
	wf := NewWebFile(u)
	body, err := ioutil.ReadAll(wf)
	if err != nil {
		t.Fatal(err)
	}
	if string(body) != content {
		t.Fatalf("expected %q but got %q", content, string(body))
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
	_, err = ioutil.ReadAll(wf)
	if err == nil {
		t.Fatal("expected error")
	}
}
