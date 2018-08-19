package files

import (
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	"testing"
)

func TestWebFile(t *testing.T) {
	http.HandleFunc("/my/url/content.txt", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "Hello world!")
	})
	listener, err := net.Listen("tcp", ":18281")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	go func() {
		http.Serve(listener, nil)
	}()

	u, err := url.Parse("http://127.0.0.1:18281/my/url/content.txt")
	if err != nil {
		t.Fatal(err)
	}
	wf := NewWebFile(u)
	body, err := ioutil.ReadAll(wf)
	if err != nil {
		t.Fatal(err)
	}
	if string(body) != "Hello world!" {
		t.Fatal("should have read the web file")
	}
}
