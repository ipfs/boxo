package mfs

import (
	"context"
	"errors"
	"testing"
	"time"

	dag "github.com/ipfs/boxo/ipld/merkledag"
	ft "github.com/ipfs/boxo/ipld/unixfs"
	cid "github.com/ipfs/go-cid"
	ipld "github.com/ipfs/go-ipld-format"
)

// blockingDAGService stands in for a DAGService asked for a block that is not
// available locally and never arrives from the network: Get blocks until the
// context is done. Writes go through the embedded service.
type blockingDAGService struct{ ipld.DAGService }

func (b blockingDAGService) Get(ctx context.Context, _ cid.Cid) (ipld.Node, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

func (b blockingDAGService) GetMany(ctx context.Context, _ []cid.Cid) <-chan *ipld.NodeOption {
	ch := make(chan *ipld.NodeOption, 1)
	go func() {
		<-ctx.Done()
		ch <- &ipld.NodeOption{Err: ctx.Err()}
		close(ch)
	}()
	return ch
}

// TestWithFetchTimeout verifies that WithFetchTimeout bounds an under-lock DAG
// read: a directory lookup whose block cannot be fetched returns a deadline
// error and releases the lock, instead of blocking forever (the wedge behind
// ipfs/kubo#7008, #7844, #10842).
func TestWithFetchTimeout(t *testing.T) {
	ctx := context.Background()
	ds := getDagserv(t)

	// Build /sub/file and persist it.
	root, err := NewEmptyRoot(ctx, ds, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := Mkdir(root, "/sub", MkdirOpts{}); err != nil {
		t.Fatal(err)
	}
	child := dag.NodeWithData(ft.FilePBData([]byte("data"), 4))
	if err := ds.Add(ctx, child); err != nil {
		t.Fatal(err)
	}
	if err := PutNode(root, "/sub/file", child); err != nil {
		t.Fatal(err)
	}
	if err := root.GetDirectory().Flush(); err != nil {
		t.Fatal(err)
	}
	rootNode, err := root.GetDirectory().GetNode()
	if err != nil {
		t.Fatal(err)
	}
	pbRoot, ok := rootNode.(*dag.ProtoNode)
	if !ok {
		t.Fatalf("root node is %T, want *dag.ProtoNode", rootNode)
	}

	// Reopen over a DAGService whose reads block forever, with a short fetch
	// timeout. Resolving /sub must time out rather than hang.
	root2, err := NewRoot(ctx, blockingDAGService{ds}, pbRoot, nil, nil, WithFetchTimeout(200*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}

	done := make(chan error, 1)
	start := time.Now()
	go func() {
		_, err := root2.GetDirectory().Child("sub")
		done <- err
	}()

	select {
	case err := <-done:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Child(sub) error = %v, want context.DeadlineExceeded", err)
		}
		if elapsed := time.Since(start); elapsed > 5*time.Second {
			t.Fatalf("Child(sub) took %s; the fetch timeout was not enforced", elapsed)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Child(sub) hung; the fetch timeout was not enforced")
	}
}

// TestFetchTimeoutDefault checks that the bound is on by default at
// DefaultFetchTimeout, that a root can override it, and that WithFetchTimeout(0)
// disables it.
func TestFetchTimeoutDefault(t *testing.T) {
	ctx := context.Background()
	ds := getDagserv(t)

	cases := []struct {
		name string
		opts []Option
		want time.Duration
	}{
		{"unset uses default", nil, DefaultFetchTimeout},
		{"explicit value", []Option{WithFetchTimeout(30 * time.Second)}, 30 * time.Second},
		{"zero disables", []Option{WithFetchTimeout(0)}, 0},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			root, err := NewEmptyRoot(ctx, ds, nil, nil, tc.opts...)
			if err != nil {
				t.Fatal(err)
			}
			// The root's value is inherited by every directory under it.
			if got := root.GetDirectory().fetchTimeout; got != tc.want {
				t.Fatalf("fetchTimeout = %s, want %s", got, tc.want)
			}
		})
	}
}

// TestOpenContextCancelsFetch confirms that the context passed to File.Open
// bounds the block fetches the descriptor makes, so an operation on a file
// whose data cannot be fetched is cancelled through that context instead of
// hanging forever. Without the context threaded into Open (an earlier version
// used context.TODO), this read would block forever and the test would time
// out.
func TestOpenContextCancelsFetch(t *testing.T) {
	ctx := context.Background()
	ds := getDagserv(t)

	// Create /f, write enough content to span several blocks, and persist it.
	root, err := NewEmptyRoot(ctx, ds, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	empty := dag.NodeWithData(ft.FilePBData(nil, 0))
	if err := ds.Add(ctx, empty); err != nil {
		t.Fatal(err)
	}
	if err := PutNode(root, "/f", empty); err != nil {
		t.Fatal(err)
	}
	fsn, err := root.GetDirectory().Child("f")
	if err != nil {
		t.Fatal(err)
	}
	file := fsn.(*File)
	wfd, err := file.Open(ctx, Flags{Write: true, Sync: true})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := wfd.Write(make([]byte, 1<<20)); err != nil { // 1 MiB, multiple blocks
		t.Fatal(err)
	}
	if err := wfd.Close(); err != nil {
		t.Fatal(err)
	}
	fileNode, err := file.GetNode()
	if err != nil {
		t.Fatal(err)
	}

	// Reopen the file over a DAG service whose reads block forever, with a
	// context that expires quickly. Reading has to fetch a block that never
	// arrives; it must return when the context expires, not hang.
	blocked, err := NewFile("f", fileNode, root.GetDirectory(), blockingDAGService{ds}, nil)
	if err != nil {
		t.Fatal(err)
	}
	opctx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
	defer cancel()
	rfd, err := blocked.Open(opctx, Flags{Read: true})
	if err != nil {
		t.Fatal(err)
	}

	done := make(chan error, 1)
	go func() {
		_, err := rfd.Read(make([]byte, 16))
		done <- err
	}()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("Read over a blocking DAG service succeeded; expected a context error")
		}
		if !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
			t.Fatalf("Read error = %v, want a context deadline or cancel error", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Read hung; the context passed to File.Open was not honored")
	}
}
