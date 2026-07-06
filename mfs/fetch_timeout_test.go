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
