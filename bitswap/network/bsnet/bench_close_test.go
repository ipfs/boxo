package bsnet_test

import (
	"context"
	"testing"
	"time"

	tnet "github.com/libp2p/go-libp2p-testing/net"
)

// BenchmarkSendMessageHappyPath measures the per-call cost of SendMessage
// against a live peer. With the async-close change, the caller returns as
// soon as the bytes are written; the goroutine cost of spawning Close is the
// main extra work on the happy path.
func BenchmarkSendMessageHappyPath(b *testing.B) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	p1, err := tnet.RandIdentity()
	if err != nil {
		b.Fatal(err)
	}
	p2, err := tnet.RandIdentity()
	if err != nil {
		b.Fatal(err)
	}
	r1 := newReceiver()
	r2 := newReceiver()
	_, bsnet1, _, _, msg := prepareNetwork(b, ctx, p1, r1, p2, r2)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if err := bsnet1.SendMessage(ctx, p2.ID(), msg); err != nil {
			b.Fatalf("SendMessage failed: %v", err)
		}
	}
}

// BenchmarkSendMessageHappyPathParallel measures concurrent SendMessage
// throughput. With the old synchronous close, goroutines would serialize on
// each Close; with async close, they pipeline freely.
func BenchmarkSendMessageHappyPathParallel(b *testing.B) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	p1, err := tnet.RandIdentity()
	if err != nil {
		b.Fatal(err)
	}
	p2, err := tnet.RandIdentity()
	if err != nil {
		b.Fatal(err)
	}
	r1 := newReceiver()
	r2 := newReceiver()
	_, bsnet1, _, _, msg := prepareNetwork(b, ctx, p1, r1, p2, r2)

	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err := bsnet1.SendMessage(ctx, p2.ID(), msg); err != nil {
				b.Fatalf("SendMessage failed: %v", err)
			}
		}
	})
}
