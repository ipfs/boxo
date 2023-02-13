package main

import (
	"context"

	"github.com/libp2p/go-libp2p/core/routing"
)

type staticRouting struct {
}

func newStaticRouting() routing.ValueStore {
	return &staticRouting{}
}

func (s *staticRouting) PutValue(context.Context, string, []byte, ...routing.Option) error {
	return routing.ErrNotSupported
}

func (s *staticRouting) GetValue(ctx context.Context, k string, opts ...routing.Option) ([]byte, error) {
	return nil, routing.ErrNotSupported
}

func (s *staticRouting) SearchValue(ctx context.Context, k string, opts ...routing.Option) (<-chan []byte, error) {
	return nil, routing.ErrNotSupported
}
