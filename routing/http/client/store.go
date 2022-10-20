package client

import (
	"context"
	"fmt"

	"github.com/ipfs/go-ipns"
	"github.com/libp2p/go-libp2p-core/routing"
	record "github.com/libp2p/go-libp2p-record"
)

var _ routing.ValueStore = &ValueStore{}

type ValueStore struct {
	Client Client
}

func (s *ValueStore) PutValue(ctx context.Context, key string, val []byte, opts ...routing.Option) error {
	ns, path, err := record.SplitKey(key)
	if err != nil {
		return fmt.Errorf("invalid key: %w", err)
	}

	if ns != "ipns" {
		return ipns.ErrKeyFormat
	}

	return s.Client.PutIPNSRecord(ctx, []byte(path), val)
}

func (s *ValueStore) GetValue(ctx context.Context, key string, opts ...routing.Option) ([]byte, error) {
	ns, path, err := record.SplitKey(key)
	if err != nil {
		return nil, fmt.Errorf("invalid key: %w", err)
	}

	if ns != "ipns" {
		return nil, ipns.ErrKeyFormat
	}

	return s.Client.GetIPNSRecord(ctx, []byte(path))
}

func (s *ValueStore) SearchValue(ctx context.Context, key string, opts ...routing.Option) (<-chan []byte, error) {
	ns, path, err := record.SplitKey(key)
	if err != nil {
		return nil, fmt.Errorf("invalid key: %w", err)
	}

	if ns != "ipns" {
		return nil, ipns.ErrKeyFormat
	}

	recordBytes, err := s.Client.GetIPNSRecord(ctx, []byte(path))
	if err != nil {
		return nil, err
	}
	ch := make(chan []byte, 1)
	ch <- recordBytes
	close(ch)

	return ch, nil
}
