// Package blockservice implements a BlockService interface that provides
// a single GetBlock/AddBlock interface that seamlessly retrieves data either
// locally or from a remote peer through the exchange.
package blockservice

import (
	"context"
	"sync"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/ipfs/boxo/blockstore"
	"github.com/ipfs/boxo/exchange"
	"github.com/ipfs/boxo/verifcid"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	ipld "github.com/ipfs/go-ipld-format"
	logging "github.com/ipfs/go-log/v2"

	"github.com/ipfs/boxo/blockservice/internal"
)

var logger = logging.Logger("blockservice")

// BlockGetter is the common interface shared between blockservice sessions and
// the blockservice.
type BlockGetter interface {
	// GetBlock gets the requested block.
	GetBlock(ctx context.Context, c cid.Cid) (blocks.Block, error)

	// GetBlocks does a batch request for the given cids, returning blocks as
	// they are found, in no particular order.
	//
	// It may not be able to find all requested blocks (or the context may
	// be canceled). In that case, it will close the channel early. It is up
	// to the consumer to detect this situation and keep track which blocks
	// it has received and which it hasn't.
	GetBlocks(ctx context.Context, ks []cid.Cid) <-chan blocks.Block
}

// Blocker returns err != nil if the CID is disallowed to be fetched or stored in blockservice.
// It returns an error so error messages could be passed.
type Blocker func(cid.Cid) error

// BlockService is a hybrid block datastore. It stores data in a local
// datastore and may retrieve data from a remote Exchange.
type BlockService struct {
	allowlist  verifcid.Allowlist
	blocker    Blocker
	blockstore blockstore.Blockstore
	exchange   exchange.Interface
	// If checkFirst is true then first check that a block doesn't
	// already exist to avoid republishing the block on the exchange.
	checkFirst bool
}

type Option func(*BlockService)

// WriteThrough disable cache checks for writes and make them go straight to
// the blockstore.
func WriteThrough() Option {
	return func(bs *BlockService) {
		bs.checkFirst = false
	}
}

// WithAllowlist sets a custom [verifcid.Allowlist] which will be used
func WithAllowlist(allowlist verifcid.Allowlist) Option {
	return func(bs *BlockService) {
		bs.allowlist = allowlist
	}
}

// WithContentBlocker allows to filter what blocks can be fetched or added to the blockservice.
func WithContentBlocker(blocker Blocker) Option {
	return func(bs *BlockService) {
		bs.blocker = blocker
	}
}

// New creates a BlockService with given datastore instance.
func New(bs blockstore.Blockstore, exchange exchange.Interface, opts ...Option) *BlockService {
	if exchange == nil {
		logger.Debug("blockservice running in local (offline) mode.")
	}

	service := &BlockService{
		allowlist:  verifcid.DefaultAllowlist,
		blockstore: bs,
		exchange:   exchange,
		checkFirst: true,
	}

	for _, opt := range opts {
		opt(service)
	}

	return service
}

// NewSession creates a new session that allows for
// controlled exchange of wantlists to decrease the bandwidth overhead.
// If the current exchange is a SessionExchange, a new exchange
// session will be created. Otherwise, the current exchange will be used
// directly.
// Sessions are lazily setup, this is cheap.
func (s *BlockService) NewSession(ctx context.Context) *Session {
	ses := grabSessionFromContext(ctx, s)
	if ses != nil {
		return ses
	}

	return s.newSession(ctx)
}

// newSession is like [NewSession] but it does not attempt to reuse session from the existing context.
func (s *BlockService) newSession(ctx context.Context) *Session {
	return &Session{bs: s, sesctx: ctx}
}

// AddBlock adds a particular block to the service, Putting it into the datastore.
func (s *BlockService) AddBlock(ctx context.Context, o blocks.Block) error {
	ctx, span := internal.StartSpan(ctx, "blockService.AddBlock")
	defer span.End()

	c := o.Cid()
	err := verifcid.ValidateCid(s.allowlist, c) // hash security
	if err != nil {
		return err
	}

	if s.blocker != nil {
		if err := s.blocker(c); err != nil {
			return err
		}
	}

	if s.checkFirst {
		if has, err := s.blockstore.Has(ctx, c); has || err != nil {
			return err
		}
	}

	if err := s.blockstore.Put(ctx, o); err != nil {
		return err
	}

	logger.Debugf("BlockService.BlockAdded %s", c)

	if s.exchange != nil {
		if err := s.exchange.NotifyNewBlocks(ctx, o); err != nil {
			logger.Errorf("NotifyNewBlocks: %s", err.Error())
		}
	}

	return nil
}

func (s *BlockService) AddBlocks(ctx context.Context, bs []blocks.Block) error {
	ctx, span := internal.StartSpan(ctx, "blockService.AddBlocks")
	defer span.End()

	// hash security
	for _, b := range bs {
		c := b.Cid()
		err := verifcid.ValidateCid(s.allowlist, c)
		if err != nil {
			return err
		}

		if s.blocker != nil {
			if err := s.blocker(c); err != nil {
				return err
			}
		}
	}
	var toput []blocks.Block
	if s.checkFirst {
		toput = make([]blocks.Block, 0, len(bs))
		for _, b := range bs {
			has, err := s.blockstore.Has(ctx, b.Cid())
			if err != nil {
				return err
			}
			if !has {
				toput = append(toput, b)
			}
		}
	} else {
		toput = bs
	}

	if len(toput) == 0 {
		return nil
	}

	err := s.blockstore.PutMany(ctx, toput)
	if err != nil {
		return err
	}

	if s.exchange != nil {
		logger.Debugf("BlockService.BlockAdded %d blocks", len(toput))
		if err := s.exchange.NotifyNewBlocks(ctx, toput...); err != nil {
			logger.Errorf("NotifyNewBlocks: %s", err.Error())
		}
	}
	return nil
}

// GetBlock retrieves a particular block from the service,
// Getting it from the datastore using the key (hash).
func (s *BlockService) GetBlock(ctx context.Context, c cid.Cid) (blocks.Block, error) {
	if ses := grabSessionFromContext(ctx, s); ses != nil {
		return ses.GetBlock(ctx, c)
	}

	ctx, span := internal.StartSpan(ctx, "blockService.GetBlock", trace.WithAttributes(attribute.Stringer("CID", c)))
	defer span.End()

	return s.getBlock(ctx, c, s.getExchangeFetcher)
}

// Look at what I have to do, no interface covariance :'(
func (s *BlockService) getExchangeFetcher() exchange.Fetcher {
	return s.exchange
}

func (s *BlockService) getBlock(ctx context.Context, c cid.Cid, fetchFactory func() exchange.Fetcher) (blocks.Block, error) {
	err := verifcid.ValidateCid(s.allowlist, c) // hash security
	if err != nil {
		return nil, err
	}

	if s.blocker != nil {
		if err := s.blocker(c); err != nil {
			return nil, err
		}
	}

	block, err := s.blockstore.Get(ctx, c)
	switch {
	case err == nil:
		return block, nil
	case ipld.IsNotFound(err):
		break
	default:
		return nil, err
	}

	fetch := fetchFactory() // lazily create session if needed
	if fetch == nil {
		logger.Debug("BlockService GetBlock: Not found")
		return nil, err
	}

	logger.Debug("BlockService: Searching")
	blk, err := fetch.GetBlock(ctx, c)
	if err != nil {
		return nil, err
	}
	// also write in the blockstore for caching, inform the exchange that the block is available
	err = s.blockstore.Put(ctx, blk)
	if err != nil {
		return nil, err
	}
	if s.exchange != nil {
		err = s.exchange.NotifyNewBlocks(ctx, blk)
		if err != nil {
			return nil, err
		}
	}
	logger.Debugf("BlockService.BlockFetched %s", c)
	return blk, nil
}

// GetBlocks gets a list of blocks asynchronously and returns through
// the returned channel.
// NB: No guarantees are made about order.
func (s *BlockService) GetBlocks(ctx context.Context, ks []cid.Cid) <-chan blocks.Block {
	if ses := grabSessionFromContext(ctx, s); ses != nil {
		return ses.GetBlocks(ctx, ks)
	}

	ctx, span := internal.StartSpan(ctx, "blockService.GetBlocks")
	defer span.End()

	return s.getBlocks(ctx, ks, s.getExchangeFetcher)
}

func (s *BlockService) getBlocks(ctx context.Context, ks []cid.Cid, fetchFactory func() exchange.Fetcher) <-chan blocks.Block {
	out := make(chan blocks.Block)

	go func() {
		defer close(out)

		var lastAllValidIndex int
		var c cid.Cid
		for lastAllValidIndex, c = range ks {
			if err := verifcid.ValidateCid(s.allowlist, c); err != nil {
				break
			}

			if s.blocker != nil {
				if err := s.blocker(c); err != nil {
					break
				}
			}
		}

		if lastAllValidIndex != len(ks) {
			// can't shift in place because we don't want to clobber callers.
			ks2 := make([]cid.Cid, lastAllValidIndex, len(ks))
			copy(ks2, ks[:lastAllValidIndex])          // fast path for already filtered elements
			for _, c := range ks[lastAllValidIndex:] { // don't rescan already scanned elements
				// hash security
				if err := verifcid.ValidateCid(s.allowlist, c); err != nil {
					logger.Errorf("unsafe CID (%s) passed to blockService.GetBlocks: %s", c, err)
					continue
				}

				if s.blocker != nil {
					if err := s.blocker(c); err != nil {
						logger.Errorf("blocked CID (%s) passed to blockService.GetBlocks: %s", c, err)
						continue
					}
				}

				ks2 = append(ks2, c)
			}
			ks = ks2
		}

		var misses []cid.Cid
		for _, c := range ks {
			hit, err := s.blockstore.Get(ctx, c)
			if err != nil {
				misses = append(misses, c)
				continue
			}
			select {
			case out <- hit:
			case <-ctx.Done():
				return
			}
		}

		fetch := fetchFactory() // don't load exchange unless we have to
		if len(misses) == 0 || fetch == nil {
			return
		}

		rblocks, err := fetch.GetBlocks(ctx, misses)
		if err != nil {
			logger.Debugf("Error with GetBlocks: %s", err)
			return
		}

		var cache [1]blocks.Block // preallocate once for all iterations
		for {
			var b blocks.Block
			select {
			case v, ok := <-rblocks:
				if !ok {
					return
				}
				b = v
			case <-ctx.Done():
				return
			}

			// write in the blockstore for caching
			err = s.blockstore.Put(ctx, b)
			if err != nil {
				logger.Errorf("could not write blocks from the network to the blockstore: %s", err)
				return
			}

			if s.exchange != nil {
				// inform the exchange that the blocks are available
				cache[0] = b
				err = s.exchange.NotifyNewBlocks(ctx, cache[:]...)
				if err != nil {
					logger.Errorf("could not tell the exchange about new blocks: %s", err)
					return
				}
				cache[0] = nil // early gc
			}

			select {
			case out <- b:
			case <-ctx.Done():
				return
			}
		}
	}()
	return out
}

// DeleteBlock deletes a block in the blockservice from the datastore
func (s *BlockService) DeleteBlock(ctx context.Context, c cid.Cid) error {
	ctx, span := internal.StartSpan(ctx, "blockService.DeleteBlock", trace.WithAttributes(attribute.Stringer("CID", c)))
	defer span.End()

	err := s.blockstore.DeleteBlock(ctx, c)
	if err == nil {
		logger.Debugf("BlockService.BlockDeleted %s", c)
	}
	return err
}

func (s *BlockService) Close() error {
	logger.Debug("blockservice is shutting down...")
	if s.exchange == nil {
		return nil
	}
	return s.exchange.Close()
}

// Session is a helper type to provide higher level access to bitswap sessions
type Session struct {
	createSession sync.Once
	bs            *BlockService
	ses           exchange.Fetcher
	sesctx        context.Context
}

// grabSession is used to lazily create sessions.
func (s *Session) grabSession() exchange.Fetcher {
	s.createSession.Do(func() {
		defer func() {
			s.sesctx = nil // early gc
		}()

		if s.bs.exchange == nil {
			return
		}

		sesEx, ok := s.bs.exchange.(exchange.SessionExchange)
		if !ok {
			s.ses = s.bs.exchange // always fallback to non session fetches
			return
		}
		s.ses = sesEx.NewSession(s.sesctx)
	})

	return s.ses
}

// GetBlock gets a block in the context of a request session
func (s *Session) GetBlock(ctx context.Context, c cid.Cid) (blocks.Block, error) {
	ctx, span := internal.StartSpan(ctx, "Session.GetBlock", trace.WithAttributes(attribute.Stringer("CID", c)))
	defer span.End()

	return s.bs.getBlock(ctx, c, s.grabSession)
}

// GetBlocks gets blocks in the context of a request session
func (s *Session) GetBlocks(ctx context.Context, ks []cid.Cid) <-chan blocks.Block {
	ctx, span := internal.StartSpan(ctx, "Session.GetBlocks")
	defer span.End()

	return s.bs.getBlocks(ctx, ks, s.grabSession)
}

var _ BlockGetter = (*Session)(nil)

// ContextWithSession is a helper which creates a context with an embded session,
// future calls to [BlockGetter.GetBlock], [BlockGetter.GetBlocks] and [NewSession] with the same [BlockService]
// will be redirected to this same session instead.
// Sessions are lazily setup, this is cheap.
// It wont make a new session if one exists already in the context.
func (s *BlockService) ContextWithSession(ctx context.Context) context.Context {
	if grabSessionFromContext(ctx, s) != nil {
		return ctx
	}
	return EmbedSessionInContext(ctx, s.newSession(ctx))
}

// EmbedSessionInContext is like [ContextWithSession] but it allows to embed an existing session.
func EmbedSessionInContext(ctx context.Context, ses *Session) context.Context {
	// use ses.bs as a key, so if multiple blockservices use embeded sessions it gets dispatched to the matching blockservice.
	return context.WithValue(ctx, ses.bs, ses)
}

// grabSessionFromContext returns nil if the session was not found
// This is a private API on purposes, I dislike when consumers tradeoff compiletime typesafety with runtime typesafety,
// if this API is public it is too easy to forget to pass a [BlockService] or [Session] object around in your app.
// By having this private we allow consumers to follow the trace of where the blockservice is passed and used.
func grabSessionFromContext(ctx context.Context, bs *BlockService) *Session {
	s := ctx.Value(bs)
	if s == nil {
		return nil
	}

	ss, ok := s.(*Session)
	if !ok {
		// idk what to do here, that kinda sucks, giveup
		return nil
	}

	return ss
}

func (s *BlockService) Has(ctx context.Context, c cid.Cid) (bool, error) {
	if err := verifcid.ValidateCid(s.allowlist, c); err != nil {
		return false, err
	}

	if s.blocker != nil {
		if err := s.blocker(c); err != nil {
			return false, err
		}
	}

	return s.blockstore.Has(ctx, c)
}
