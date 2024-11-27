package providerquerymanager

import (
	"context"
	"sync"
	"time"

	"github.com/gammazero/chanqueue"
	"github.com/gammazero/deque"
	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	peer "github.com/libp2p/go-libp2p/core/peer"
	swarm "github.com/libp2p/go-libp2p/p2p/net/swarm"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap/zapcore"
)

var log = logging.Logger("routing/provqrymgr")

const (
	defaultMaxInProcessRequests = 16
	defaultMaxProviders         = 0
	defaultTimeout              = 10 * time.Second
)

type inProgressRequestStatus struct {
	ctx            context.Context
	cancelFn       func()
	providersSoFar []peer.AddrInfo
	listeners      map[chan peer.AddrInfo]struct{}
}

type findProviderRequest struct {
	k   cid.Cid
	ctx context.Context
}

// ProviderQueryDialer is an interface for connecting to peers. Usually a
// libp2p.Host
type ProviderQueryDialer interface {
	Connect(context.Context, peer.AddrInfo) error
}

// ProviderQueryRouter is an interface for finding providers. Usually a libp2p
// ContentRouter.
type ProviderQueryRouter interface {
	FindProvidersAsync(context.Context, cid.Cid, int) <-chan peer.AddrInfo
}

type providerQueryMessage interface {
	debugMessage()
	handle(pqm *ProviderQueryManager)
}

type receivedProviderMessage struct {
	ctx context.Context
	k   cid.Cid
	p   peer.AddrInfo
}

type finishedProviderQueryMessage struct {
	ctx context.Context
	k   cid.Cid
}

type newProvideQueryMessage struct {
	ctx                   context.Context
	k                     cid.Cid
	inProgressRequestChan chan<- inProgressRequest
}

type cancelRequestMessage struct {
	ctx               context.Context
	incomingProviders chan peer.AddrInfo
	k                 cid.Cid
}

// ProviderQueryManager manages requests to find more providers for blocks
// for bitswap sessions. It's main goals are to:
// - rate limit requests -- don't have too many find provider calls running
// simultaneously
// - connect to found peers and filter them if it can't connect
// - ensure two findprovider calls for the same block don't run concurrently
// - manage timeouts
type ProviderQueryManager struct {
	ctx                        context.Context
	dialer                     ProviderQueryDialer
	router                     ProviderQueryRouter
	providerQueryMessages      chan providerQueryMessage
	providerRequestsProcessing *chanqueue.ChanQueue[*findProviderRequest]

	findProviderTimeout time.Duration

	maxProviders         int
	maxInProcessRequests int

	// do not touch outside the run loop
	inProgressRequestStatuses map[cid.Cid]*inProgressRequestStatus
}

type Option func(*ProviderQueryManager) error

func WithMaxTimeout(timeout time.Duration) Option {
	return func(mgr *ProviderQueryManager) error {
		mgr.findProviderTimeout = timeout
		return nil
	}
}

// WithMaxInProcessRequests is the maximum number of requests that can be
// processed in parallel. If this is 0, then the number is unlimited. Default
// is defaultMaxInProcessRequests (16).
func WithMaxInProcessRequests(count int) Option {
	return func(mgr *ProviderQueryManager) error {
		mgr.maxInProcessRequests = count
		return nil
	}
}

// WithMaxProviders is the maximum number of providers that will be looked up
// per query. We only return providers that we can connect to. Defaults to 0,
// which means unbounded.
func WithMaxProviders(count int) Option {
	return func(mgr *ProviderQueryManager) error {
		mgr.maxProviders = count
		return nil
	}
}

// New initializes a new ProviderQueryManager for a given context and a given
// network provider.
func New(ctx context.Context, dialer ProviderQueryDialer, router ProviderQueryRouter, opts ...Option) (*ProviderQueryManager, error) {
	pqm := &ProviderQueryManager{
		ctx:                   ctx,
		dialer:                dialer,
		router:                router,
		providerQueryMessages: make(chan providerQueryMessage),
		findProviderTimeout:   defaultTimeout,
		maxInProcessRequests:  defaultMaxInProcessRequests,
		maxProviders:          defaultMaxProviders,
	}

	for _, o := range opts {
		if err := o(pqm); err != nil {
			return nil, err
		}
	}

	return pqm, nil
}

// Startup starts processing for the ProviderQueryManager.
func (pqm *ProviderQueryManager) Startup() {
	go pqm.run()
}

type inProgressRequest struct {
	providersSoFar []peer.AddrInfo
	incoming       chan peer.AddrInfo
}

// FindProvidersAsync finds providers for the given block. The max parameter
// controls how many will be returned at most. For a provider to be returned,
// we must have successfully connected to it. Setting max to 0 will use the
// configured MaxProviders which defaults to 0 (unbounded).
func (pqm *ProviderQueryManager) FindProvidersAsync(sessionCtx context.Context, k cid.Cid, max int) <-chan peer.AddrInfo {
	if max == 0 {
		max = pqm.maxProviders
	}

	inProgressRequestChan := make(chan inProgressRequest)

	var span trace.Span
	sessionCtx, span = otel.Tracer("routing").Start(sessionCtx, "ProviderQueryManager.FindProvidersAsync", trace.WithAttributes(attribute.Stringer("cid", k)))

	select {
	case pqm.providerQueryMessages <- &newProvideQueryMessage{
		ctx:                   sessionCtx,
		k:                     k,
		inProgressRequestChan: inProgressRequestChan,
	}:
	case <-pqm.ctx.Done():
		ch := make(chan peer.AddrInfo)
		close(ch)
		span.End()
		return ch
	}

	// DO NOT select on sessionCtx. We only want to abort here if we're
	// shutting down because we can't actually _cancel_ the request till we
	// get to receiveProviders.
	var receivedInProgressRequest inProgressRequest
	select {
	case <-pqm.ctx.Done():
		ch := make(chan peer.AddrInfo)
		close(ch)
		span.End()
		return ch
	case receivedInProgressRequest = <-inProgressRequestChan:
	}

	return pqm.receiveProviders(sessionCtx, k, max, receivedInProgressRequest, func() { span.End() })
}

func (pqm *ProviderQueryManager) receiveProviders(sessionCtx context.Context, k cid.Cid, max int, receivedInProgressRequest inProgressRequest, onCloseFn func()) <-chan peer.AddrInfo {
	// maintains an unbuffered queue for incoming providers for given request
	// for a given session. Essentially, as a provider comes in, for a given
	// CID, immediately broadcast to all sessions that queried that CID,
	// without worrying about whether the client code is actually reading from
	// the returned channel -- so that the broadcast never blocks.
	returnedProviders := make(chan peer.AddrInfo)
	var receivedProviders deque.Deque[peer.AddrInfo]
	receivedProviders.Grow(len(receivedInProgressRequest.providersSoFar))
	for _, addrInfo := range receivedInProgressRequest.providersSoFar {
		receivedProviders.PushBack(addrInfo)
	}
	incomingProviders := receivedInProgressRequest.incoming

	// count how many providers we received from our workers etc.
	// these providers should be peers we managed to connect to.
	total := receivedProviders.Len()
	go func() {
		defer close(returnedProviders)
		defer onCloseFn()
		outgoingProviders := func() chan<- peer.AddrInfo {
			if receivedProviders.Len() == 0 {
				return nil
			}
			return returnedProviders
		}
		nextProvider := func() peer.AddrInfo {
			if receivedProviders.Len() == 0 {
				return peer.AddrInfo{}
			}
			return receivedProviders.Front()
		}

		stopWhenMaxReached := func() {
			if max > 0 && total >= max {
				if incomingProviders != nil {
					// drains incomingProviders.
					pqm.cancelProviderRequest(sessionCtx, k, incomingProviders)
					incomingProviders = nil
				}
			}
		}

		// Handle the case when providersSoFar already is more than we
		// need.
		stopWhenMaxReached()

		for receivedProviders.Len() > 0 || incomingProviders != nil {
			select {
			case <-pqm.ctx.Done():
				return
			case <-sessionCtx.Done():
				if incomingProviders != nil {
					pqm.cancelProviderRequest(sessionCtx, k, incomingProviders)
				}
				return
			case provider, ok := <-incomingProviders:
				if !ok {
					incomingProviders = nil
				} else {
					receivedProviders.PushBack(provider)
					total++
					stopWhenMaxReached()
					// we do not return, we will loop on
					// the case below until
					// len(receivedProviders) == 0, which
					// means they have all been sent out
					// via returnedProviders
				}
			case outgoingProviders() <- nextProvider():
				receivedProviders.PopFront()
			}
		}
	}()
	return returnedProviders
}

func (pqm *ProviderQueryManager) cancelProviderRequest(ctx context.Context, k cid.Cid, incomingProviders chan peer.AddrInfo) {
	cancelMessageChannel := pqm.providerQueryMessages
	for {
		select {
		case cancelMessageChannel <- &cancelRequestMessage{
			ctx:               ctx,
			incomingProviders: incomingProviders,
			k:                 k,
		}:
			cancelMessageChannel = nil
		// clear out any remaining providers, in case and "incoming provider"
		// messages get processed before our cancel message
		case _, ok := <-incomingProviders:
			if !ok {
				return
			}
		case <-pqm.ctx.Done():
			return
		}
	}
}

// findProviderWorker cycles through incoming provider queries one at a time.
func (pqm *ProviderQueryManager) findProviderWorker() {
	var findSem chan struct{}
	// If limiting the number of concurrent requests, create a counting
	// semaphore to enforce this limit.
	if pqm.maxInProcessRequests > 0 {
		findSem = make(chan struct{}, pqm.maxInProcessRequests)
	}

	// Read find provider requests until channel is closed. The channl is
	// closed as soon as pqm.ctx is canceled, so there is no need to select on
	// that context here.
	for fpr := range pqm.providerRequestsProcessing.Out() {
		if findSem != nil {
			select {
			case findSem <- struct{}{}:
			case <-pqm.ctx.Done():
				return
			}
		}

		go func(ctx context.Context, k cid.Cid) {
			if findSem != nil {
				defer func() {
					<-findSem
				}()
			}

			log.Debugf("Beginning Find Provider Request for cid: %s", k.String())
			findProviderCtx, cancel := context.WithTimeout(ctx, pqm.findProviderTimeout)
			span := trace.SpanFromContext(findProviderCtx)
			span.AddEvent("StartFindProvidersAsync")
			// We set count == 0. We will cancel the query manually once we
			// have enough. This assumes the ContentDiscovery
			// implementation does that, which a requirement per the
			// libp2p/core/routing interface.
			providers := pqm.router.FindProvidersAsync(findProviderCtx, k, 0)
			wg := &sync.WaitGroup{}
			for p := range providers {
				wg.Add(1)
				go func(p peer.AddrInfo) {
					defer wg.Done()
					span.AddEvent("FoundProvider", trace.WithAttributes(attribute.Stringer("peer", p.ID)))
					err := pqm.dialer.Connect(findProviderCtx, p)
					if err != nil && err != swarm.ErrDialToSelf {
						span.RecordError(err, trace.WithAttributes(attribute.Stringer("peer", p.ID)))
						log.Debugf("failed to connect to provider %s: %s", p.ID, err)
						return
					}
					span.AddEvent("ConnectedToProvider", trace.WithAttributes(attribute.Stringer("peer", p.ID)))
					select {
					case pqm.providerQueryMessages <- &receivedProviderMessage{
						ctx: ctx,
						k:   k,
						p:   p,
					}:
					case <-pqm.ctx.Done():
						return
					}
				}(p)
			}
			wg.Wait()
			cancel()
			select {
			case pqm.providerQueryMessages <- &finishedProviderQueryMessage{
				ctx: ctx,
				k:   k,
			}:
			case <-pqm.ctx.Done():
			}
		}(fpr.ctx, fpr.k)
	}
}

func (pqm *ProviderQueryManager) cleanupInProcessRequests() {
	for _, requestStatus := range pqm.inProgressRequestStatuses {
		for listener := range requestStatus.listeners {
			close(listener)
		}
		requestStatus.cancelFn()
	}
}

func (pqm *ProviderQueryManager) run() {
	defer pqm.cleanupInProcessRequests()

	pqm.providerRequestsProcessing = chanqueue.New[*findProviderRequest]()
	defer pqm.providerRequestsProcessing.Shutdown()

	go pqm.findProviderWorker()

	for {
		select {
		case nextMessage := <-pqm.providerQueryMessages:
			nextMessage.debugMessage()
			nextMessage.handle(pqm)
		case <-pqm.ctx.Done():
			return
		}
	}
}

func (rpm *receivedProviderMessage) debugMessage() {
	log.Debugf("Received provider (%s) (%s)", rpm.p, rpm.k)
	trace.SpanFromContext(rpm.ctx).AddEvent("ReceivedProvider", trace.WithAttributes(attribute.Stringer("provider", rpm.p), attribute.Stringer("cid", rpm.k)))
}

func (rpm *receivedProviderMessage) handle(pqm *ProviderQueryManager) {
	requestStatus, ok := pqm.inProgressRequestStatuses[rpm.k]
	if !ok {
		log.Debugf("Received provider (%s) for cid (%s) not requested", rpm.p.String(), rpm.k.String())
		return
	}
	requestStatus.providersSoFar = append(requestStatus.providersSoFar, rpm.p)
	for listener := range requestStatus.listeners {
		select {
		case listener <- rpm.p:
		case <-pqm.ctx.Done():
			return
		}
	}
}

func (fpqm *finishedProviderQueryMessage) debugMessage() {
	log.Debugf("Finished Provider Query on cid: %s", fpqm.k)
	trace.SpanFromContext(fpqm.ctx).AddEvent("FinishedProviderQuery", trace.WithAttributes(attribute.Stringer("cid", fpqm.k)))
}

func (fpqm *finishedProviderQueryMessage) handle(pqm *ProviderQueryManager) {
	requestStatus, ok := pqm.inProgressRequestStatuses[fpqm.k]
	if !ok {
		// we canceled the request as it finished.
		return
	}
	for listener := range requestStatus.listeners {
		close(listener)
	}
	delete(pqm.inProgressRequestStatuses, fpqm.k)
	if len(pqm.inProgressRequestStatuses) == 0 {
		pqm.inProgressRequestStatuses = nil
	}
	requestStatus.cancelFn()
}

func (npqm *newProvideQueryMessage) debugMessage() {
	log.Debugf("New Provider Query on cid: %s", npqm.k)
	trace.SpanFromContext(npqm.ctx).AddEvent("NewProvideQuery", trace.WithAttributes(attribute.Stringer("cid", npqm.k)))
}

func (npqm *newProvideQueryMessage) handle(pqm *ProviderQueryManager) {
	requestStatus, ok := pqm.inProgressRequestStatuses[npqm.k]
	if !ok {
		ctx, cancelFn := context.WithCancel(pqm.ctx)
		span := trace.SpanFromContext(npqm.ctx)
		span.AddEvent("NewQuery", trace.WithAttributes(attribute.Stringer("cid", npqm.k)))
		ctx = trace.ContextWithSpan(ctx, span)

		// Use context derived from pqm.ctx here, and not the context from the
		// request (npqm.ctx), because this inProgressRequestStatus applies to
		// all in-progress requests for the CID (npqm.k).
		//
		// For tracing, this means that only the span from the first
		// request-in-progress for a CID is used, even if there are multiple
		// requests for the same CID.
		requestStatus = &inProgressRequestStatus{
			listeners: make(map[chan peer.AddrInfo]struct{}),
			ctx:       ctx,
			cancelFn:  cancelFn,
		}

		if pqm.inProgressRequestStatuses == nil {
			pqm.inProgressRequestStatuses = make(map[cid.Cid]*inProgressRequestStatus)
		}
		pqm.inProgressRequestStatuses[npqm.k] = requestStatus

		select {
		case pqm.providerRequestsProcessing.In() <- &findProviderRequest{
			k:   npqm.k,
			ctx: ctx,
		}:
		case <-pqm.ctx.Done():
			return
		}
	} else {
		trace.SpanFromContext(npqm.ctx).AddEvent("JoinQuery", trace.WithAttributes(attribute.Stringer("cid", npqm.k)))
		if log.Level().Enabled(zapcore.DebugLevel) {
			log.Debugf("Joined existing query for cid %s which now has %d queries in progress", npqm.k, len(requestStatus.listeners)+1)
		}
	}
	inProgressChan := make(chan peer.AddrInfo)
	requestStatus.listeners[inProgressChan] = struct{}{}
	select {
	case npqm.inProgressRequestChan <- inProgressRequest{
		providersSoFar: requestStatus.providersSoFar,
		incoming:       inProgressChan,
	}:
	case <-pqm.ctx.Done():
	}
}

func (crm *cancelRequestMessage) debugMessage() {
	log.Debugf("Cancel provider query on cid: %s", crm.k)
	trace.SpanFromContext(crm.ctx).AddEvent("CancelRequest", trace.WithAttributes(attribute.Stringer("cid", crm.k)))
}

func (crm *cancelRequestMessage) handle(pqm *ProviderQueryManager) {
	requestStatus, ok := pqm.inProgressRequestStatuses[crm.k]
	if !ok {
		// Request finished while queued.
		return
	}
	_, ok = requestStatus.listeners[crm.incomingProviders]
	if !ok {
		// Request finished and _restarted_ while queued.
		return
	}
	delete(requestStatus.listeners, crm.incomingProviders)
	close(crm.incomingProviders)
	if len(requestStatus.listeners) == 0 {
		delete(pqm.inProgressRequestStatuses, crm.k)
		if len(pqm.inProgressRequestStatuses) == 0 {
			pqm.inProgressRequestStatuses = nil
		}
		requestStatus.cancelFn()
	}
}
