package providerquerymanager

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gammazero/deque"
	"github.com/ipfs/boxo/bitswap/client/internal"
	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	peer "github.com/libp2p/go-libp2p/core/peer"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

var log = logging.Logger("bitswap/client/provqrymgr")

const (
	maxProviders         = 10
	maxInProcessRequests = 6
	defaultTimeout       = 10 * time.Second
)

type inProgressRequestStatus struct {
	ctx            context.Context
	cancelFn       func()
	providersSoFar []peer.ID
	listeners      map[chan peer.ID]struct{}
}

type findProviderRequest struct {
	k   cid.Cid
	ctx context.Context
}

// ProviderQueryNetwork is an interface for finding providers and connecting to
// peers.
type ProviderQueryNetwork interface {
	ConnectTo(context.Context, peer.ID) error
	FindProvidersAsync(context.Context, cid.Cid, int) <-chan peer.ID
}

type providerQueryMessage interface {
	debugMessage()
	handle(pqm *ProviderQueryManager)
}

type receivedProviderMessage struct {
	ctx context.Context
	k   cid.Cid
	p   peer.ID
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
	incomingProviders chan peer.ID
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
	ctx                          context.Context
	network                      ProviderQueryNetwork
	providerQueryMessages        chan providerQueryMessage
	providerRequestsProcessing   chan *findProviderRequest
	incomingFindProviderRequests chan *findProviderRequest

	findProviderTimeout atomic.Int64

	// do not touch outside the run loop
	inProgressRequestStatuses map[cid.Cid]*inProgressRequestStatus
}

// New initializes a new ProviderQueryManager for a given context and a given
// network provider.
func New(ctx context.Context, network ProviderQueryNetwork) *ProviderQueryManager {
	pqm := &ProviderQueryManager{
		ctx:                          ctx,
		network:                      network,
		providerQueryMessages:        make(chan providerQueryMessage, 16),
		providerRequestsProcessing:   make(chan *findProviderRequest),
		incomingFindProviderRequests: make(chan *findProviderRequest),
	}
	pqm.SetFindProviderTimeout(defaultTimeout)
	return pqm
}

// Startup starts processing for the ProviderQueryManager.
func (pqm *ProviderQueryManager) Startup() {
	go pqm.run()
}

type inProgressRequest struct {
	providersSoFar []peer.ID
	incoming       chan peer.ID
}

// SetFindProviderTimeout changes the timeout for finding providers
func (pqm *ProviderQueryManager) SetFindProviderTimeout(findProviderTimeout time.Duration) {
	pqm.findProviderTimeout.Store(int64(findProviderTimeout))
}

// FindProvidersAsync finds providers for the given block.
func (pqm *ProviderQueryManager) FindProvidersAsync(sessionCtx context.Context, k cid.Cid) <-chan peer.ID {
	inProgressRequestChan := make(chan inProgressRequest)

	var span trace.Span
	sessionCtx, span = internal.StartSpan(sessionCtx, "ProviderQueryManager.FindProvidersAsync", trace.WithAttributes(attribute.Stringer("cid", k)))

	select {
	case pqm.providerQueryMessages <- &newProvideQueryMessage{
		ctx:                   sessionCtx,
		k:                     k,
		inProgressRequestChan: inProgressRequestChan,
	}:
	case <-pqm.ctx.Done():
		ch := make(chan peer.ID)
		close(ch)
		span.End()
		return ch
	case <-sessionCtx.Done():
		ch := make(chan peer.ID)
		close(ch)
		return ch
	}

	// DO NOT select on sessionCtx. We only want to abort here if we're
	// shutting down because we can't actually _cancel_ the request till we
	// get to receiveProviders.
	var receivedInProgressRequest inProgressRequest
	select {
	case <-pqm.ctx.Done():
		ch := make(chan peer.ID)
		close(ch)
		span.End()
		return ch
	case receivedInProgressRequest = <-inProgressRequestChan:
	}

	return pqm.receiveProviders(sessionCtx, k, receivedInProgressRequest, func() { span.End() })
}

func (pqm *ProviderQueryManager) receiveProviders(sessionCtx context.Context, k cid.Cid, receivedInProgressRequest inProgressRequest, onCloseFn func()) <-chan peer.ID {
	// maintains an unbuffered queue for incoming providers for given request for a given session
	// essentially, as a provider comes in, for a given CID, we want to immediately broadcast to all
	// sessions that queried that CID, without worrying about whether the client code is actually
	// reading from the returned channel -- so that the broadcast never blocks
	// based on: https://medium.com/capital-one-tech/building-an-unbounded-channel-in-go-789e175cd2cd
	returnedProviders := make(chan peer.ID)
	receivedProviders := deque.New[peer.ID]()
	for _, pid := range receivedInProgressRequest.providersSoFar {
		receivedProviders.PushBack(pid)
	}
	incomingProviders := receivedInProgressRequest.incoming

	go func() {
		defer close(returnedProviders)
		defer onCloseFn()
		outgoingProviders := func() chan<- peer.ID {
			if receivedProviders.Len() == 0 {
				return nil
			}
			return returnedProviders
		}
		nextProvider := func() peer.ID {
			if receivedProviders.Len() == 0 {
				return ""
			}
			return receivedProviders.Front()
		}
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
				}
			case outgoingProviders() <- nextProvider():
				receivedProviders.PopFront()
			}
		}
	}()
	return returnedProviders
}

func (pqm *ProviderQueryManager) cancelProviderRequest(ctx context.Context, k cid.Cid, incomingProviders chan peer.ID) {
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

func (pqm *ProviderQueryManager) findProviderWorker() {
	// findProviderWorker just cycles through incoming provider queries one
	// at a time. We have six of these workers running at once
	// to let requests go in parallel but keep them rate limited
	for {
		select {
		case fpr, ok := <-pqm.providerRequestsProcessing:
			if !ok {
				return
			}
			k := fpr.k
			log.Debugw("Beginning Find Provider request", "cid", k.String())
			findProviderCtx, cancel := context.WithTimeout(fpr.ctx, time.Duration(pqm.findProviderTimeout.Load()))
			span := trace.SpanFromContext(findProviderCtx)
			span.AddEvent("StartFindProvidersAsync")
			providers := pqm.network.FindProvidersAsync(findProviderCtx, k, maxProviders)
			wg := &sync.WaitGroup{}
			for p := range providers {
				wg.Add(1)
				go func(p peer.ID) {
					defer wg.Done()
					span.AddEvent("FoundProvider", trace.WithAttributes(attribute.Stringer("peer", p)))
					err := pqm.network.ConnectTo(findProviderCtx, p)
					if err != nil {
						span.RecordError(err, trace.WithAttributes(attribute.Stringer("peer", p)))
						log.Debugw("failed to connect to provider", "err", err, "peerID", p)
						return
					}
					span.AddEvent("ConnectedToProvider", trace.WithAttributes(attribute.Stringer("peer", p)))
					select {
					case pqm.providerQueryMessages <- &receivedProviderMessage{
						ctx: fpr.ctx,
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
				ctx: fpr.ctx,
				k:   k,
			}:
			case <-pqm.ctx.Done():
			}
		case <-pqm.ctx.Done():
			return
		}
	}
}

func (pqm *ProviderQueryManager) providerRequestBufferWorker() {
	// the provider request buffer worker just maintains an unbounded
	// buffer for incoming provider queries and dispatches to the find
	// provider workers as they become available
	// based on: https://medium.com/capital-one-tech/building-an-unbounded-channel-in-go-789e175cd2cd
	providerQueryRequestBuffer := deque.New[*findProviderRequest]()
	nextProviderQuery := func() *findProviderRequest {
		if providerQueryRequestBuffer.Len() == 0 {
			return nil
		}
		return providerQueryRequestBuffer.Front()
	}
	outgoingRequests := func() chan<- *findProviderRequest {
		if providerQueryRequestBuffer.Len() == 0 {
			return nil
		}
		return pqm.providerRequestsProcessing
	}

	for {
		select {
		case incomingRequest, ok := <-pqm.incomingFindProviderRequests:
			if !ok {
				return
			}
			providerQueryRequestBuffer.PushBack(incomingRequest)
		case outgoingRequests() <- nextProviderQuery():
			providerQueryRequestBuffer.PopFront()
		case <-pqm.ctx.Done():
			return
		}
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

	go pqm.providerRequestBufferWorker()
	for i := 0; i < maxInProcessRequests; i++ {
		go pqm.findProviderWorker()
	}

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
	log.Debugw("Received provider", "peerID", rpm.p, "cid", rpm.k)
	trace.SpanFromContext(rpm.ctx).AddEvent("ReceivedProvider", trace.WithAttributes(attribute.Stringer("provider", rpm.p), attribute.Stringer("cid", rpm.k)))
}

func (rpm *receivedProviderMessage) handle(pqm *ProviderQueryManager) {
	requestStatus, ok := pqm.inProgressRequestStatuses[rpm.k]
	if !ok {
		log.Debugw("Received provider not requested", "peerID", rpm.p.String(), "cid", rpm.k.String())
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
	log.Debugw("Finished Provider Query", "cid", fpqm.k)
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
	log.Debugw("New Provider Query", "cid", npqm.k)
	trace.SpanFromContext(npqm.ctx).AddEvent("NewProvideQuery", trace.WithAttributes(attribute.Stringer("cid", npqm.k)))
}

func (npqm *newProvideQueryMessage) handle(pqm *ProviderQueryManager) {
	requestStatus, ok := pqm.inProgressRequestStatuses[npqm.k]
	if !ok {
		ctx, cancelFn := context.WithCancel(pqm.ctx)
		span := trace.SpanFromContext(npqm.ctx)
		span.AddEvent("NewQuery", trace.WithAttributes(attribute.Stringer("cid", npqm.k)))
		ctx = trace.ContextWithSpan(ctx, span)

		requestStatus = &inProgressRequestStatus{
			listeners: make(map[chan peer.ID]struct{}),
			ctx:       ctx,
			cancelFn:  cancelFn,
		}

		if pqm.inProgressRequestStatuses == nil {
			pqm.inProgressRequestStatuses = make(map[cid.Cid]*inProgressRequestStatus)
		}
		pqm.inProgressRequestStatuses[npqm.k] = requestStatus

		select {
		case pqm.incomingFindProviderRequests <- &findProviderRequest{
			k:   npqm.k,
			ctx: ctx,
		}:
		case <-pqm.ctx.Done():
			return
		}
	} else {
		trace.SpanFromContext(npqm.ctx).AddEvent("JoinQuery", trace.WithAttributes(attribute.Stringer("cid", npqm.k)))
	}
	inProgressChan := make(chan peer.ID)
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
	log.Debugw("Cancel provider query", "cid", crm.k)
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
