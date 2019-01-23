package providerquerymanager

import (
	"context"
	"sync"

	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log"
	peer "github.com/libp2p/go-libp2p-peer"
)

var log = logging.Logger("bitswap")

const (
	maxProviders         = 10
	maxInProcessRequests = 6
)

type inProgressRequestStatus struct {
	providersSoFar []peer.ID
	listeners      map[uint64]chan peer.ID
}

// ProviderQueryNetwork is an interface for finding providers and connecting to
// peers.
type ProviderQueryNetwork interface {
	ConnectTo(context.Context, peer.ID) error
	FindProvidersAsync(context.Context, cid.Cid, int) <-chan peer.ID
}

type providerQueryMessage interface {
	handle(pqm *ProviderQueryManager)
}

type receivedProviderMessage struct {
	k cid.Cid
	p peer.ID
}

type finishedProviderQueryMessage struct {
	k cid.Cid
}

type newProvideQueryMessage struct {
	ses                   uint64
	k                     cid.Cid
	inProgressRequestChan chan<- inProgressRequest
}

type cancelRequestMessage struct {
	ses uint64
	k   cid.Cid
}

// ProviderQueryManager manages requests to find more providers for blocks
// for bitswap sessions. It's main goals are to:
// - rate limit requests -- don't have too many find provider calls running
// simultaneously
// - connect to found peers and filter them if it can't connect
// - ensure two findprovider calls for the same block don't run concurrently
// TODO:
// - manage timeouts
type ProviderQueryManager struct {
	ctx                   context.Context
	network               ProviderQueryNetwork
	providerQueryMessages chan providerQueryMessage

	// do not touch outside the run loop
	providerRequestsProcessing   chan cid.Cid
	incomingFindProviderRequests chan cid.Cid
	inProgressRequestStatuses    map[cid.Cid]*inProgressRequestStatus
}

// New initializes a new ProviderQueryManager for a given context and a given
// network provider.
func New(ctx context.Context, network ProviderQueryNetwork) *ProviderQueryManager {
	return &ProviderQueryManager{
		ctx:                          ctx,
		network:                      network,
		providerQueryMessages:        make(chan providerQueryMessage, 16),
		providerRequestsProcessing:   make(chan cid.Cid),
		incomingFindProviderRequests: make(chan cid.Cid),
		inProgressRequestStatuses:    make(map[cid.Cid]*inProgressRequestStatus),
	}
}

// Startup starts processing for the ProviderQueryManager.
func (pqm *ProviderQueryManager) Startup() {
	go pqm.run()
}

type inProgressRequest struct {
	providersSoFar []peer.ID
	incoming       <-chan peer.ID
}

// FindProvidersAsync finds providers for the given block.
func (pqm *ProviderQueryManager) FindProvidersAsync(sessionCtx context.Context, k cid.Cid, ses uint64) <-chan peer.ID {
	inProgressRequestChan := make(chan inProgressRequest)

	select {
	case pqm.providerQueryMessages <- &newProvideQueryMessage{
		ses:                   ses,
		k:                     k,
		inProgressRequestChan: inProgressRequestChan,
	}:
	case <-pqm.ctx.Done():
		return nil
	case <-sessionCtx.Done():
		return nil
	}

	var receivedInProgressRequest inProgressRequest
	select {
	case <-sessionCtx.Done():
		return nil
	case receivedInProgressRequest = <-inProgressRequestChan:
	}

	return pqm.receiveProviders(sessionCtx, k, ses, receivedInProgressRequest)
}

func (pqm *ProviderQueryManager) receiveProviders(sessionCtx context.Context, k cid.Cid, ses uint64, receivedInProgressRequest inProgressRequest) <-chan peer.ID {
	// maintains an unbuffered queue for incoming providers for given request for a given session
	// essentially, as a provider comes in, for a given CID, we want to immediately broadcast to all
	// sessions that queried that CID, without worrying about whether the client code is actually
	// reading from the returned channel -- so that the broadcast never blocks
	// based on: https://medium.com/capital-one-tech/building-an-unbounded-channel-in-go-789e175cd2cd
	returnedProviders := make(chan peer.ID)
	receivedProviders := append([]peer.ID(nil), receivedInProgressRequest.providersSoFar[0:]...)
	incomingProviders := receivedInProgressRequest.incoming

	go func() {
		defer close(returnedProviders)
		outgoingProviders := func() chan<- peer.ID {
			if len(receivedProviders) == 0 {
				return nil
			}
			return returnedProviders
		}
		nextProvider := func() peer.ID {
			if len(receivedProviders) == 0 {
				return ""
			}
			return receivedProviders[0]
		}
		for len(receivedProviders) > 0 || incomingProviders != nil {
			select {
			case <-sessionCtx.Done():
				pqm.providerQueryMessages <- &cancelRequestMessage{
					ses: ses,
					k:   k,
				}
				// clear out any remaining providers
				for range incomingProviders {
				}
				return
			case provider, ok := <-incomingProviders:
				if !ok {
					incomingProviders = nil
				} else {
					receivedProviders = append(receivedProviders, provider)
				}
			case outgoingProviders() <- nextProvider():
				receivedProviders = receivedProviders[1:]
			}
		}
	}()
	return returnedProviders
}

func (pqm *ProviderQueryManager) findProviderWorker() {
	// findProviderWorker just cycles through incoming provider queries one
	// at a time. We have six of these workers running at once
	// to let requests go in parallel but keep them rate limited
	for {
		select {
		case k, ok := <-pqm.providerRequestsProcessing:
			if !ok {
				return
			}

			providers := pqm.network.FindProvidersAsync(pqm.ctx, k, maxProviders)
			wg := &sync.WaitGroup{}
			for p := range providers {
				wg.Add(1)
				go func(p peer.ID) {
					defer wg.Done()
					err := pqm.network.ConnectTo(pqm.ctx, p)
					if err != nil {
						log.Debugf("failed to connect to provider %s: %s", p, err)
						return
					}
					select {
					case pqm.providerQueryMessages <- &receivedProviderMessage{
						k: k,
						p: p,
					}:
					case <-pqm.ctx.Done():
						return
					}
				}(p)
			}
			wg.Wait()
			select {
			case pqm.providerQueryMessages <- &finishedProviderQueryMessage{
				k: k,
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
	var providerQueryRequestBuffer []cid.Cid
	nextProviderQuery := func() cid.Cid {
		if len(providerQueryRequestBuffer) == 0 {
			return cid.Cid{}
		}
		return providerQueryRequestBuffer[0]
	}
	outgoingRequests := func() chan<- cid.Cid {
		if len(providerQueryRequestBuffer) == 0 {
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
			providerQueryRequestBuffer = append(providerQueryRequestBuffer, incomingRequest)
		case outgoingRequests() <- nextProviderQuery():
			providerQueryRequestBuffer = providerQueryRequestBuffer[1:]
		case <-pqm.ctx.Done():
			return
		}
	}
}

func (pqm *ProviderQueryManager) cleanupInProcessRequests() {
	for _, requestStatus := range pqm.inProgressRequestStatuses {
		for _, listener := range requestStatus.listeners {
			close(listener)
		}
	}
}

func (pqm *ProviderQueryManager) run() {
	defer close(pqm.incomingFindProviderRequests)
	defer close(pqm.providerRequestsProcessing)
	defer pqm.cleanupInProcessRequests()

	go pqm.providerRequestBufferWorker()
	for i := 0; i < maxInProcessRequests; i++ {
		go pqm.findProviderWorker()
	}

	for {
		select {
		case nextMessage := <-pqm.providerQueryMessages:
			nextMessage.handle(pqm)
		case <-pqm.ctx.Done():
			return
		}
	}
}

func (rpm *receivedProviderMessage) handle(pqm *ProviderQueryManager) {
	requestStatus, ok := pqm.inProgressRequestStatuses[rpm.k]
	if !ok {
		log.Errorf("Received provider (%s) for cid (%s) not requested", rpm.p.String(), rpm.k.String())
		return
	}
	requestStatus.providersSoFar = append(requestStatus.providersSoFar, rpm.p)
	for _, listener := range requestStatus.listeners {
		select {
		case listener <- rpm.p:
		case <-pqm.ctx.Done():
			return
		}
	}
}

func (fpqm *finishedProviderQueryMessage) handle(pqm *ProviderQueryManager) {
	requestStatus, ok := pqm.inProgressRequestStatuses[fpqm.k]
	if !ok {
		log.Errorf("Ended request for cid (%s) not in progress", fpqm.k.String())
		return
	}
	for _, listener := range requestStatus.listeners {
		close(listener)
	}
	delete(pqm.inProgressRequestStatuses, fpqm.k)
}

func (npqm *newProvideQueryMessage) handle(pqm *ProviderQueryManager) {
	requestStatus, ok := pqm.inProgressRequestStatuses[npqm.k]
	if !ok {
		requestStatus = &inProgressRequestStatus{
			listeners: make(map[uint64]chan peer.ID),
		}
		pqm.inProgressRequestStatuses[npqm.k] = requestStatus
		select {
		case pqm.incomingFindProviderRequests <- npqm.k:
		case <-pqm.ctx.Done():
			return
		}
	}
	requestStatus.listeners[npqm.ses] = make(chan peer.ID)
	select {
	case npqm.inProgressRequestChan <- inProgressRequest{
		providersSoFar: requestStatus.providersSoFar,
		incoming:       requestStatus.listeners[npqm.ses],
	}:
	case <-pqm.ctx.Done():
	}
}

func (crm *cancelRequestMessage) handle(pqm *ProviderQueryManager) {
	requestStatus, ok := pqm.inProgressRequestStatuses[crm.k]
	if !ok {
		log.Errorf("Attempt to cancel request for session (%d) for cid (%s) not in progress", crm.ses, crm.k.String())
		return
	}
	listener, ok := requestStatus.listeners[crm.ses]
	if !ok {
		log.Errorf("Attempt to cancel request for session (%d) for cid (%s) this is not a listener", crm.ses, crm.k.String())
		return
	}
	close(listener)
	delete(requestStatus.listeners, crm.ses)
}
