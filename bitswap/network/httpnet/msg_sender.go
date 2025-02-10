package httpnet

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	bsmsg "github.com/ipfs/boxo/bitswap/message"
	pb "github.com/ipfs/boxo/bitswap/message/pb"
	"github.com/ipfs/boxo/bitswap/network"
	blocks "github.com/ipfs/go-block-format"
	"github.com/libp2p/go-libp2p/core/peer"
)

// MessageSender option defaults.
var (
	// DefaultMaxRetries specifies how many requests to make to available
	// HTTP endpoints in case of failure.
	DefaultMaxRetries = 1
	// DefaultSendTimeout specifies sending each individual HTTP
	// request can take.
	DefaultSendTimeout = 5 * time.Second
	// SendErrorBackoff specifies how long to wait between retries to the
	// same endpoint after failure. It is overriden by Retry-After
	// headers and must be at least 50ms.
	DefaultSendErrorBackoff = time.Second
)

func setSenderOpts(opts *network.MessageSenderOpts) network.MessageSenderOpts {
	defopts := network.MessageSenderOpts{
		MaxRetries:       DefaultMaxRetries,
		SendTimeout:      DefaultSendTimeout,
		SendErrorBackoff: DefaultSendErrorBackoff,
	}

	if opts == nil {
		return defopts
	}

	if mr := opts.MaxRetries; mr > 0 {
		defopts.MaxRetries = mr
	}
	if st := opts.SendTimeout; st > time.Second {
		defopts.SendTimeout = st
	}
	if seb := opts.SendErrorBackoff; seb > 50*time.Millisecond {
		defopts.SendErrorBackoff = seb
	}
	return defopts
}

// senderURL wraps url with information about cooldowns and errors.
type senderURL struct {
	network.ParsedURL
	cooldown     atomic.Value
	serverErrors atomic.Int64
}

// httpMsgSender implements a network.MessageSender.
// For NewMessageSender see func (ht *httpnet) NewMessageSender(...)
type httpMsgSender struct {
	peer      peer.ID
	urls      []*senderURL
	ht        *Network
	opts      network.MessageSenderOpts
	closing   chan struct{}
	closeOnce sync.Once
}

// For NewMessageSender see func (ht *httpnet) NewMessageSender(...)

// sortURLS sorts the sender urls as follows:
//   - urls with exhausted retries go to the end
//   - urls are sorted by cooldown (shorter first)
//   - same, or no cooldown, are sorted by number of server errors
func (sender *httpMsgSender) sortURLS() []*senderURL {
	if len(sender.urls) <= 1 {
		return sender.urls
	}

	// sender.urls must be read-only as multiple workers
	// attempt to sort it.
	urlCopy := make([]*senderURL, len(sender.urls))
	copy(urlCopy, sender.urls)

	slices.SortFunc(urlCopy, func(a, b *senderURL) int {
		// urls without exhausted retries come first
		serverErrorsA := a.serverErrors.Load()
		serverErrorsB := b.serverErrors.Load()
		if serverErrorsA >= int64(sender.opts.MaxRetries) {
			return 1 // a > b
		}
		if serverErrorsB >= int64(sender.opts.MaxRetries) {
			return -1 // a < b
		}

		cooldownA := a.cooldown.Load().(time.Time)
		cooldownB := b.cooldown.Load().(time.Time)
		dlComp := cooldownA.Compare(cooldownB)
		if dlComp != 0 {
			return dlComp
		}

		return int(serverErrorsA - serverErrorsB)
	})
	return urlCopy
}

// bestURL calls sortURLS are returns the first one of the list that is not in
// the ignore list. The returned senderURL can be nil when no valid URL was
// found (i.e. there are valid urls but they are also in the ignore list).
// An error is only returned when all urls have exceeded maxRetries (abort).
func (sender *httpMsgSender) bestURL(ignore []*senderURL) (*senderURL, error) {
	urls := sender.sortURLS()

	ignoreMap := make(map[*senderURL]struct{}, len(ignore))
	for _, ig := range ignore {
		ignoreMap[ig] = struct{}{}
	}

	var first *senderURL
	for _, u := range urls {
		if _, ok := ignoreMap[u]; !ok {
			first = u
			break
		}
	}

	if first == nil {
		return nil, nil
	}

	if first.serverErrors.Load() >= int64(sender.opts.MaxRetries) {
		return nil, errors.New("urls exceeded server errors")
	}

	return first, nil
}

// sendErrorType explains why a request failed.
type senderErrorType int

const (
	// Usually errors that require aborting processing a wantlist.
	typeFatal senderErrorType = 0
	// Usually errors like 404, etc. which do not signal any issues
	// on the server.
	typeClient senderErrorType = 1
	// Usually errors that signal issues in the server.
	typeServer senderErrorType = 2
	// Usually errors due to cancelled contexts or timeouts.
	typeContext senderErrorType = 3
	// Errors due to 429 and 503 (retry later)
	typeRetryLater senderErrorType = 4
)

// senderError attatches type to a regular error. Implements the Error interface.
type senderError struct {
	Type senderErrorType
	Err  error
}

// Error returns the underlying error message.
func (err senderError) Error() string {
	return err.Err.Error()
}

// tryURL attemps to make a requests to the given URL using the given entry.
// Blocks, Haves etc. are recorded in the given response. cancellations are
// processed. tryURL returns an error so that it can be decided what to do next:
// i.e. retry, or move to next item in wantlist, or abort completely.
func (sender *httpMsgSender) tryURL(ctx context.Context, u *senderURL, entry bsmsg.Entry) (blocks.Block, *senderError) {
	// sleep whatever needed
	if dl := u.cooldown.Load().(time.Time); !dl.IsZero() {
		log.Debugf("sleeping on %s", u.URL)
		time.Sleep(time.Until(dl))
	}

	var method string

	switch {
	case entry.WantType == pb.Message_Wantlist_Block:
		method = "GET"
	case entry.WantType == pb.Message_Wantlist_Have:
		method = "HEAD"
	default:
		panic("unknown bitswap entry type")
	}

	// We do not abort ongoing requests.  This is known to cause "http2:
	// server sent GOAWAY and closed the connection" Losing a connection
	// is worse than downloading some extra bytes.  We do abort if the
	// context WAS already cancelled before making the request.
	if err := ctx.Err(); err != nil {
		log.Debugf("aborted before sending: %s %q", method, u.ParsedURL.URL)
		return nil, &senderError{
			Type: typeContext,
			Err:  err,
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), sender.opts.SendTimeout)
	defer cancel()
	req, err := buildRequest(ctx, u.ParsedURL, method, entry.Cid.String(), sender.ht.userAgent)
	if err != nil {
		return nil, &senderError{
			Type: typeFatal,
			Err:  err,
		}
	}

	log.Debugf("%d/%d %s %q", u.serverErrors.Load(), sender.opts.MaxRetries, method, req.URL)
	atomic.AddUint64(&sender.ht.stats.MessagesSent, 1)
	sender.ht.metrics.RequestsInFlight.Inc()
	resp, err := sender.ht.client.Do(req)
	if err != nil {
		err = fmt.Errorf("error making request to %q: %w", req.URL, err)
		sender.ht.metrics.RequestsFailure.Inc()
		sender.ht.metrics.RequestsInFlight.Dec()
		log.Debug(err)
		// Something prevents us from making a request.  We cannot
		// dial, or setup the connection perhaps.  This counts as
		// server error (unless context cancellation).  This means we
		// allow ourselves to hit this a maximum of MaxRetries per url.
		// and Disconnect() the peer when no urls work.
		serr := &senderError{
			Type: typeServer,
			Err:  ctx.Err(),
		}

		if ctx.Err() != nil {
			serr.Type = typeContext // cont. with next block.
		}

		return nil, serr
	}

	// Record request size
	var buf bytes.Buffer
	req.Write(&buf)
	sender.ht.metrics.RequestsSentBytes.Add(float64((&buf).Len()))

	// Handle responses
	limReader := &io.LimitedReader{
		R: resp.Body,
		N: sender.ht.maxBlockSize,
	}

	body, err := io.ReadAll(limReader)
	if err != nil {
		// treat this as server error
		err = fmt.Errorf("error reading body from %q: %w", req.URL, err)
		sender.ht.metrics.RequestsBodyFailure.Inc()
		sender.ht.metrics.RequestsInFlight.Dec()
		log.Debug(err)
		return nil, &senderError{
			Type: typeServer,
			Err:  err,
		}
	}

	// Calculate full response size with headers and everything.
	// So this is comparable to bitswap message response sizes.
	resp.Body = nil
	var respBuf bytes.Buffer
	resp.Write(&respBuf)
	respLen := (&respBuf).Len() + len(body)

	sender.ht.metrics.ResponseSizes.Observe(float64(respLen))
	sender.ht.metrics.RequestsInFlight.Dec()
	sender.ht.metrics.updateStatusCounter(resp.StatusCode)

	switch resp.StatusCode {
	// Valid responses signaling unavailability of the
	// content.
	case http.StatusNotFound,
		http.StatusGone,
		http.StatusForbidden,
		http.StatusUnavailableForLegalReasons:

		err := fmt.Errorf("%s %q -> %d: %q", req.Method, req.URL, resp.StatusCode, string(body))
		log.Error(err)
		// clear cooldowns since we got a proper reply
		if !u.cooldown.Load().(time.Time).IsZero() {
			sender.ht.cooldownTracker.remove(req.URL.Host)
			u.cooldown.Store(time.Time{})
		}
		// We should not fail more than maxRetries for each block.
		return nil, &senderError{
			Type: typeClient,
			Err:  err,
		}
	case http.StatusOK: // \(^°^)/
		// clear cooldowns since we got a proper reply
		if !u.cooldown.Load().(time.Time).IsZero() {
			sender.ht.cooldownTracker.remove(req.URL.Host)
			u.cooldown.Store(time.Time{})
		}
		log.Debugf("%s %q -> %d (%d bytes)", req.Method, req.URL, resp.StatusCode, len(body))

		if req.Method == "HEAD" {
			return nil, nil
		}
		// GET
		b, err := blocks.NewBlockWithCid(body, entry.Cid)
		if err != nil {
			log.Error("block received for cid %s does not match!", entry.Cid)
			// avoid entertaining servers that send us wrong data
			// too much.
			return nil, &senderError{
				Type: typeServer,
				Err:  err,
			}
		}
		atomic.AddUint64(&sender.ht.stats.MessagesRecvd, 1)
		return b, nil
	case http.StatusTooManyRequests,
		http.StatusServiceUnavailable:
		err := fmt.Errorf("%q -> %d: %q", req.URL, resp.StatusCode, string(body))
		log.Error(err)
		retryAfter := resp.Header.Get("Retry-After")
		cooldownUntil, ok := parseRetryAfter(retryAfter)
		if ok {
			sender.ht.cooldownTracker.setByDate(req.URL.Host, cooldownUntil)
			u.cooldown.Store(cooldownUntil)
		} else {
			sender.ht.cooldownTracker.setByDuration(req.URL.Host, sender.opts.SendErrorBackoff)
			u.cooldown.Store(time.Now().Add(sender.opts.SendErrorBackoff))
		}
		return nil, &senderError{
			Type: typeRetryLater,
			Err:  err,
		}

	// For any other code, we assume we must temporally
	// backoff from the URL per the options.
	// Tolerance for server errors per url is low. If after waiting etc.
	// it fails MaxRetries, we will fully disconnect.
	default:
		err := fmt.Errorf("%q -> %d: %q", req.URL, resp.StatusCode, string(body))
		log.Error(err)
		sender.ht.cooldownTracker.setByDuration(req.URL.Host, sender.opts.SendErrorBackoff)
		u.cooldown.Store(time.Now().Add(sender.opts.SendErrorBackoff))
		return nil, &senderError{
			Type: typeServer,
			Err:  err,
		}
	}
}

// SendMsg performs an http request for the wanted cids per the msg's
// Wantlist. It reads the response and records it in a reponse BitswapMessage
// which is forwarded to the receivers (in a separate goroutine).
func (sender *httpMsgSender) SendMsg(ctx context.Context, msg bsmsg.BitSwapMessage) error {
	// SendMsg gets called from MessageQueue and returning an error
	// results in a MessageQueue shutdown. Errors are only returned when
	// we are unable to obtain a single valid Block/Has response. When a
	// URL errors in a bad way (connection, 500s), we continue checking
	// with the next available one.

	// unless we have a wantlist, we bailout.
	wantlist := msg.Wantlist()
	if len(wantlist) == 0 {
		return nil
	}

	// Keep metrics of wantlists sent and how long it took
	sender.ht.metrics.WantlistsTotal.Inc()
	sender.ht.metrics.WantlistsItemsTotal.Add(float64(len(wantlist)))
	log.Debugf("sending wantlist: %s (%d items)", sender.peer, len(wantlist))
	now := time.Now()
	defer func() {
		sender.ht.metrics.WantlistsSeconds.Observe(float64(time.Since(now)) / float64(time.Second))
	}()

	// This is mostly a cosmetic action since the bitswap.Client is just
	// logging the errors.
	sendErrors := func(err error) {
		if err != nil {
			for _, recv := range sender.ht.receivers {
				recv.ReceiveError(err)
			}
		}
	}

	var err error

	// obtain contexts for all the entries in the wantlits.  This allows
	// us to react when cancels arrive to wantlists that we are going
	// through.  We use a Background context because requests will be
	// ongoing when we return and the parent context is cancelled.
	parentCtx := context.Background()
	entryCtxs := make([]context.Context, len(wantlist))
	entryCancels := make([]context.CancelFunc, len(wantlist))
	nop := func() {}
	for i, entry := range wantlist {
		if entry.Cancel {
			entryCtxs[i] = ctx
			entryCancels[i] = nop
		} else {
			entryCtxs[i], entryCancels[i] = sender.ht.requestTracker.requestContext(parentCtx, entry.Cid)
		}
	}

	var wgRequests sync.WaitGroup
	resultsCollector := make(chan httpResult, len(wantlist))

WANTLIST_LOOP:
	for i, entry := range wantlist {
		reqStart := time.Now()

		if entry.Cancel { // shortcut cancel entries.
			sender.ht.requestTracker.cancelRequest(entry.Cid)
			sender.ht.metrics.updateStatusCounter(0)
			// Do not observe request time for cancel requests as
			// they cost us nothing, so it is unfair to compare
			// against bsnet requests-time.
			// sender.ht.metrics.RequestTime.Observe(float64(time.Since(reqStart))
			// / float64(time.Second))
			continue
		}

		resultCh := make(chan httpResult, 1)

		reqInfo := httpRequestInfo{
			ctx:    entryCtxs[i],
			sender: sender,
			entry:  entry,
			result: resultCh,
		}

		select {
		case <-ctx.Done():
			// our context cancelled so we must abort.
			err = ctx.Err()
			break WANTLIST_LOOP
		case sender.ht.httpRequests <- reqInfo:
			wgRequests.Add(1)
			go func(start time.Time) {
				defer wgRequests.Done()
				result := <-resultCh
				close(resultCh)
				resultsCollector <- result
				sender.ht.metrics.RequestTime.Observe(float64(time.Since(start)) / float64(time.Second))
			}(reqStart)
		}
	}

	go func() {
		wgRequests.Wait()
		close(resultsCollector)
	}()

	// We are finished sending. Like bitswap/bsnet, we return.
	// Receiving results is async and we leave a goroutine taking care of
	// that.
	go func() {
		bsresp := bsmsg.New(false)

	COLLECT_RESULTS:
		for result := range resultsCollector {
			entry := result.entry

			if result.err == nil {
				sender.ht.connEvtMgr.OnMessage(sender.peer)

				if entry.WantType == pb.Message_Wantlist_Block {
					bsresp.AddBlock(result.block)
				} else {
					bsresp.AddHave(entry.Cid)
				}
				continue
			}

			// error handling
			switch result.err.Type {
			case typeFatal:
				log.Errorf("Disconnecting from %s: %w", sender.peer, result.err.Err)
				sender.ht.DisconnectFrom(ctx, sender.peer)
				err = result.err
				break COLLECT_RESULTS // cancel all requests
			case typeClient:
				if entry.SendDontHave {
					bsresp.AddDontHave(entry.Cid)
				}
			case typeContext: // ignore and move on
			case typeServer: // should not be returned as retried until fatal
			default:
				panic("unexpected returned error type")
			}
		}

		// We return a special "cancel" function that we need to call
		// explicitally. This cleans up our request-tracker.
		for _, cancel := range entryCancels {
			cancel()
		}
		sender.ht.requestTracker.cleanEmptyRequests(wantlist)
		sender.notifyReceivers(bsresp)
		// This just logs errors apparently.
		sendErrors(err)
	}()

	// We never return error once we started sending. Whatever happened,
	// we will be cooling down urls etc. but we don't need to disconnect
	// or report that "peer is down" for the moment, as we disconnect
	// manually on error.
	return nil
}

func (sender *httpMsgSender) notifyReceivers(bsresp bsmsg.BitSwapMessage) {
	lb := len(bsresp.Blocks())
	lh := len(bsresp.Haves())
	ldh := len(bsresp.DontHaves())
	if lb+lh+ldh == 0 { // nothing to do
		return
	}

	for i, recv := range sender.ht.receivers {
		log.Debugf("ReceiveMessage from %s#%d. Blocks: %d. Haves: %d", sender.peer, i, lb, lh)
		recv.ReceiveMessage(
			context.Background(), // todo: which context?
			sender.peer,
			bsresp,
		)
	}
}

// Reset resets the sender (currently noop)
func (sender *httpMsgSender) Reset() error {
	sender.closeOnce.Do(func() {
		close(sender.closing)
	})
	return nil
}

// SupportsHave can advertise whether we would like to support HAVE entries
// (they trigger HEAD requests).
func (sender *httpMsgSender) SupportsHave() bool {
	return sender.ht.supportsHave
}

// parseRetryAfter returns how many seconds the Retry-After header header
// wants us to wait.
func parseRetryAfter(ra string) (time.Time, bool) {
	if len(ra) == 0 {
		return time.Time{}, false
	}
	secs, err := strconv.ParseInt(ra, 10, 64)
	if err != nil {
		date, err := time.Parse(time.RFC1123, ra)
		if err != nil {
			return time.Time{}, false
		}
		return date, true
	}
	if secs <= 0 {
		return time.Time{}, false
	}

	return time.Now().Add(time.Duration(secs) * time.Second), true
}
