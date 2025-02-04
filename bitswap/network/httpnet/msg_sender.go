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

	// how many errors we tolerate before we give up making requests for a
	// block, when the error is client-side: 404, 401 etc. It makes no
	// sense to retry such requests, therefore 1.
	defaultMaxClientErrors = 1
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
	cooldown         time.Time
	clientErrors     int
	serverErrors     int
	retryLaterErrors int
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
//   - same, or no cooldown, are sorted by number of client errors
//   - if same number of client errors, sorted by number
//     of server errors.
func (sender *httpMsgSender) sortURLS() {
	slices.SortFunc(sender.urls, func(a, b *senderURL) int {
		// urls without exhausted retries come first
		if a.clientErrors >= defaultMaxClientErrors || a.serverErrors >= sender.opts.MaxRetries {
			return 1 // a > b
		}
		if b.clientErrors >= defaultMaxClientErrors || b.serverErrors >= sender.opts.MaxRetries {
			return -1 // a < b
		}

		dlComp := a.cooldown.Compare(b.cooldown)
		if dlComp != 0 {
			return dlComp
		}

		if a.clientErrors == b.clientErrors {
			return a.serverErrors - b.serverErrors
		}

		return a.clientErrors - b.clientErrors
	})
}

// bestURL calls sortURLS are returns the first one of the list.
// If the url has exhausted client or server retries, it errors.
func (sender *httpMsgSender) bestURL() (*senderURL, error) {
	sender.sortURLS()
	first := sender.urls[0]

	// A nil URL and no error signals that we don't need to abort, but
	// cannot keep retrying the same. client errors will be reset for next
	// block in the wantlist.
	if first.clientErrors >= defaultMaxClientErrors {
		return nil, nil
	}

	if first.serverErrors >= sender.opts.MaxRetries {
		return nil, errors.New("urls exceeded server errors")
	}

	return first, nil
}

// resetClientErrors sets all clientErrors on urls to 0.
// It is called every time we move to the next item in a wantlist.
func (sender *httpMsgSender) resetClientErrors() {
	for i := range sender.urls {
		sender.urls[i].clientErrors = 0
	}
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
func (err *senderError) Error() string {
	return err.Err.Error()
}

// tryURL attemps to make a requests to the given URL using the given entry.
// Blocks, Haves etc. are recorded in the given response. cancellations are
// processed. tryURL returns an error so that it can be decided what to do next:
// i.e. retry, or move to next item in wantlist, or abort completely.
func (sender *httpMsgSender) tryURL(ctx context.Context, u *senderURL, entry bsmsg.Entry, bsresp bsmsg.BitSwapMessage) *senderError {
	// sleep whatever needed
	if dl := u.cooldown; !dl.IsZero() {
		time.Sleep(time.Until(dl))
	}

	var method string

	switch {
	case entry.Cancel:
		// log.Debugf("received cancel entry for %s: %s", u.url, entry.Cid)
		sender.ht.requestTracker.cancelRequest(entry.Cid)
		return nil // cont with next block

	case entry.WantType == pb.Message_Wantlist_Block:
		method = "GET"
	case entry.WantType == pb.Message_Wantlist_Have:
		method = "HEAD"
	default:
		panic("unknown bitswap entry type")
	}

	ctx, cancel := context.WithTimeout(ctx, sender.opts.SendTimeout)
	defer cancel()
	req, err := buildRequest(ctx, u.ParsedURL, method, entry.Cid.String(), sender.ht.userAgent)
	if err != nil {
		return &senderError{
			Type: typeFatal,
			Err:  err,
		}
	}

	log.Debugf("%d/%d %d/%d %s %q", u.clientErrors, defaultMaxClientErrors, u.serverErrors, sender.opts.MaxRetries, method, req.URL)
	atomic.AddUint64(&sender.ht.stats.MessagesSent, 1)
	reqStart := time.Now()
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

		return serr
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
		return &senderError{
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
	sender.ht.metrics.RequestTime.Observe(float64(time.Since(reqStart)) / float64(time.Second))
	sender.ht.metrics.updateStatusCounter(resp.StatusCode)

	sender.ht.connEvtMgr.OnMessage(sender.peer)

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
		if !u.cooldown.IsZero() {
			sender.ht.cooldownTracker.remove(req.URL.Host)
		}
		if entry.SendDontHave {
			bsresp.AddDontHave(entry.Cid)
		}
		// We should not fail more than maxRetries for each block.
		return &senderError{
			Type: typeClient,
			Err:  err,
		}
	case http.StatusOK: // \(^°^)/
		// clear cooldowns since we got a proper reply
		if !u.cooldown.IsZero() {
			sender.ht.cooldownTracker.remove(req.URL.Host)
		}
		log.Debugf("%q -> %d (%d bytes)", req.URL, resp.StatusCode, len(body))

		if req.Method == "HEAD" {
			bsresp.AddHave(entry.Cid)
			return nil
		}
		// GET
		b, err := blocks.NewBlockWithCid(body, entry.Cid)
		if err != nil {
			log.Error("block received for cid %s does not match!", entry.Cid)
			// avoid entertaining servers that send us wrong data
			// too much.
			return &senderError{
				Type: typeServer,
				Err:  err,
			}
		}
		bsresp.AddBlock(b)
		atomic.AddUint64(&sender.ht.stats.MessagesRecvd, 1)
		return nil
	case http.StatusTooManyRequests,
		http.StatusServiceUnavailable:
		err := fmt.Errorf("%q -> %d: %q", req.URL, resp.StatusCode, string(body))
		log.Error(err)
		retryAfter := resp.Header.Get("Retry-After")
		cooldownUntil, ok := parseRetryAfter(retryAfter)
		if ok {
			sender.ht.cooldownTracker.setByDate(req.URL.Host, cooldownUntil)
		} else {
			sender.ht.cooldownTracker.setByDuration(req.URL.Host, sender.opts.SendErrorBackoff)
		}
		return &senderError{
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
		return &senderError{
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

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

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

	go func() {
		select {
		case <-sender.closing:
			cancel()
			return
		case <-ctx.Done():
			return
		}
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

	bsresp := bsmsg.New(false)
	var err error

	// obtain contexts for all the entries in the wantlits.
	// This allows us to react when cancels arrive to wantlists
	// that we are going through.
	entryCtxs := make([]context.Context, len(wantlist))
	entryCancels := make([]context.CancelFunc, len(wantlist))
	nop := func() {}
	for i, entry := range wantlist {
		if entry.Cancel {
			entryCtxs[i] = ctx
			entryCancels[i] = nop
		} else {
			entryCtxs[i], entryCancels[i] = sender.ht.requestTracker.requestContext(ctx, entry.Cid)
		}
	}

WANTLIST_LOOP:
	for i, entry := range wantlist {
	URL_LOOP:
		for {
			// our context cancelled so we must abort.
			if ctx.Err() != nil {
				err = ctx.Err()
				break WANTLIST_LOOP
			}

			u, err := sender.bestURL()
			// must abort
			// possibly server errors maxed
			// we disconnect this peer
			// and avoid it for the time being.
			if err != nil {
				// notify new blocks before disconnecting. As
				// disconnecting may trigger cleanups or
				// something.
				sender.notifyReceivers(bsresp)
				sender.ht.DisconnectFrom(ctx, sender.peer)
				return nil
			}

			// we move to next block, no good urls
			// possibly client-errors maxed.
			if u == nil {
				break
			}

			serr := sender.tryURL(entryCtxs[i], u, entry, bsresp)
			if serr == nil {
				break // cont. with next block
			}

			switch serr.Type {
			case typeFatal:
				log.Error(err)
				break WANTLIST_LOOP // full abort
			case typeClient:
				u.clientErrors++
				continue // retry until bestURL forces moving to next block
			case typeServer:
				u.serverErrors++
				continue // retry until bestURL forces moving to next block
			case typeContext:
				// context error probably results from
				// cancellations, in which case we move to
				// next block.
				break URL_LOOP // cont. with next block
			case typeRetryLater:
				// This error signals that the server
				// specifically indicated that it is
				// overloaded.  We are going to retry 3 times
				// and then increase the serverError count.
				// Retries happen following the cooldown
				// period. When multiple urls, retries may
				// happen on a different url.
				u.retryLaterErrors++
				if u.retryLaterErrors%3 == 0 {
					u.retryLaterErrors = 0
					u.serverErrors++
				}
				continue

			default:
				panic("unknown sender error type")
			}
		}
		// client errors cleared for every new block
		sender.resetClientErrors()
	}

	// clean up request tracker
	for _, cancel := range entryCancels {
		cancel()
	}
	sender.ht.requestTracker.cleanEmptyRequests(wantlist)

	go sender.notifyReceivers(bsresp)
	// This just logs errors apparently.
	sendErrors(err)

	// We never return error. Whatever happened, we will be cooling down
	// urls etc. but we don't need to disconnect or report that "peer is
	// down" for the moment, as we disconnect manually on error.
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
