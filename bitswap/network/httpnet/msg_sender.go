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
	"strings"
	"sync"
	"sync/atomic"
	"time"

	bsmsg "github.com/ipfs/boxo/bitswap/message"
	pb "github.com/ipfs/boxo/bitswap/message/pb"
	"github.com/ipfs/boxo/bitswap/network"
	blocks "github.com/ipfs/go-block-format"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/peerstore"
)

// MessageSender option defaults.
const (
	// DefaultMaxRetries specifies how many requests to make to available
	// HTTP endpoints in case of failure.
	DefaultMaxRetries = 1
	// DefaultSendTimeout specifies sending each individual HTTP
	// request can take.
	DefaultSendTimeout = 5 * time.Second
	// SendErrorBackoff specifies how long to wait between retries to the
	// same endpoint after failure. It is overridden by Retry-After
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
	urlCopy := slices.Clone(sender.urls)

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

// tryURL makes one HTTP request for entry against u. It returns a
// senderError describing what the caller should do next: retry the same
// URL, move on to the next URL, skip the entry, or abort.
//
// Concurrent identical requests against the same HTTP endpoint share one
// round trip via the inflight tracker. See inflight.go for the rationale.
// Each caller still updates its own per-URL cooldown from the shared
// response.
func (sender *httpMsgSender) tryURL(ctx context.Context, u *senderURL, entry bsmsg.Entry) (blocks.Block, *senderError) {
	var method string

	switch entry.WantType {
	case pb.Message_Wantlist_Block:
		method = http.MethodGet
	case pb.Message_Wantlist_Have:
		method = http.MethodHead
	default:
		panic("unknown bitswap entry type")
	}

	if dl := u.cooldown.Load().(time.Time); !dl.IsZero() {
		err := fmt.Errorf("cooldown (%s): %s %q ", dl, method, u.URL)
		log.Debug(err)
		return nil, &senderError{Type: typeRetryLater, Err: err}
	}

	// Abort only if the parent context is already cancelled. Cancelling
	// an in-flight request triggers "http2: server sent GOAWAY and
	// closed the connection"; the lost connection costs more than the
	// extra bytes.
	if err := ctx.Err(); err != nil {
		log.Debugf("aborted before sending: %s %q", method, u.URL)
		return nil, &senderError{Type: typeContext, Err: err}
	}

	cidStr := entry.Cid.String()
	key := inflightKey(u.URL.Scheme, u.URL.Host, u.SNI, method, cidStr)
	res, shared := sender.ht.inflight.do(key, func() *inflightResult {
		return sender.executeRequest(u, method, cidStr)
	})
	if shared {
		log.Debugf("piggybacked on inflight request: %s %q", method, u.URL)
	}

	return sender.handleResponse(u, entry, method, res)
}

// executeRequest runs the HTTP round trip and records wire-level metrics.
// It runs once per coalesced request; waiters reuse the result via
// inflightTracker.do.
func (sender *httpMsgSender) executeRequest(u *senderURL, method, cidStr string) *inflightResult {
	// Detached context with the configured send timeout: the request must
	// outlive any single caller's context so that waiters always get a
	// usable result.
	ctx, cancel := context.WithTimeout(context.Background(), sender.opts.SendTimeout)
	defer cancel()

	req, err := buildRequest(ctx, u.ParsedURL, method, cidStr, sender.ht.userAgent)
	if err != nil {
		return &inflightResult{err: err, errType: typeFatal}
	}

	log.Debugf("%d/%d %s %q", u.serverErrors.Load(), sender.opts.MaxRetries, method, req.URL)
	atomic.AddUint64(&sender.ht.stats.MessagesSent, 1)
	sender.ht.metrics.RequestsInFlight.Inc()
	resp, err := sender.ht.client.Do(req)
	if err != nil {
		wrapped := fmt.Errorf("error making request to %q: %w", req.URL, err)
		sender.ht.metrics.RequestsFailure.Inc()
		sender.ht.metrics.RequestsInFlight.Dec()
		log.Debug(wrapped)
		errType := typeServer
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			errType = typeContext
		}
		return &inflightResult{
			err:        wrapped,
			errType:    errType,
			requestURL: req.URL.String(),
		}
	}
	defer resp.Body.Close()

	var buf bytes.Buffer
	req.Write(&buf)
	sender.ht.metrics.RequestsSentBytes.Add(float64(buf.Len()))

	limReader := &io.LimitedReader{R: resp.Body, N: sender.ht.maxBlockSize}
	body, err := io.ReadAll(limReader)
	if err != nil {
		wrapped := fmt.Errorf("error reading body from %q: %w", req.URL, err)
		sender.ht.metrics.RequestsBodyFailure.Inc()
		sender.ht.metrics.RequestsInFlight.Dec()
		log.Debug(wrapped)
		return &inflightResult{
			err:        wrapped,
			errType:    typeServer,
			statusCode: resp.StatusCode,
			requestURL: req.URL.String(),
		}
	}

	statusCode := resp.StatusCode
	// Some gateway implementations return 500 with an IPLD-shaped error
	// body when they cannot find the content. Treat those as 404.
	if statusCode != http.StatusOK && isKnownNotFoundError(string(body)) {
		statusCode = http.StatusNotFound
		log.Debugf("treating as 404: %q -> %d: %q", req.URL, resp.StatusCode, string(body))
	}

	// Record the full response size including headers so it stays
	// comparable to bitswap message response sizes.
	resp.Body = nil
	var respBuf bytes.Buffer
	resp.Write(&respBuf)
	respLen := respBuf.Len() + len(body)

	sender.ht.metrics.ResponseSizes.Observe(float64(respLen))
	sender.ht.metrics.RequestsInFlight.Dec()
	sender.ht.metrics.updateStatusCounter(req.Method, statusCode, u.URL.Hostname())

	return &inflightResult{
		statusCode: statusCode,
		body:       body,
		retryAfter: resp.Header.Get("Retry-After"),
		requestURL: req.URL.String(),
	}
}

// handleResponse classifies the shared HTTP result for the calling sender
// and updates its per-URL cooldown. Each waiter on a coalesced request
// runs this independently so that bitswap's per-peer accounting matches
// what it would see if every peer had made its own request.
func (sender *httpMsgSender) handleResponse(u *senderURL, entry bsmsg.Entry, method string, res *inflightResult) (blocks.Block, *senderError) {
	if res.err != nil {
		return nil, &senderError{Type: res.errType, Err: res.err}
	}

	statusCode := res.statusCode
	body := res.body

	switch statusCode {
	// Valid responses that signal the content is unavailable.
	case http.StatusNotFound,
		http.StatusGone,
		http.StatusForbidden,
		http.StatusUnavailableForLegalReasons,
		http.StatusMovedPermanently,
		http.StatusFound,
		http.StatusSeeOther,
		http.StatusTemporaryRedirect,
		http.StatusPermanentRedirect:

		err := fmt.Errorf("%s %q -> %d: %q", method, res.requestURL, statusCode, string(body))
		log.Debug(err)
		sender.clearCooldown(u)
		return nil, &senderError{Type: typeClient, Err: err}

	case http.StatusOK:
		sender.clearCooldown(u)
		log.Debugf("%s %q -> %d (%d bytes)", method, res.requestURL, statusCode, len(body))

		if method == http.MethodHead {
			return nil, nil
		}
		b, err := bsmsg.NewWantlistBlock(body, entry.Cid, entry.Cid.Prefix())
		if err != nil {
			log.Debugf("error making wantlist block for %s: %s", entry.Cid, err)
			// Server returned wrong data; treat as a server error so
			// repeat offenders trip the breaker.
			return nil, &senderError{Type: typeServer, Err: err}
		}
		atomic.AddUint64(&sender.ht.stats.MessagesRecvd, 1)
		return b, nil

	case http.StatusTooManyRequests,
		http.StatusServiceUnavailable,
		http.StatusBadGateway,
		http.StatusGatewayTimeout:
		// Per the path-gateway spec these codes SHOULD carry
		// Retry-After. They cover both fatal server issues and "block
		// not available right now", which overlap. We treat them as
		// non-fatal until they repeat:
		//   - MaxRetries defaults to 1, so we disconnect on the second
		//     consecutive server error.
		//   - First failure: cooldown using Retry-After if present,
		//     else the configured backoff.
		//   - Retry the same CID. If it fails again, count it as a
		//     server error and avoid retrying that URL.
		//   - When all URLs hit MaxRetries, abort.
		//
		// Wantlists are typically 1-3 items; tolerating many server
		// errors per cycle just means hammering broken servers. We
		// prefer that endpoints reserve these codes for genuine
		// server issues and return 404 for missing content.
		err := fmt.Errorf("%s %q -> %d: %q", method, res.requestURL, statusCode, string(body))
		log.Warn(err)
		sender.applyBackoff(u, res.retryAfter)
		return nil, &senderError{Type: typeRetryLater, Err: err}

	// For any other code we back off from the URL per the options.
	// Tolerance for server errors per URL is low: after MaxRetries we
	// disconnect from the peer.
	default:
		err := fmt.Errorf("%q -> %d: %q", res.requestURL, statusCode, string(body))
		log.Warn(err)
		sender.applyBackoff(u, "")
		return nil, &senderError{Type: typeServer, Err: err}
	}
}

// clearCooldown removes any active cooldown on u after a definitive
// response. Both the per-URL cooldown and the host-level cooldownTracker
// are cleared so that future senders for the same host start fresh.
func (sender *httpMsgSender) clearCooldown(u *senderURL) {
	if !u.cooldown.Load().(time.Time).IsZero() {
		sender.ht.cooldownTracker.remove(u.URL.Host)
		u.cooldown.Store(time.Time{})
	}
}

// applyBackoff sets a cooldown on u. If retryAfter parses as a date or
// seconds, that wins; otherwise we fall back to the configured
// SendErrorBackoff.
func (sender *httpMsgSender) applyBackoff(u *senderURL, retryAfter string) {
	if t, ok := parseRetryAfter(retryAfter); ok {
		sender.ht.cooldownTracker.setByDate(u.URL.Host, t)
		u.cooldown.Store(t)
		return
	}
	sender.ht.cooldownTracker.setByDuration(u.URL.Host, sender.opts.SendErrorBackoff)
	u.cooldown.Store(time.Now().Add(sender.opts.SendErrorBackoff))
}

// isKnownNotFoundError checks if the response body contains a known IPLD-specific
// error message that indicates the content was not found. Some gateway implementations
// return 500 (Internal Server Error) with these error messages instead of the more
// appropriate 404 (Not Found).
//
// Following IPFS's robustness principle of "be strict about the outcomes, be tolerant
// about the methods" (https://specs.ipfs.tech/architecture/principles/#robustness),
// we recognize these error patterns to enable interoperability with gateways that
// understand IPLD and bitswap semantics but return non-standard status codes.
//
// The strictness comes from verifying the error message proves the server understands
// IPLD requests (not just any 500 error). The tolerance allows us to work with more
// gateway implementations without requiring them to change their error codes first.
func isKnownNotFoundError(body string) bool {
	return strings.HasPrefix(body, "ipld: could not find node") ||
		strings.HasPrefix(body, "peer does not have") ||
		strings.HasPrefix(body, "getting pieces containing cid") ||
		strings.HasPrefix(body, "failed to load root node")
}

// SendMsg performs an http request for the wanted cids per the msg's
// Wantlist. It reads the response and records it in a response BitswapMessage
// which is forwarded to the receivers (in a separate goroutine).
func (sender *httpMsgSender) SendMsg(ctx context.Context, msg bsmsg.BitSwapMessage) error {
	// SendMsg gets called from MessageQueue and returning an error
	// results in a MessageQueue shutdown. Errors are only returned when
	// we are unable to obtain a single valid Block/Has response. When a
	// URL errors in a bad way (connection, 500s), we continue checking
	// with the next available one.

	// unless we have a wantlist, we bailout.
	wantlist := msg.Wantlist()
	lenWantlist := len(wantlist)
	if lenWantlist == 0 {
		return nil
	}

	// Keep metrics of wantlists sent and how long it took
	sender.ht.metrics.WantlistsTotal.Inc()
	sender.ht.metrics.WantlistsItemsTotal.Add(float64(lenWantlist))
	log.Debugf("sending wantlist: %s (%d items)", sender.peer, lenWantlist)
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

	resultsCollector := make(chan httpResult, len(wantlist))

	totalSent := 0

WANTLIST_LOOP:
	for i, entry := range wantlist {
		if entry.Cancel { // shortcut cancel entries.
			sender.ht.requestTracker.cancelRequest(entry.Cid)
			sender.ht.metrics.updateStatusCounter("CANCEL", 0, "")
			// Do not observe request time for cancel requests as
			// they cost us nothing, so it is unfair to compare
			// against bsnet requests-time.
			// sender.ht.metrics.RequestTime.Observe(float64(time.Since(reqStart))
			// / float64(time.Second))
			log.Debugf("wantlist msg %d/%d: %s %s cancel", i, lenWantlist-1, sender.peer, entry.Cid)
			continue
		}
		log.Debugf("wantlist msg %d/%d: %s %s %s DH:%t", i, lenWantlist-1, sender.peer, entry.Cid, entry.WantType, entry.SendDontHave)

		reqInfo := httpRequestInfo{
			ctx:       entryCtxs[i],
			sender:    sender,
			entry:     entry,
			result:    resultsCollector,
			startTime: time.Now(),
		}

		select {
		case <-ctx.Done():
			// our context cancelled so we must abort.
			err = ctx.Err()
			break WANTLIST_LOOP
		case sender.ht.httpRequests <- reqInfo:
			totalSent++
		}
	}

	if totalSent == 0 {
		return nil
	}

	// We are finished sending. Like bitswap/bsnet, we return.
	// Receiving results is async and we leave a goroutine taking care of
	// that.
	go func() {
		bsresp := bsmsg.New(false)
		totalResponses := 0
		totalClientErrors := 0

		for result := range resultsCollector {
			// Record total request time.
			sender.ht.metrics.RequestTime.Observe(float64(time.Since(result.info.startTime)) / float64(time.Second))

			entry := result.info.entry

			if result.err == nil {
				sender.ht.connEvtMgr.OnMessage(sender.peer)

				if entry.WantType == pb.Message_Wantlist_Block {
					bsresp.AddBlock(result.block)
				} else {
					bsresp.AddHave(entry.Cid)
				}
			} else {
				// error handling
				switch result.err.Type {
				case typeFatal:
					log.Warnf("disconnecting from %s: %s", sender.peer, result.err.Err)
					sender.ht.DisconnectFrom(ctx, sender.peer)
					err = result.err
					// continue processing responses as workers
					// might have done other requests in parallel
				case typeClient:
					totalClientErrors++
					if entry.SendDontHave {
						bsresp.AddDontHave(entry.Cid)
					}
				case typeContext: // ignore and move on
				case typeServer: // should not be returned as retried until fatal
				default:
					panic("unexpected returned error type")
				}
			}
			// Leave loop when we read all what we
			// expected.
			totalResponses++
			if totalResponses >= totalSent {
				close(resultsCollector)
				break
			}
		}

		// if totalClientErrors == 0, count is reset.
		if err := sender.ht.errorTracker.logErrors(sender.peer, totalClientErrors, sender.ht.maxDontHaveErrors); err != nil {
			log.Debugf("too many client errors. Disconnecting from %s", sender.peer)
			sender.ht.DisconnectFrom(ctx, sender.peer)
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
		log.Debugf("ReceiveMessage from %s#%d. Blocks: %d. Haves: %d. DontHaves: %d", sender.peer, i, lb, lh, ldh)
		recv.ReceiveMessage(
			context.Background(),
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

// SupportsHave indicates whether the peer answers to HEAD requests.
// This has been probed during Connect().
func (sender *httpMsgSender) SupportsHave() bool {
	return supportsHave(sender.ht.host.Peerstore(), sender.peer)
}

func supportsHave(pstore peerstore.Peerstore, p peer.ID) bool {
	var haveSupport bool
	v, err := pstore.Get(p, peerstoreSupportsHeadKey)
	if err != nil {
		haveSupport = false
	} else {
		b, ok := v.(bool)
		haveSupport = ok && b
	}
	log.Debugf("supportsHave: %s %t", p, haveSupport)
	return haveSupport
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
