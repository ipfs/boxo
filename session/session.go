// session implements a finality independent session system. It let session producers transparently integrate with any session consumer without prequired shared state.
// A session bundles a logical set of related requests, it let download clients more efficiently send requests in the network, the assumption is that the same nodes should host most of the data in the related requests.
// To create a new session use [ContextWithSession], compatible consumers like bitswap will automatically pick it up from the context.
package session

import (
	"context"
	"sync"
)

type contextKey struct{}

// ContextWithSession attach a session to the context, it may reuse an existing session if one already exists in the context.
// The context must be canceled in some way to ensure all sessions are cleaned up.
func ContextWithSession(ctx context.Context) context.Context {
	if _, ok := getRegistryFromContext(ctx); ok {
		return ctx
	}

	return context.WithValue(ctx, contextKey{}, &sessionRegistry{sesCtx: ctx})
}

// sessionRegistry is meant for session consumers.
// This type allows session consumers to inject their own session object within an existing context.
// This exists as it allows to satisfy theses two requirements:
// - Session producers must not need to hold some prexisting session factory object to inject into their contextes.
// - Session consumers must be able to not to maintain any state to track inflight sessions, instead they can store their session object in the sessionRegistry and rely on GC to cleanup.
// It is threadsafe.
type sessionRegistry struct {
	sesCtx   context.Context
	lk       sync.Mutex // protects sessions
	sessions map[any]any
}

func getRegistryFromContext(ctx context.Context) (_ *sessionRegistry, ok bool) {
	r := ctx.Value(contextKey{})
	if r == nil {
		return nil, false
	}

	rr, ok := r.(*sessionRegistry)
	return rr, ok
}

// Get attempt to fetch the session for the producer.
func Get[P comparable, S any](ctx context.Context, producer P) (session S, ok bool) {
	r, ok := getRegistryFromContext(ctx)
	if !ok {
		return
	}

	r.lk.Lock()
	s, ok := r.sessions[producer]
	r.lk.Unlock()
	if !ok {
		return
	}

	ss, ok := s.(S)
	return ss, ok
}

// GetOrCreate atomically fetch a session from the context, if none is present the create function is invoked and saved in the context.
// All contextes scoped to the first [ContextWithSession] will be able to reuse this session.
// create must not block.
// sessionContext is scoped to the session, it allows consumers to cleanup session scoped goroutines or other similar things.
func GetOrCreate[P comparable, S any](ctx context.Context, producer P, create func(sessionContext context.Context) (session S, ok bool)) (session S, ok bool) {
	r, ok := getRegistryFromContext(ctx)
	if !ok {
		return
	}

	r.lk.Lock()
	defer r.lk.Unlock()
	s, ok := r.sessions[producer]
	if ok {
		if ss, ok := s.(S); ok {
			return ss, true
		}
		// Overwrite with a new session if types don't match ?
	}

	ss, ok := create(r.sesCtx)
	if !ok {
		return
	}

	if r.sessions == nil {
		r.sessions = make(map[any]any, 1)
	}
	r.sessions[producer] = ss

	return ss, true
}
