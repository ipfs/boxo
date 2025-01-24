package httpnet

import (
	"net/url"
	"sync"
	"time"
)

type cooldownTracker struct {
	maxBackoff time.Duration

	urlsLock sync.RWMutex
	urls     map[string]time.Time
}

func newCooldownTracker(maxBackoff time.Duration) *cooldownTracker {
	return &cooldownTracker{
		maxBackoff: maxBackoff,
		urls:       make(map[string]time.Time),
	}
}

func (ct *cooldownTracker) setByDate(host string, t time.Time) {
	latestDate := time.Now().Add(ct.maxBackoff)
	if t.After(latestDate) {
		t = latestDate
	}
	ct.urlsLock.Lock()
	ct.urls[host] = t
	ct.urlsLock.Unlock()
}

func (ct *cooldownTracker) setByDuration(host string, d time.Duration) {
	if d > ct.maxBackoff {
		d = ct.maxBackoff
	}
	ct.urlsLock.Lock()
	ct.urls[host] = time.Now().Add(d)
	ct.urlsLock.Unlock()
}

func (ct *cooldownTracker) remove(host string) {
	ct.urlsLock.Lock()
	delete(ct.urls, host)
	ct.urlsLock.Unlock()
}

func (ct *cooldownTracker) fillSenderURLs(urls []*url.URL) []*senderURL {
	now := time.Now()
	surls := make([]*senderURL, len(urls))
	ct.urlsLock.RLock()
	{

		for i, u := range urls {
			var cooldown time.Time
			dl, ok := ct.urls[u.Host]
			if ok {
				if now.Before(dl) {
					cooldown = dl
				} else {
					// TODO: remove if we add a cleaning
					// thread.
					delete(ct.urls, u.Host)
				}
			}
			surls[i] = &senderURL{
				url:      u,
				cooldown: cooldown,
			}
		}
	}
	ct.urlsLock.RUnlock()
	return surls
}
