package httpnet

import (
	"testing"
	"time"
)

func TestCooldownInCooldown(t *testing.T) {
	ct := NewCooldownTracker()

	host := "gateway.example.net:443"

	if _, cooling := ct.inCooldown(host); cooling {
		t.Fatal("unset host should not be cooling")
	}

	ct.setByDuration(host, time.Second)
	dl, cooling := ct.inCooldown(host)
	if !cooling {
		t.Fatal("host should be cooling")
	}
	if until := time.Until(dl); until <= 0 || until > time.Second {
		t.Errorf("unexpected deadline: %s", dl)
	}

	// setByDuration caps at maxBackoff.
	ct.setByDuration(host, time.Hour)
	dl, _ = ct.inCooldown(host)
	if time.Until(dl) > DefaultMaxBackoff {
		t.Errorf("cooldown not capped at maxBackoff: %s", dl)
	}

	// setByDate caps at maxBackoff too.
	ct.setByDate(host, time.Now().Add(time.Hour))
	dl, _ = ct.inCooldown(host)
	if time.Until(dl) > DefaultMaxBackoff {
		t.Errorf("cooldown not capped at maxBackoff: %s", dl)
	}

	// A past date is not an active cooldown.
	ct.setByDate(host, time.Now().Add(-time.Second))
	if _, cooling := ct.inCooldown(host); cooling {
		t.Error("past deadline should not be cooling")
	}

	ct.setByDuration(host, time.Second)
	ct.remove(host)
	if _, cooling := ct.inCooldown(host); cooling {
		t.Error("removed host should not be cooling")
	}
}

func TestCooldownSweepOnWrite(t *testing.T) {
	ct := NewCooldownTracker()

	expired := "expired.example.net:443"
	ct.setByDate(expired, time.Now().Add(-time.Second))

	// Sweeps are rate-limited; age the last one to force the next write to
	// sweep.
	ct.urlsLock.Lock()
	ct.lastSweep = time.Now().Add(-2 * cooldownSweepInterval)
	ct.urlsLock.Unlock()

	ct.setByDuration("other.example.net:443", time.Second)

	ct.urlsLock.RLock()
	_, stillThere := ct.urls[expired]
	entries := len(ct.urls)
	ct.urlsLock.RUnlock()

	if stillThere {
		t.Error("expired entry should have been swept on write")
	}
	if entries != 1 {
		t.Errorf("only the live entry should remain, have %d", entries)
	}
}
