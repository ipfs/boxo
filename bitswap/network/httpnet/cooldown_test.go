package httpnet

import (
	"testing"
	"time"
)

func TestCooldownTracker_OnCooldown(t *testing.T) {
	ct := newCooldownTracker(time.Minute)
	defer ct.stopCleaner()

	if ct.onCooldown("missing") {
		t.Errorf("unset host reported on cooldown")
	}

	ct.setByDuration("active", 10*time.Second)
	if !ct.onCooldown("active") {
		t.Errorf("active deadline not reported on cooldown")
	}

	// A deadline in the past must not be reported as on cooldown.
	ct.setByDate("expired", time.Now().Add(-1*time.Second))
	if ct.onCooldown("expired") {
		t.Errorf("expired deadline reported on cooldown")
	}

	ct.remove("active")
	if ct.onCooldown("active") {
		t.Errorf("removed host still reported on cooldown")
	}
}

func TestCooldownTracker_MaxBackoffCap(t *testing.T) {
	ct := newCooldownTracker(2 * time.Second)
	defer ct.stopCleaner()

	// Asking for 1 hour must be clamped to maxBackoff.
	ct.setByDuration("clamp", time.Hour)

	ct.urlsLock.RLock()
	dl := ct.urls["clamp"]
	ct.urlsLock.RUnlock()
	if got := time.Until(dl); got > 3*time.Second {
		t.Errorf("setByDuration not clamped: %s remaining, want <=2s+slack", got)
	}
}
