package peerpromtracker

import (
	"errors"
	"fmt"

	bsmsg "github.com/ipfs/boxo/bitswap/message"
	"github.com/ipfs/boxo/bitswap/tracer"
	"github.com/ipfs/go-log/v2"
	peer "github.com/libp2p/go-libp2p/core/peer"
	prom "github.com/prometheus/client_golang/prometheus"
)

var logger = log.Logger("bitswap/peer-prom-tracker")

var _ tracer.Tracer = (*PeerTracer)(nil)

type PeerTracer struct {
	bytesSent        *prom.CounterVec
	messagesSent     *prom.CounterVec
	bytesReceived    *prom.CounterVec
	messagesReceived *prom.CounterVec

	reg *prom.Registry
}

func NewPeerTracer(reg *prom.Registry) (*PeerTracer, error) {
	pt := &PeerTracer{
		reg: reg,
	}

	var good bool
	defer func() {
		if !good {
			pt.Close() // will unregister the sucessfully registered metrics
		}
	}()

	peerIdLabel := []string{"peer-id"}

	bytesSent := prom.NewCounterVec(prom.CounterOpts{
		Namespace: "ipfs",
		Subsystem: "bitswap/messages",
		Name:      "bytes-sent",
		Help:      "This records the number of bitswap messages bytes sent per peer.",
	}, peerIdLabel)
	if err := reg.Register(bytesSent); err != nil {
		return nil, fmt.Errorf("registering bytes-sent: %w", err)
	}
	pt.bytesSent = bytesSent

	messagesSent := prom.NewCounterVec(prom.CounterOpts{
		Namespace: "ipfs",
		Subsystem: "bitswap/messages",
		Name:      "messages-sent",
		Help:      "This records the number of bitswap messages sent per peer.",
	}, peerIdLabel)
	if err := reg.Register(messagesSent); err != nil {
		return nil, fmt.Errorf("registering messages-sent: %w", err)
	}
	pt.messagesSent = messagesSent

	bytesReceived := prom.NewCounterVec(prom.CounterOpts{
		Namespace: "ipfs",
		Subsystem: "bitswap/messages",
		Name:      "bytes-received",
		Help:      "This records the number of bitswap messages bytes received from each peer.",
	}, peerIdLabel)
	if err := reg.Register(bytesReceived); err != nil {
		return nil, fmt.Errorf("registering bytes-received: %w", err)
	}
	pt.bytesReceived = bytesReceived

	messagesReceived := prom.NewCounterVec(prom.CounterOpts{
		Namespace: "ipfs",
		Subsystem: "bitswap/messages",
		Name:      "messages-received",
		Help:      "This records the number of bitswap messages received from each peer.",
	}, peerIdLabel)
	if err := reg.Register(messagesReceived); err != nil {
		return nil, fmt.Errorf("registering messages-received: %w", err)
	}
	pt.messagesReceived = messagesReceived

	good = true
	return pt, nil
}

func (t *PeerTracer) MessageReceived(p peer.ID, msg bsmsg.BitSwapMessage) {
	strPeerid := p.Pretty()

	counter, err := t.messagesReceived.GetMetricWithLabelValues(strPeerid)
	if err == nil {
		logger.Debugf("failed to grab messages received label %s", err)
	} else {
		counter.Inc()
	}

	counter, err = t.bytesReceived.GetMetricWithLabelValues(strPeerid)
	if err == nil {
		logger.Debugf("failed to grab messages received label %s", err)
	} else {
		counter.Add(float64(msg.Size()))
	}
}

func (t *PeerTracer) MessageSent(p peer.ID, msg bsmsg.BitSwapMessage) {
	strPeerid := p.Pretty()

	counter, err := t.messagesSent.GetMetricWithLabelValues(strPeerid)
	if err == nil {
		logger.Debugf("failed to grab messages received label %s", err)
	} else {
		counter.Inc()
	}

	counter, err = t.bytesSent.GetMetricWithLabelValues(strPeerid)
	if err == nil {
		logger.Debugf("failed to grab messages received label %s", err)
	} else {
		counter.Add(float64(msg.Size()))
	}
}

func (t *PeerTracer) Close() error {
	if t.reg == nil {
		return errors.New("already closed")
	}

	// we have to check because this can be called by [NewPeerTracer] if an errors occurs.
	if t.bytesSent != nil {
		t.reg.Unregister(t.bytesSent)
	}
	if t.messagesSent != nil {
		t.reg.Unregister(t.messagesSent)
	}
	if t.bytesReceived != nil {
		t.reg.Unregister(t.bytesReceived)
	}
	if t.messagesReceived != nil {
		t.reg.Unregister(t.messagesReceived)
	}
	*t = PeerTracer{}

	return nil
}
