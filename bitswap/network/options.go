package network

import "github.com/libp2p/go-libp2p-core/protocol"

type NetOpt func(*Settings)

type Settings struct {
	ProtocolPrefix protocol.ID
}

func Prefix(prefix protocol.ID) NetOpt {
	return func(settings *Settings) {
		settings.ProtocolPrefix = prefix
	}
}
