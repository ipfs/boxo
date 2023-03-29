package common

import (
	"net/http"

	"github.com/ipfs/boxo/gateway"
)

func NewBlocksHandler(gw gateway.IPFSBackend, port int) http.Handler {
	headers := map[string][]string{}
	gateway.AddAccessControlHeaders(headers)

	conf := gateway.Config{
		Headers: headers,
	}

	mux := http.NewServeMux()
	gwHandler := gateway.NewHandler(conf, gw)
	mux.Handle("/ipfs/", gwHandler)
	mux.Handle("/ipns/", gwHandler)
	return mux
}
