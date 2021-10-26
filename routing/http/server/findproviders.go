package server

import (
	"bytes"
	"encoding/json"
	"net/http"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-delegated-routing/client"
	"github.com/ipfs/go-delegated-routing/parser"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/multiformats/go-multiaddr"
)

type FindProvidersAsyncFunc func(cid.Cid, chan<- client.FindProvidersAsyncResult) error

func FindProvidersAsyncHandler(f FindProvidersAsyncFunc) http.HandlerFunc {
	return func(writer http.ResponseWriter, request *http.Request) {
		msg := request.URL.Query().Get("q")
		dec := json.NewDecoder(bytes.NewBufferString(msg))
		env := parser.Envelope{Payload: &parser.GetP2PProvideRequest{}}
		err := dec.Decode(&env)
		if err != nil {
			writer.WriteHeader(400)
			return
		}
		switch env.Tag {
		case parser.MethodGetP2PProvide:
			req, ok := env.Payload.(*parser.GetP2PProvideRequest)
			if !ok {
				writer.WriteHeader(400)
				return
			}
			// extract key and return it in the form of a cid
			parsedCid, err := ParseGetP2PProvideRequest(req)
			if err != nil {
				writer.WriteHeader(400)
				return
			}
			// proxy to func
			ch := make(chan client.FindProvidersAsyncResult)
			if err = f(parsedCid, ch); err != nil {
				writer.WriteHeader(500)
				return
			}
			for x := range ch {
				if x.Err != nil {
					continue
				}
				resp := GenerateGetP2PProvideResponse(x.AddrInfo)
				env := &parser.Envelope{
					Tag:     parser.MethodGetP2PProvide,
					Payload: resp,
				}
				enc, err := json.Marshal(env)
				if err != nil {
					continue
				}
				writer.Write(enc)
			}
		default:
			writer.WriteHeader(404)
		}
	}
}

// ParseGetP2PProvideRequest parses a GetP2PProvideRequest and returns the included bytes key in the form of a cid.
func ParseGetP2PProvideRequest(req *parser.GetP2PProvideRequest) (cid.Cid, error) {
	mhBytes, err := parser.FromDJSpecialBytes(req.Key)
	if err != nil {
		return cid.Undef, err
	}
	parsedCid := cid.NewCidV1(cid.Raw, mhBytes)
	if err != nil {
		return cid.Undef, err
	}
	return parsedCid, nil
}

func GenerateGetP2PProvideResponse(infos []peer.AddrInfo) *parser.GetP2PProvideResponse {
	resp := &parser.GetP2PProvideResponse{}
	for _, info := range infos {
		for _, addr := range info.Addrs {
			peerAddr := addr.Encapsulate(multiaddr.StringCast("/p2p/" + info.ID.String()))
			resp.Peers = append(resp.Peers, parser.ToDJSpecialBytes(peerAddr.Bytes()))
		}
	}
	return resp
}
