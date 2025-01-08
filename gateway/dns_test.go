package gateway

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"

	"github.com/miekg/dns"
	"github.com/stretchr/testify/require"
)

func TestAddNewDNSResolver(t *testing.T) {
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer l.Close()

	dnslinkName := "dnslink-test.foobar"
	dnslinkValue := "dnslink=/ipfs/bafkqaaa"

	go func() {
		_ = http.Serve(l, http.HandlerFunc(dnslinkServerHandlerFunc(t, dnslinkName, dnslinkValue)))
	}()

	listenAddr := l.Addr().(*net.TCPAddr)
	r, err := NewDNSResolver(map[string]string{
		"foobar.": fmt.Sprintf("http://%s:%d", listenAddr.IP, listenAddr.Port),
	})
	require.NoError(t, err)

	res, err := r.LookupTXT(ctx, fmt.Sprintf("_dnslink.%s.", dnslinkName))
	require.NoError(t, err)
	require.Len(t, res, 1)
	require.Equal(t, dnslinkValue, res[0])
}

func TestOverrideDNSDefaults(t *testing.T) {
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer l.Close()

	dnslinkName := "dnslink-test.eth"
	dnslinkValue := "dnslink=/ipfs/bafkqaaa"

	go func() {
		_ = http.Serve(l, http.HandlerFunc(dnslinkServerHandlerFunc(t, dnslinkName, dnslinkValue)))
	}()

	listenAddr := l.Addr().(*net.TCPAddr)
	r, err := NewDNSResolver(map[string]string{
		"eth.": fmt.Sprintf("http://%s:%d", listenAddr.IP, listenAddr.Port),
	})
	require.NoError(t, err)

	res, err := r.LookupTXT(ctx, fmt.Sprintf("_dnslink.%s.", dnslinkName))
	require.NoError(t, err)
	require.Len(t, res, 1)
	require.Equal(t, dnslinkValue, res[0])
}

func dnslinkServerHandlerFunc(t *testing.T, dnslinkName string, txtResponse string) func(w http.ResponseWriter, req *http.Request) {
	return func(w http.ResponseWriter, req *http.Request) {
		b, err := io.ReadAll(req.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		msg := &dns.Msg{}
		err = msg.Unpack(b)
		require.NoError(t, err)
		var answers []dns.RR
		for _, q := range msg.Question {
			if strings.ToLower(q.Name) != fmt.Sprintf("_dnslink.%s.", dnslinkName) || q.Qtype != dns.TypeTXT {
				answers = append(answers, &dns.RR_Header{
					Name:   q.Name,
					Rrtype: dns.RcodeServerFailure,
					Class:  q.Qclass,
					Ttl:    0,
				})
			} else {
				answers = append(answers, &dns.TXT{
					Hdr: dns.RR_Header{
						Name:   q.Name,
						Rrtype: dns.TypeTXT,
						Class:  dns.ClassINET,
						Ttl:    uint32(3600),
					},
					Txt: []string{txtResponse},
				})
			}
		}
		var m dns.Msg
		m.SetReply(msg)
		m.Authoritative = true
		m.Answer = answers
		encoded, err := m.Pack()
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/dns-message")
		w.WriteHeader(http.StatusOK)
		if _, err := w.Write(encoded); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}
}
