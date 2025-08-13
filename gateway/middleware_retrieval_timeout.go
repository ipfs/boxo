package gateway

import (
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"
)

const truncationMessage = "\n\n[Gateway Error: Response truncated - unable to retrieve remaining data within timeout period]"

// withRetrievalTimeout wraps an http.Handler with a timeout that enforces:
// 1. Maximum time to first byte (initial retrieval)
// 2. Maximum time between subsequent non-empty writes (timeout resets on each write)
// If no data is written within the specified duration, the connection is
// terminated with a 504 Gateway Timeout.
//
// Parameters:
//   - handler: The HTTP handler to wrap with retrieval timeout
//   - timeout: Maximum duration between writes (0 disables timeout)
//   - c: Optional configuration for controlling error page rendering (can be nil)
func withRetrievalTimeout(handler http.Handler, timeout time.Duration, c *Config, metrics *middlewareMetrics) http.Handler {
	if timeout <= 0 {
		return handler
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Create channels for coordination
		handlerDone := make(chan struct{})
		timeoutChan := make(chan struct{})

		// Create a custom response writer that tracks writes
		tw := &timeoutWriter{
			ResponseWriter: w,
			timeout:        timeout,
			timer:          time.NewTimer(timeout),
			done:           handlerDone,
			timeoutSignal:  timeoutChan,
			request:        r,
			config:         c,
		}

		// Start the timeout monitoring goroutine
		go func() {
			for {
				select {
				case <-tw.timer.C:
					tw.mu.Lock()
					if !tw.timedOut && !tw.handlerComplete {
						tw.timedOut = true
						log.Debugw("retrieval timeout triggered",
							"path", r.URL.Path,
							"headerSent", tw.wroteHeader,
							"bytesWritten", tw.bytesWritten)

						if !tw.wroteHeader {
							// Headers not sent yet, we can send 504
							metrics.recordTimeout(http.StatusGatewayTimeout, false)
							message := "Unable to retrieve content within timeout period"
							log.Debugw("sending 504 gateway timeout",
								"path", tw.request.URL.Path,
								"method", tw.request.Method,
								"remoteAddr", tw.request.RemoteAddr)
							writeErrorResponse(tw.ResponseWriter, tw.request, tw.config, http.StatusGatewayTimeout, message)
						} else {
							// Headers already sent, response is being truncated
							statusCode := tw.headerCode
							if statusCode == 0 {
								statusCode = http.StatusOK
							}
							metrics.recordTimeout(statusCode, true)

							// Try to write truncation message (best effort)
							tw.ResponseWriter.Write([]byte(truncationMessage))

							// Try to hijack and force connection reset
							if hijacker, ok := tw.ResponseWriter.(http.Hijacker); ok {
								conn, _, err := hijacker.Hijack()
								if err == nil {
									tw.hijacked = true
									// Force TCP RST instead of graceful close
									if tcpConn, ok := conn.(*net.TCPConn); ok {
										tcpConn.SetLinger(0)
									}
									conn.Close()
									log.Debugw("response truncated due to timeout",
										"path", tw.request.URL.Path,
										"method", tw.request.Method,
										"remoteAddr", tw.request.RemoteAddr,
										"status", statusCode,
										"bytesWritten", tw.bytesWritten)
								} else {
									log.Warnw("failed to hijack connection for timeout reset",
										"path", tw.request.URL.Path,
										"error", err)
								}
							}
						}

						// Signal timeout to potentially waiting handler
						close(timeoutChan)
					}
					tw.mu.Unlock()
					return

				case <-handlerDone:
					// Handler completed, stop timer and exit
					tw.timer.Stop()
					return
				}
			}
		}()

		// Run handler in a goroutine so we can interrupt it on timeout
		go func() {
			defer func() {
				tw.mu.Lock()
				tw.handlerComplete = true
				tw.mu.Unlock()
				close(handlerDone)
			}()
			handler.ServeHTTP(tw, r)
		}()

		// Wait for either handler completion or timeout
		select {
		case <-handlerDone:
			// Handler completed normally
		case <-timeoutChan:
			// Timeout occurred, response already sent by timeout goroutine
		}
	})
}

// timeoutWriter wraps http.ResponseWriter to track write activity
type timeoutWriter struct {
	http.ResponseWriter
	timeout         time.Duration
	timer           *time.Timer
	mu              sync.Mutex
	timedOut        bool
	wroteHeader     bool
	headerCode      int
	done            chan struct{}
	timeoutSignal   chan struct{}
	hijacked        bool
	handlerComplete bool
	bytesWritten    int64
	request         *http.Request
	config          *Config
}

func (tw *timeoutWriter) Write(p []byte) (int, error) {
	tw.mu.Lock()
	defer tw.mu.Unlock()

	if tw.timedOut {
		return 0, fmt.Errorf("response write timeout")
	}

	// Reset timer on non-empty write
	if len(p) > 0 {
		tw.timer.Reset(tw.timeout)
		tw.bytesWritten += int64(len(p))
	}

	n, err := tw.ResponseWriter.Write(p)
	if !tw.wroteHeader {
		tw.wroteHeader = true
		// If WriteHeader wasn't called explicitly, it's 200
		if tw.headerCode == 0 {
			tw.headerCode = http.StatusOK
		}
	}
	return n, err
}

func (tw *timeoutWriter) WriteHeader(statusCode int) {
	tw.mu.Lock()
	defer tw.mu.Unlock()

	if tw.timedOut || tw.wroteHeader {
		return
	}

	tw.wroteHeader = true
	tw.headerCode = statusCode
	tw.ResponseWriter.WriteHeader(statusCode)
}

// Flush implements http.Flusher
func (tw *timeoutWriter) Flush() {
	if f, ok := tw.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}
