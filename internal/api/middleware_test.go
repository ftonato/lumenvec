package api

import (
	"bytes"
	"log"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"
)

func newMiddlewareServer(t *testing.T, apiKey string) *Server {
	t.Helper()
	base := t.TempDir()
	return NewServerWithOptions(ServerOptions{
		Port:         ":0",
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,
		APIKey:       apiKey,
		RateLimitRPS: 1,
		SnapshotPath: filepath.Join(base, "snapshot.json"),
		WALPath:      filepath.Join(base, "wal.log"),
	})
}

func TestGetClientIP(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("X-Forwarded-For", "1.2.3.4, 5.6.7.8")
	if got := getClientIP(req); got != "1.2.3.4" {
		t.Fatalf("getClientIP() = %q", got)
	}
}

func TestAuthMiddleware(t *testing.T) {
	server := newMiddlewareServer(t, "secret")
	nextCalled := false
	handler := server.authMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		nextCalled = true
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodPost, "/vectors", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", rec.Code)
	}

	req = httptest.NewRequest(http.MethodPost, "/vectors", nil)
	req.Header.Set("Authorization", "Bearer secret")
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK || !nextCalled {
		t.Fatal("expected authenticated request to pass")
	}
}

func TestRateLimitAndMetricsMiddleware(t *testing.T) {
	server := newMiddlewareServer(t, "")
	handler := server.rateLimitMiddleware(server.metricsMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusCreated)
	})))

	req1 := httptest.NewRequest(http.MethodGet, "/vectors", nil)
	req1.RemoteAddr = "127.0.0.1:1234"
	rec1 := httptest.NewRecorder()
	handler.ServeHTTP(rec1, req1)
	if rec1.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d", rec1.Code)
	}

	req2 := httptest.NewRequest(http.MethodGet, "/vectors", nil)
	req2.RemoteAddr = "127.0.0.1:1234"
	rec2 := httptest.NewRecorder()
	handler.ServeHTTP(rec2, req2)
	if rec2.Code != http.StatusTooManyRequests {
		t.Fatalf("expected 429, got %d", rec2.Code)
	}
}

func TestMetricsRegistryAndStatusRecorder(t *testing.T) {
	rec := httptest.NewRecorder()
	sr := &statusRecorder{ResponseWriter: rec, status: http.StatusOK}
	sr.WriteHeader(http.StatusAccepted)
	if sr.status != http.StatusAccepted {
		t.Fatal("expected updated status")
	}
	total, duration, registry := newMetricsRegistry()
	if total == nil || duration == nil || registry == nil {
		t.Fatal("expected metrics registry")
	}
}

func TestMiddlewarePublicPathsAndAccessLog(t *testing.T) {
	server := newMiddlewareServer(t, "secret")

	auth := server.authMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	auth.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodPost, "/vectors", nil)
	req.Header.Set("X-API-Key", "secret")
	auth.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	rate := server.rateLimitMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rate.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	var buf bytes.Buffer
	oldWriter := log.Writer()
	log.SetOutput(&buf)
	t.Cleanup(func() { log.SetOutput(oldWriter) })

	access := server.accessLogMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusAccepted)
	}))
	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/x", nil)
	access.ServeHTTP(rec, req)
	if rec.Code != http.StatusAccepted || buf.Len() == 0 {
		t.Fatal("expected access log output")
	}

	req = httptest.NewRequest(http.MethodGet, "/", nil)
	req.RemoteAddr = "10.0.0.1:9999"
	if got := getClientIP(req); got != "10.0.0.1" {
		t.Fatalf("unexpected client ip %q", got)
	}
}
