package api

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"lumenvec/internal/core"
)

func newAPITestServer(t *testing.T) *Server {
	t.Helper()
	base := t.TempDir()
	return NewServerWithOptions(ServerOptions{
		Port:          ":0",
		ReadTimeout:   5 * time.Second,
		WriteTimeout:  5 * time.Second,
		MaxBodyBytes:  1 << 20,
		MaxVectorDim:  16,
		MaxK:          5,
		SnapshotPath:  filepath.Join(base, "snapshot.json"),
		WALPath:       filepath.Join(base, "wal.log"),
		SnapshotEvery: 2,
		SearchMode:    "exact",
	})
}

func TestApplyDefaults(t *testing.T) {
	opts := applyDefaults(ServerOptions{})
	if opts.Port != ":19190" || opts.SearchMode != "exact" {
		t.Fatal("unexpected defaults")
	}
}

func TestServerHandlersLifecycleAndBatch(t *testing.T) {
	server := newAPITestServer(t)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/vectors", bytes.NewBufferString(`{"id":"a","values":[1,2,3]}`))
	server.Router().ServeHTTP(rec, req)
	if rec.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d", rec.Code)
	}

	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/vectors/a", nil)
	server.Router().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodPost, "/vectors/search", bytes.NewBufferString(`{"values":[1,2,3],"k":1}`))
	server.Router().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodPost, "/vectors/batch", bytes.NewBufferString(`{"vectors":[{"id":"b","values":[4,5,6]}]}`))
	server.Router().ServeHTTP(rec, req)
	if rec.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d", rec.Code)
	}

	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodPost, "/vectors/search/batch", bytes.NewBufferString(`{"queries":[{"id":"q1","values":[4,5,6],"k":1}]}`))
	server.Router().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	var got []map[string]interface{}
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}
	if len(got) != 1 || got[0]["id"] != "q1" {
		t.Fatal("unexpected batch search payload")
	}

	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodDelete, "/vectors/a", nil)
	server.Router().ServeHTTP(rec, req)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("expected 204, got %d", rec.Code)
	}
}

func TestServerValidationErrors(t *testing.T) {
	server := newAPITestServer(t)

	cases := []struct {
		method string
		path   string
		body   string
		code   int
	}{
		{http.MethodPost, "/vectors", "{", http.StatusBadRequest},
		{http.MethodPost, "/vectors", `{"id":"","values":[1]}`, http.StatusBadRequest},
		{http.MethodPost, "/vectors/search", `{"values":[],"k":1}`, http.StatusBadRequest},
		{http.MethodPost, "/vectors/search/batch", "{", http.StatusBadRequest},
	}

	for _, tc := range cases {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(tc.method, tc.path, bytes.NewBufferString(tc.body))
		server.Router().ServeHTTP(rec, req)
		if rec.Code != tc.code {
			t.Fatalf("%s %s: expected %d, got %d", tc.method, tc.path, tc.code, rec.Code)
		}
	}
}

func TestHealthRouterAndStatusMapping(t *testing.T) {
	server := newAPITestServer(t)
	if server.Router() == nil {
		t.Fatal("expected router")
	}

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	server.HealthHandler(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	if got := statusFromServiceError(core.ErrInvalidValues); got != http.StatusBadRequest {
		t.Fatalf("unexpected status %d", got)
	}
	if got := statusFromServiceError(errors.New("x")); got != http.StatusInternalServerError {
		t.Fatalf("unexpected status %d", got)
	}

	httpServer := server.httpServer()
	if httpServer.Addr != server.port || httpServer.Handler == nil {
		t.Fatal("expected configured http server")
	}
}

func TestNewServerAndStart(t *testing.T) {
	server := NewServer("19190")
	if server == nil {
		t.Fatal("expected server")
	}

	oldListen := listenAndServeFunc
	oldFatal := logFatalfAPI
	oldPrintf := logPrintfAPI
	t.Cleanup(func() {
		listenAndServeFunc = oldListen
		logFatalfAPI = oldFatal
		logPrintfAPI = oldPrintf
	})

	var loggedStart bool
	var fatalCalled bool
	logPrintfAPI = func(string, ...interface{}) { loggedStart = true }
	listenAndServeFunc = func(*http.Server) error { return errors.New("boom") }
	logFatalfAPI = func(string, ...interface{}) { fatalCalled = true }

	server.Start()
	if !loggedStart || !fatalCalled {
		t.Fatal("expected start path logging and fatal")
	}
}

func TestServerHandlerErrorBranches(t *testing.T) {
	server := newAPITestServer(t)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/vectors", bytes.NewBufferString(`{"id":"dup","values":[1,2,3]}`))
	server.Router().ServeHTTP(rec, req)
	if rec.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d", rec.Code)
	}

	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodPost, "/vectors", bytes.NewBufferString(`{"id":"dup","values":[1,2,3]}`))
	server.Router().ServeHTTP(rec, req)
	if rec.Code != http.StatusConflict {
		t.Fatalf("expected 409, got %d", rec.Code)
	}

	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodPost, "/vectors/batch", bytes.NewBufferString(`{"vectors":[]}`))
	server.Router().ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}

	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/vectors/missing", nil)
	server.Router().ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", rec.Code)
	}

	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodDelete, "/vectors/missing", nil)
	server.Router().ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", rec.Code)
	}
}
