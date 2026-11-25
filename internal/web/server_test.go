package web

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestDashboardEndpointsAndClose(t *testing.T) {
	s, err := NewServer()
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
	}()

	h := s.Handler()
	for _, path := range []string{
		"/",
		"/static/app.js",
		"/static/styles.css",
		"/api/state",
		"/api/frames?limit=10",
		"/api/timeseries",
		"/api/compare",
		"/api/verify",
	} {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("%s status=%d body=%s", path, rec.Code, rec.Body.String())
		}
		if path == "/" && !strings.Contains(rec.Body.String(), "当前淘汰候选") {
			t.Fatal("index does not contain expected panels")
		}
	}

	body := strings.NewReader(`{"capacity":240}`)
	req := httptest.NewRequest(http.MethodPost, "/api/capacity", body)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"capacity": 240`) {
		t.Fatalf("capacity update failed code=%d body=%s", rec.Code, rec.Body.String())
	}
}
