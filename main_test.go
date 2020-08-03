package main

import (
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
)

func TestLivenessProbeHandler(t *testing.T) {
	req, err := http.NewRequest("GET", "/healthz", nil)
	if err != nil {
		t.Fatal(err)
	}
	rr := httptest.NewRecorder()
	handler := http.HandlerFunc(livenessProbeHandler)
	handler.ServeHTTP(rr, req)
	if status := rr.Code; status != http.StatusOK {
		t.Errorf("handler returned wrong status code: got %v want %v", status, http.StatusOK)
	}
}

// TestReadinessProbeHandler relies on Google's public buckets being available otherwise this test may return false
// positives.
func TestReadinessProbeHandler(t *testing.T) {

	var tests = []struct {
		Name string
		value string
		expected int
	}{
		{"good", "gcp-public-data-landsat,gcp-public-data-nexrad-l2,gcp-public-data-sentinel-2", http.StatusOK},
		{"bad", "fake-bucket-1", http.StatusServiceUnavailable},
		{"bad-bad", "fake-bucket-1,fake-bucket-2", http.StatusServiceUnavailable},
		{"bad-good", "fake-bucket-1,gcp-public-data-landsat,gcp-public-data-nexrad-l2,gcp-public-data-sentinel-2", http.StatusOK},
		{"good-bad", "gcp-public-data-landsat,gcp-public-data-nexrad-l2,gcp-public-data-sentinel-2,fake-bucket-1", http.StatusOK},
	}

	req, err := http.NewRequest("GET", "/readiness", nil)
	if err != nil {
		t.Fatal(err)
	}

	for _, test := range tests {
		*readinessBuckets = test.value
		handler := http.HandlerFunc(readinessProbeHandler)
		rr := httptest.NewRecorder()
		handler.ServeHTTP(rr, req)
		if status := rr.Code; status != test.expected {
			t.Errorf("handler returned incorrect status code for test '%s': got %v want %v", test.Name, status, test.expected)
		}
	}

}

func TestMain(m *testing.M) {
	initClient()
	os.Exit(m.Run())
}
