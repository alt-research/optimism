package client

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRollupBoostNextHealthcheck(t *testing.T) {
	testCases := []struct {
		name       string
		body       string
		statusCode int
		wantStatus HealthStatus
		wantErr    string
	}{
		{
			name:       "healthy",
			body:       "OK",
			statusCode: http.StatusOK,
			wantStatus: HealthStatusHealthy,
		},
		{
			name:       "partial",
			body:       "Partial Content",
			statusCode: http.StatusPartialContent,
			wantStatus: HealthStatusPartial,
		},
		{
			name:       "unhealthy",
			body:       "Service Unavailable",
			statusCode: http.StatusServiceUnavailable,
			wantStatus: HealthStatusUnhealthy,
		},
		{
			name:       "unexpected status code",
			body:       "Accepted",
			statusCode: http.StatusAccepted,
			wantErr:    "unexpected status code: 202",
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				require.Equal(t, HealthzEndpoint, r.URL.Path)
				w.WriteHeader(tc.statusCode)
				_, _ = w.Write([]byte(tc.body))
			}))
			defer server.Close()

			client := NewRollupBoostNextClient(server.URL+HealthzEndpoint, server.Client())
			status, err := client.Healthcheck(context.Background())

			if tc.wantErr != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.wantErr)
				return
			}

			require.NoError(t, err)
			require.Equal(t, tc.wantStatus, status)
		})
	}
}
