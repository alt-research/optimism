package client

import (
	"context"
	"fmt"
	"net/http"
)

// RollupBoostNextClient retrieves rollup-boost health using the HTTP-based healthcheck endpoint.
// It interprets the HTTP status code to determine health:
//   - 200 OK: Healthy (both L2 and builder producing blocks)
//   - 206 Partial Content: L2 healthy but builder is not
//   - 503 Service Unavailable: unhealthy
type RollupBoostNextClient struct {
	url        string
	httpClient *http.Client
}

// NewRollupBoostNextClient constructs a client for querying the rollup-boost health endpoint.
// The url parameter should be the full URL including path (e.g., "http://localhost:8080/healthz").
func NewRollupBoostNextClient(url string, httpClient *http.Client) *RollupBoostNextClient {
	if httpClient == nil {
		httpClient = http.DefaultClient
	}
	return &RollupBoostNextClient{
		url:        url,
		httpClient: httpClient,
	}
}

// Healthcheck fetches the rollup-boost health endpoint and interprets the HTTP status code.
func (c *RollupBoostNextClient) Healthcheck(ctx context.Context) (HealthStatus, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.url, nil)
	if err != nil {
		return "", fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("failed to make request: %w", err)
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusOK:
		return HealthStatusHealthy, nil
	case http.StatusPartialContent:
		return HealthStatusPartial, nil
	case http.StatusServiceUnavailable:
		return HealthStatusUnhealthy, nil
	default:
		return "", fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}
}

// Ensure RollupBoostNextClient implements RollupBoostHealthChecker
var _ RollupBoostHealthChecker = (*RollupBoostNextClient)(nil)
