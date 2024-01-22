package celestia

import (
	"context"
	"fmt"

	"github.com/ethereum-optimism/optimism/op-service/pb/calldata"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/rollkit/go-da/proxy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	client *proxy.Client

	metrics = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "da",
		Subsystem: "celestia",
		Name:      "operation",
	}, []string{
		"kind",
		"state",
	})
	kindPut      = "put"
	kindGet      = "get"
	stateSuccess = "success"
	stateFailure = "failure"
)

type CelestiaConfig struct {
	CelestiaRPC string `env:"CELESTIA_RPC"`
}

func (c CelestiaConfig) sanitize() error {
	if c.CelestiaRPC == "" {
		return fmt.Errorf("invalid rpc endpoint")
	}
	return nil
}

func Init(c *CelestiaConfig) error {
	if err := c.sanitize(); err != nil {
		return err
	}
	client = proxy.NewClient()
	err := client.Start(c.CelestiaRPC, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	return nil
}

func Put(ctx context.Context, data []byte) (*calldata.Calldata, error) {
	ids, _, err := client.Submit(ctx, [][]byte{data}, -1)
	if err != nil {
		metrics.WithLabelValues(kindPut, stateFailure).Inc()
		return nil, err
	}
	if len(ids) != 1 {
		metrics.WithLabelValues(kindPut, stateFailure).Inc()
		return nil, fmt.Errorf("submit returned %d ids, expected 1", len(ids))
	}
	metrics.WithLabelValues(kindPut, stateSuccess).Inc()
	return &calldata.Calldata{
		Value: &calldata.Calldata_CelestiaRef{
			CelestiaRef: &calldata.CelestiaRef{
				Id: ids[0],
			},
		},
	}, nil
}

func Get(ctx context.Context, d *calldata.CelestiaRef) ([]byte, error) {
	bs, err := client.Get(ctx, [][]byte{d.GetId()})
	if err != nil {
		metrics.WithLabelValues(kindGet, stateFailure).Inc()
		return nil, err
	}
	if len(bs) != 1 {
		metrics.WithLabelValues(kindGet, stateFailure).Inc()
		return nil, fmt.Errorf("retrieve data by id returned %d blobs, expected 1", len(bs))
	}
	metrics.WithLabelValues(kindGet, stateSuccess).Inc()
	return bs[0], nil
}
