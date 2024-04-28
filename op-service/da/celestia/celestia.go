package celestia

import (
	"context"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/ethereum-optimism/optimism/op-service/da/pb/calldata"
	"github.com/ethereum/go-ethereum/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/rollkit/go-da"
	"github.com/rollkit/go-da/proxy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	client *proxy.Client
	conf   *CelestiaConfig

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
	Enable    bool          `env:"CELESTIA_ENABLE"`
	RPC       string        `env:"CELESTIA_RPC"`
	Namespace string        `env:"CELESTIA_NAMESPACE"`
	Timeout   time.Duration `env:"CELESTIA_TIMEOUT" default:"2m"`
}

func (c CelestiaConfig) sanitize() error {
	if c.RPC == "" {
		return fmt.Errorf("invalid rpc endpoint")
	}
	return nil
}

func Init(c *CelestiaConfig) error {
	if err := c.sanitize(); err != nil {
		return err
	}
	if c.Timeout.Seconds() <= 0 {
		c.Timeout = 2 * time.Minute
	}
	conf = c
	client = proxy.NewClient()
	err := client.Start(
		c.RPC,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return err
	}
	return nil
}

func Put(ctx context.Context, log log.Logger, data []byte) (*calldata.Calldata, error) {
	cctx, cancel := context.WithTimeout(ctx, conf.Timeout)
	defer cancel()
	log.Info("trying to put data to celestia")
	ids, _, err := client.Submit(cctx, [][]byte{data}, &da.SubmitOptions{
		GasPrice:  -1,
		Namespace: []byte(conf.Namespace),
	})
	if err != nil {
		metrics.WithLabelValues(kindPut, stateFailure).Inc()
		return nil, err
	}
	if len(ids) != 1 {
		metrics.WithLabelValues(kindPut, stateFailure).Inc()
		return nil, fmt.Errorf("submit returned %d ids, expected 1", len(ids))
	}
	log.Info("successfully put data to celestia", "id", hex.EncodeToString(ids[0]))
	metrics.WithLabelValues(kindPut, stateSuccess).Inc()
	return &calldata.Calldata{
		Value: &calldata.Calldata_CelestiaRef{
			CelestiaRef: &calldata.CelestiaRef{
				Id: ids[0],
			},
		},
	}, nil
}

func Get(ctx context.Context, log log.Logger, d *calldata.CelestiaRef) ([]byte, error) {
	cctx, cancel := context.WithTimeout(ctx, conf.Timeout)
	defer cancel()
	log.Info(
		"trying to get data from celestia",
		"id", hex.EncodeToString(d.GetId()),
	)
	bs, err := client.Get(cctx, [][]byte{d.GetId()})
	if err != nil {
		metrics.WithLabelValues(kindGet, stateFailure).Inc()
		return nil, err
	}
	if len(bs) != 1 {
		metrics.WithLabelValues(kindGet, stateFailure).Inc()
		return nil, fmt.Errorf("retrieve data by id returned %d blobs, expected 1", len(bs))
	}
	log.Info(
		"successfully get data from celestia",
		"id", hex.EncodeToString(d.GetId()),
	)
	metrics.WithLabelValues(kindGet, stateSuccess).Inc()
	return bs[0], nil
}
