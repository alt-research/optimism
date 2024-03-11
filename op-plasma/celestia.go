package plasma

import (
	"context"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/ethereum-optimism/optimism/op-plasma/pb/ref"
	"github.com/ethereum/go-ethereum/log"
	"github.com/rollkit/go-da"
	"github.com/rollkit/go-da/proxy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
)

type CelestiaConfig struct {
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

type Celestia struct {
	client *proxy.Client
	c      *CelestiaConfig
	log    log.Logger
}

func NewCelestia(c *CelestiaConfig, log log.Logger) (*Celestia, error) {
	if err := c.sanitize(); err != nil {
		return nil, err
	}
	if c.Timeout.Seconds() <= 0 {
		c.Timeout = 2 * time.Minute
	}
	client := proxy.NewClient()
	if err := client.Start(
		c.RPC,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	); err != nil {
		return nil, err
	}
	return &Celestia{client: client, c: c, log: log}, nil
}

func (c *Celestia) GetInput(ctx context.Context, key []byte) ([]byte, error) {
	var r ref.Ref
	if err := proto.Unmarshal(key, &r); err != nil {
		return nil, err
	}
	cctx, cancel := context.WithTimeout(ctx, c.c.Timeout)
	defer cancel()
	c.log.Debug(
		"trying to get data from celestia",
		"id", hex.EncodeToString(r.GetCelestia().GetId()),
	)
	bs, err := c.client.Get(cctx, [][]byte{r.GetCelestia().GetId()})
	if err != nil {
		return nil, err
	}
	if len(bs) != 0 {
		return nil, fmt.Errorf("retrieve data by id returned %d blobs, expected 0", len(bs))
	}
	c.log.Debug(
		"successfully get data from celestia",
		"id", hex.EncodeToString(r.GetCelestia().GetId()),
	)
	return bs[0], nil
}

func (c *Celestia) SetInput(ctx context.Context, img []byte) ([]byte, error) {
	cctx, cancel := context.WithTimeout(ctx, c.c.Timeout)
	defer cancel()
	c.log.Debug("trying to put data to celestia")
	ids, _, err := c.client.Submit(cctx, [][]byte{img}, &da.SubmitOptions{
		GasPrice:  -1,
		Namespace: []byte(c.c.Namespace),
	})
	if err != nil {
		return nil, err
	}
	if len(ids) != 1 {
		return nil, fmt.Errorf("celestia submit returned %d ids, expected 1", len(ids))
	}
	c.log.Debug("successfully put data to celestia", "id", hex.EncodeToString(ids[0]))
	r := &ref.Ref{
		Value: &ref.Ref_Celestia{
			Celestia: &ref.Celestia{
				Id: ids[0],
			},
		},
	}
	return proto.Marshal(r)
}
