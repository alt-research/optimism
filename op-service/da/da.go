package da

import (
	"context"
	"fmt"

	env "github.com/Netflix/go-env"
	"github.com/ethereum-optimism/optimism/op-service/da/avail"
	"github.com/ethereum-optimism/optimism/op-service/da/celestia"
	"github.com/ethereum-optimism/optimism/op-service/da/eigenda"
	"github.com/ethereum-optimism/optimism/op-service/da/pb/calldata"
	"github.com/ethereum-optimism/optimism/op-service/da/s3"
)

var (
	config Config
)

type Config struct {
	DAName string `env:"DA_NAME"`
	// config for eigenda
	eigenda.EigenDAConfig
	// config for s3
	s3.S3Config
	// config for celestia
	celestia.CelestiaConfig
	avail.AvailConfig
}

func init() {
	_, err := env.UnmarshalFromEnviron(&config)
	if err != nil {
		panic(err)
	}
	switch config.DAName {
	case "eigenda":
		err = eigenda.Init(&config.EigenDAConfig)
	case "s3":
		err = s3.Init(&config.S3Config)
	case "celestia":
		err = celestia.Init(&config.CelestiaConfig)
	case "avail":
		err = avail.Init(&config.AvailConfig)
	default:
		panic("unspecified DA")
	}
	if err != nil {
		panic(err)
	}
}

func Put(ctx context.Context, data []byte) (*calldata.Calldata, error) {
	var (
		c   *calldata.Calldata
		err error
	)
	switch config.DAName {
	case "eigenda":
		c, err = eigenda.Put(ctx, data)
	case "s3":
		c, err = s3.Put(ctx, data)
	case "celestia":
		c, err = celestia.Put(ctx, data)
	case "avail":
		c, err = avail.Put(ctx, data)
	default:
		return nil, fmt.Errorf("unspecified DA")
	}
	if err != nil {
		return nil, fmt.Errorf("put data to %s error: %w", config.DAName, err)
	}
	return c, nil
}

func Get(ctx context.Context, data *calldata.Calldata) ([]byte, error) {
	var (
		res []byte
		err error
	)
	switch data.Value.(type) {
	case *calldata.Calldata_EigendaRef:
		res, err = eigenda.Get(ctx, data.GetEigendaRef())
	case *calldata.Calldata_Digest:
		res, err = s3.Get(ctx, data.GetDigest())
	case *calldata.Calldata_CelestiaRef:
		res, err = celestia.Get(ctx, data.GetCelestiaRef())
	default:
		return data.GetRaw(), nil
	}
	if err != nil {
		return nil, fmt.Errorf("get data from %s error: %w", config.DAName, err)
	}
	return res, nil
}
