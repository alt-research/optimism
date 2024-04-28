package da

import (
	"context"
	"fmt"
	"slices"

	env "github.com/Netflix/go-env"
	"github.com/ethereum-optimism/optimism/op-service/da/avail"
	"github.com/ethereum-optimism/optimism/op-service/da/celestia"
	"github.com/ethereum-optimism/optimism/op-service/da/eigenda"
	"github.com/ethereum-optimism/optimism/op-service/da/pb/calldata"
	"github.com/ethereum-optimism/optimism/op-service/da/s3"
	"github.com/ethereum/go-ethereum/log"
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
	// config for avail
	avail.AvailConfig
}

func init() {
	_, err := env.UnmarshalFromEnviron(&config)
	if err != nil {
		panic(err)
	}
	if config.DAName != "" && !slices.Contains([]string{"eigenda", "s3", "celestia", "avail"}, config.DAName) {
		panic(fmt.Sprintf("unspecified DA %s", config.DAName))
	}
	if config.EigenDAConfig.Enable {
		if err = eigenda.Init(&config.EigenDAConfig); err != nil {
			panic(err)
		}
	}
	if config.CelestiaConfig.Enable {
		if err = celestia.Init(&config.CelestiaConfig); err != nil {
			panic(err)
		}
	}
	if config.AvailConfig.Enable {
		if err = avail.Init(&config.AvailConfig); err != nil {
			panic(err)
		}
	}
	if config.S3Config.Enable {
		if err = s3.Init(&config.S3Config); err != nil {
			panic(err)
		}
	}
}

func Put(ctx context.Context, log log.Logger, data []byte) (*calldata.Calldata, error) {
	var (
		c   *calldata.Calldata
		err error
	)
	switch config.DAName {
	case "eigenda":
		c, err = eigenda.Put(ctx, log, data)
	case "s3":
		c, err = s3.Put(ctx, log, data)
	case "celestia":
		c, err = celestia.Put(ctx, log, data)
	case "avail":
		c, err = avail.Put(ctx, log, data)
	default:
		return nil, fmt.Errorf("unspecified DA: %s", config.DAName)
	}
	if err != nil {
		return nil, fmt.Errorf("put data to %s error: %w", config.DAName, err)
	}
	return c, nil
}

func Get(ctx context.Context, log log.Logger, data *calldata.Calldata) ([]byte, error) {
	var (
		res []byte
		err error
	)
	switch data.Value.(type) {
	case *calldata.Calldata_EigendaRef:
		res, err = eigenda.Get(ctx, log, data.GetEigendaRef())
	case *calldata.Calldata_Digest:
		res, err = s3.Get(ctx, log, data.GetDigest())
	case *calldata.Calldata_CelestiaRef:
		res, err = celestia.Get(ctx, log, data.GetCelestiaRef())
	case *calldata.Calldata_AvailRef:
		res, err = avail.Get(ctx, log, data.GetAvailRef())
	default:
		log.Debug("da fallback to raw data")
		return data.GetRaw(), nil
	}
	if err != nil {
		return nil, fmt.Errorf("get data from %s error: %w", config.DAName, err)
	}
	return res, nil
}
