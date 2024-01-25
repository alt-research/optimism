package celestia

import (
	"context"
	"fmt"
	"time"

	gsrpc "github.com/centrifuge/go-substrate-rpc-client/v4"
	"github.com/centrifuge/go-substrate-rpc-client/v4/signature"
	"github.com/centrifuge/go-substrate-rpc-client/v4/types"
	"github.com/centrifuge/go-substrate-rpc-client/v4/types/codec"
	"github.com/ethereum-optimism/optimism/op-service/da/pb/calldata"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/vedhavyas/go-subkey"
)

var (
	api         *gsrpc.SubstrateAPI
	conf        *AnvilConfig
	keyringPair signature.KeyringPair

	metrics = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "da",
		Subsystem: "anvil",
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

var localNonce uint32 = 0

func GetAccountNonce(accountNonce uint32) uint32 {
	if accountNonce > localNonce {
		localNonce = accountNonce
		return accountNonce
	}
	localNonce++
	return localNonce
}

type AnvilConfig struct {
	Seed         string        `env:"ANVIL_SEED"`
	ApiURL       string        `env:"ANVIL_APIURL"`
	AppID        int           `env:"ANVIL_APPID"`
	WriteTimeout time.Duration `env:"ANVIL_WRITETIMEOUT"`
}

func (c AnvilConfig) sanitize() error {
	if c.Seed == "" {
		return fmt.Errorf("invalid seed")
	}
	if c.ApiURL == "" {
		return fmt.Errorf("invalid api url")
	}
	if c.WriteTimeout == 0 {
		return fmt.Errorf("invalid write timeout")
	}
	return nil
}

func Init(c *AnvilConfig) error {
	err := c.sanitize()
	if err != nil {
		return err
	}
	keyringPair, err = signature.KeyringPairFromSecret(conf.Seed, 42)
	if err != nil {
		return err
	}
	conf = c
	api, err = gsrpc.NewSubstrateAPI(c.ApiURL)
	return err
}

func Put(ctx context.Context, data []byte) (*calldata.Calldata, error) {
	meta, err := api.RPC.State.GetMetadataLatest()
	if err != nil {
		metrics.WithLabelValues(kindPut, stateFailure).Inc()
		return nil, err
	}
	call, err := types.NewCall(meta, "DataAvailability.submit_data", types.NewBytes([]byte(data)))
	if err != nil {
		metrics.WithLabelValues(kindPut, stateFailure).Inc()
		return nil, err
	}
	ext := types.NewExtrinsic(call)
	genesisHash, err := api.RPC.Chain.GetBlockHash(0)
	if err != nil {
		metrics.WithLabelValues(kindPut, stateFailure).Inc()
		return nil, err
	}
	rv, err := api.RPC.State.GetRuntimeVersionLatest()
	if err != nil {
		metrics.WithLabelValues(kindPut, stateFailure).Inc()
		return nil, err
	}
	key, err := types.CreateStorageKey(meta, "System", "Account", keyringPair.PublicKey)
	if err != nil {
		metrics.WithLabelValues(kindPut, stateFailure).Inc()
		return nil, err
	}
	var accountInfo types.AccountInfo
	ok, err := api.RPC.State.GetStorageLatest(key, &accountInfo)
	if err != nil {
		metrics.WithLabelValues(kindPut, stateFailure).Inc()
		return nil, err
	}
	if !ok {
		metrics.WithLabelValues(kindPut, stateFailure).Inc()
		return nil, fmt.Errorf("anvil account not found")
	}
	nonce := GetAccountNonce(uint32(accountInfo.Nonce))

	o := types.SignatureOptions{
		BlockHash:          genesisHash,
		Era:                types.ExtrinsicEra{IsMortalEra: false},
		GenesisHash:        genesisHash,
		Nonce:              types.NewUCompactFromUInt(uint64(nonce)),
		SpecVersion:        rv.SpecVersion,
		Tip:                types.NewUCompactFromUInt(0),
		AppID:              types.NewUCompactFromUInt(uint64(conf.AppID)),
		TransactionVersion: rv.TransactionVersion,
	}

	if err = ext.Sign(keyringPair, o); err != nil {
		metrics.WithLabelValues(kindPut, stateFailure).Inc()
		return nil, err
	}

	sub, err := api.RPC.Author.SubmitAndWatchExtrinsic(ext)
	if err != nil {
		metrics.WithLabelValues(kindPut, stateFailure).Inc()
		return nil, err
	}
	defer sub.Unsubscribe()
	timeout := time.After(conf.WriteTimeout)
	for {
		select {
		case status := <-sub.Chan():
			if status.IsFinalized {
				metrics.WithLabelValues(kindPut, stateSuccess).Inc()
				return &calldata.Calldata{
					Value: &calldata.Calldata_AnvilRef{
						AnvilRef: &calldata.AnvilRef{
							BlockHash: status.AsFinalized.Hex(),
							Sender:    keyringPair.Address,
							Nonce:     o.Nonce.Int64(),
						},
					},
				}, nil
			}
		case <-timeout:
			metrics.WithLabelValues(kindPut, stateFailure).Inc()
			return nil, fmt.Errorf("write anvil timeout")
		}
	}
}

func Get(ctx context.Context, d *calldata.AnvilRef) ([]byte, error) {
	blockHash, err := types.NewHashFromHexString(d.BlockHash)
	if err != nil {
		metrics.WithLabelValues(kindGet, stateFailure).Inc()
		return nil, err
	}
	block, err := api.RPC.Chain.GetBlock(blockHash)
	if err != nil {
		metrics.WithLabelValues(kindGet, stateFailure).Inc()
		return nil, err
	}
	for _, ext := range block.Block.Extrinsics {
		extAddr, err := subkey.SS58Address(ext.Signature.Signer.AsID.ToBytes(), 42)
		if err != nil {
			metrics.WithLabelValues(kindGet, stateFailure).Inc()
			return nil, err
		}
		if extAddr == d.Sender && ext.Signature.Nonce.Int64() == d.Nonce {
			args := ext.Method.Args
			var data []byte
			if err = codec.Decode(args, &data); err != nil {
				metrics.WithLabelValues(kindGet, stateFailure).Inc()
				return nil, err
			}
			metrics.WithLabelValues(kindGet, stateSuccess).Inc()
			return data, nil
		}
	}
	metrics.WithLabelValues(kindGet, stateFailure).Inc()
	return nil, fmt.Errorf("anvil data not found hash:%s sender:%s, nonce:%d", d.BlockHash, d.Sender, d.Nonce)
}
