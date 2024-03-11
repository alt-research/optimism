package plasma

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	gsrpc "github.com/centrifuge/go-substrate-rpc-client/v4"
	"github.com/centrifuge/go-substrate-rpc-client/v4/signature"
	"github.com/centrifuge/go-substrate-rpc-client/v4/types"
	"github.com/centrifuge/go-substrate-rpc-client/v4/types/codec"
	"github.com/ethereum-optimism/optimism/op-plasma/pb/ref"
	"github.com/ethereum/go-ethereum/log"
	"github.com/vedhavyas/go-subkey"
	"google.golang.org/protobuf/proto"
)

type AvailConfig struct {
	Seed         string        `env:"AVAIL_SEED"`
	ApiURL       string        `env:"AVAIL_APIURL"`
	AppID        int           `env:"AVAIL_APPID"`
	WriteTimeout time.Duration `env:"AVAIL_WRITETIMEOUT"`
}

func (c AvailConfig) sanitize() error {
	if c.Seed == "" {
		return fmt.Errorf("invalid seed")
	}
	if c.ApiURL == "" {
		return fmt.Errorf("invalid client url")
	}
	if c.WriteTimeout == 0 {
		return fmt.Errorf("invalid write timeout")
	}
	return nil
}

type Avail struct {
	client      *gsrpc.SubstrateAPI
	c           *AvailConfig
	log         log.Logger
	keyringPair signature.KeyringPair
	nonce       uint32
}

func (a *Avail) getAccountNonce(accountNonce uint32) uint32 {
	if accountNonce > a.nonce {
		a.nonce = accountNonce
		return accountNonce
	}
	a.nonce++
	return a.nonce
}

func NewAvail(c *AvailConfig, log log.Logger) (*Avail, error) {
	err := c.sanitize()
	if err != nil {
		return nil, err
	}
	keyringPair, err := signature.KeyringPairFromSecret(c.Seed, 42)
	if err != nil {
		return nil, err
	}
	client, err := gsrpc.NewSubstrateAPI(c.ApiURL)
	if err != nil {
		return nil, err
	}
	return &Avail{
		client:      client,
		c:           c,
		keyringPair: keyringPair,
		log:         log,
	}, nil
}

func (a *Avail) GetInput(ctx context.Context, key []byte) ([]byte, error) {
	var r ref.Ref
	if err := proto.Unmarshal(key, &r); err != nil {
		return nil, err
	}
	a.log.Debug(
		"trying to get data from avail",
		"blockHash", r.GetAvail().GetBlockHash(),
		"sender", r.GetAvail().GetSender(),
		"nonce", r.GetAvail().GetNonce(),
	)
	blockHash, err := types.NewHashFromHexString(r.GetAvail().GetBlockHash())
	if err != nil {
		return nil, err
	}
	block, err := a.client.RPC.Chain.GetBlock(blockHash)
	if err != nil {
		return nil, err
	}
	for _, ext := range block.Block.Extrinsics {
		extAddr, err := subkey.SS58Address(ext.Signature.Signer.AsID.ToBytes(), 42)
		if err != nil {
			return nil, err
		}
		if extAddr == r.GetAvail().GetSender() && ext.Signature.Nonce.Int64() == r.GetAvail().GetNonce() {
			args := ext.Method.Args
			var data []byte
			if err = codec.Decode(args, &data); err != nil {
				return nil, err
			}
			a.log.Debug(
				"successfully get data from avail",
				"blockHash", r.GetAvail().GetBlockHash(),
				"sender", r.GetAvail().GetSender(),
				"nonce", r.GetAvail().GetNonce(),
			)
			return data, nil
		}
	}
	return nil, fmt.Errorf(
		"avail data not found hash:%s sender:%s, nonce:%d",
		r.GetAvail().GetBlockHash(),
		r.GetAvail().GetSender(),
		r.GetAvail().GetNonce(),
	)
}

func (a *Avail) SetInput(ctx context.Context, img []byte) ([]byte, error) {
	log.Info("trying to put data to avail")
	meta, err := a.client.RPC.State.GetMetadataLatest()
	if err != nil {
		return nil, err
	}
	call, err := types.NewCall(meta, "DataAvailability.submit_data", types.NewBytes([]byte(img)))
	if err != nil {
		return nil, err
	}
	ext := types.NewExtrinsic(call)
	genesisHash, err := a.client.RPC.Chain.GetBlockHash(0)
	if err != nil {
		return nil, err
	}
	rv, err := a.client.RPC.State.GetRuntimeVersionLatest()
	if err != nil {
		return nil, err
	}
	key, err := types.CreateStorageKey(meta, "System", "Account", a.keyringPair.PublicKey)
	if err != nil {
		return nil, err
	}
	var accountInfo types.AccountInfo
	ok, err := a.client.RPC.State.GetStorageLatest(key, &accountInfo)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("avail account not found")
	}
	nonce := a.getAccountNonce(uint32(accountInfo.Nonce))

	o := types.SignatureOptions{
		BlockHash:          genesisHash,
		Era:                types.ExtrinsicEra{IsMortalEra: false},
		GenesisHash:        genesisHash,
		Nonce:              types.NewUCompactFromUInt(uint64(nonce)),
		SpecVersion:        rv.SpecVersion,
		Tip:                types.NewUCompactFromUInt(0),
		AppID:              types.NewUCompactFromUInt(uint64(a.c.AppID)),
		TransactionVersion: rv.TransactionVersion,
	}

	if err = ext.Sign(a.keyringPair, o); err != nil {
		return nil, err
	}

	sub, err := a.client.RPC.Author.SubmitAndWatchExtrinsic(ext)
	if err != nil {
		return nil, err
	}
	defer sub.Unsubscribe()
	timeout := time.After(a.c.WriteTimeout)
	for {
		select {
		case status := <-sub.Chan():
			if status.IsFinalized {
				r := &ref.Ref{
					Value: &ref.Ref_Avail{
						Avail: &ref.Avail{
							BlockHash: status.AsFinalized.Hex(),
							Sender:    a.keyringPair.Address,
							Nonce:     o.Nonce.Int64(),
						},
					},
				}
				return proto.Marshal(r)
			}
			b, _ := json.Marshal(status)
			a.log.Info("avail waiting for submit status", "status", string(b))
		case <-timeout:
			return nil, fmt.Errorf("write avail submit status timeout")
		}
	}
}
