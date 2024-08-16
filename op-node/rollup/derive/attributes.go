package derive

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/log"

	"github.com/ethereum-optimism/optimism/op-bindings/predeploys"
	"github.com/ethereum-optimism/optimism/op-node/rollup"
	"github.com/ethereum-optimism/optimism/op-service/eth"
)

// L1ReceiptsFetcher fetches L1 header info and receipts for the payload attributes derivation (the info tx and deposits)
type L1ReceiptsFetcher interface {
	InfoByHash(ctx context.Context, hash common.Hash) (eth.BlockInfo, error)
	FetchReceipts(ctx context.Context, blockHash common.Hash) (eth.BlockInfo, types.Receipts, error)
}

type SystemConfigL2Fetcher interface {
	SystemConfigByL2Hash(ctx context.Context, hash common.Hash) (eth.SystemConfig, error)
}

// FetchingAttributesBuilder fetches inputs for the building of L2 payload attributes on the fly.
type FetchingAttributesBuilder struct {
	rollupCfg *rollup.Config
	l1        L1ReceiptsFetcher
	l2        SystemConfigL2Fetcher
	log       log.Logger

	blockWhichNeedFetch  map[string]bool
	blockMapMaxEndNumber uint64
}

func NewFetchingAttributesBuilder(log log.Logger, rollupCfg *rollup.Config, l1 L1ReceiptsFetcher, l2 SystemConfigL2Fetcher) *FetchingAttributesBuilder {

	blockWhichNeedFetch := make(map[string]bool, 64*1024)
	var blockEndNumber uint64

	envVar := os.Getenv("BLOCK_TAG_MAP_CONFIG_PATH")
	if len(envVar) != 0 {
		blksPath := envVar
		_, err := os.Stat(blksPath)
		if err == nil {
			content, err := os.ReadFile(blksPath)
			if err != nil {
				log.Error("Error when opening file", "err", err)
				panic(err)
			}

			blks := make([]string, 0, 64*1024)
			err = json.Unmarshal(content, &blks)
			if err != nil {
				log.Error("Error during Unmarshal()", "err", err)
			}

			for _, blk := range blks {
				blockWhichNeedFetch[blk] = true
			}
			log.Warn("The fetching will use a ext block need fetch data", "len", len(blks))

			endEnvVar := os.Getenv("BLOCK_TAG_MAP_END_NUMBER")
			if len(endEnvVar) != 0 {
				var err error
				endNumber, err := strconv.Atoi(endEnvVar)
				if err != nil {
					panic(err)
				}

				blockEndNumber = uint64(endNumber)

				log.Warn("the end block map number", "number", endEnvVar, "value", endNumber)
			} else {
				log.Error("NOT SET the end block map number")
			}

		} else {
			log.Warn("No blks found", "path", blksPath)
		}
	} else {
		log.Warn("No use block tag with BLOCK_TAG_MAP_CONFIG_PATH")
	}

	return &FetchingAttributesBuilder{
		rollupCfg:            rollupCfg,
		l1:                   l1,
		l2:                   l2,
		log:                  log,
		blockWhichNeedFetch:  blockWhichNeedFetch,
		blockMapMaxEndNumber: blockEndNumber,
	}
}

// PreparePayloadAttributes prepares a PayloadAttributes template that is ready to build a L2 block with deposits only, on top of the given l2Parent, with the given epoch as L1 origin.
// The template defaults to NoTxPool=true, and no sequencer transactions: the caller has to modify the template to add transactions,
// by setting NoTxPool=false as sequencer, or by appending batch transactions as verifier.
// The severity of the error is returned; a crit=false error means there was a temporary issue, like a failed RPC or time-out.
// A crit=true error means the input arguments are inconsistent or invalid.
func (ba *FetchingAttributesBuilder) PreparePayloadAttributes(ctx context.Context, l2Parent eth.L2BlockRef, epoch eth.BlockID) (attrs *eth.PayloadAttributes, err error) {
	var l1Info eth.BlockInfo
	var depositTxs []hexutil.Bytes
	var seqNumber uint64

	sysConfig, err := ba.l2.SystemConfigByL2Hash(ctx, l2Parent.Hash)
	if err != nil {
		return nil, NewTemporaryError(fmt.Errorf("failed to retrieve L2 parent block: %w", err))
	}

	// If the L1 origin changed in this block, then we are in the first block of the epoch. In this
	// case we need to fetch all transaction receipts from the L1 origin block so we can scan for
	// user deposits.
	if l2Parent.L1Origin.Number != epoch.Number {

		// TODO: get from map json to check
		_, isHadDepositAndSystemConfig := ba.blockWhichNeedFetch[epoch.Hash.Hex()]
		isNoNeedFetch := len(ba.blockWhichNeedFetch) != 0 && !isHadDepositAndSystemConfig

		ba.log.Info("l2 parent block",
			"isNeedFetch", isHadDepositAndSystemConfig,
			"l2 parent block", l2Parent.L1Origin.Number,
			"max", ba.blockMapMaxEndNumber)

		if isNoNeedFetch && l2Parent.L1Origin.Number <= ba.blockMapMaxEndNumber {
			info, err := ba.l1.InfoByHash(ctx, epoch.Hash)
			if err != nil {
				return nil, NewTemporaryError(fmt.Errorf("failed to fetch L1 block info and receipts: %w", err))
			}

			ba.log.Warn("Skip the block fetch because not in map", "number", info.NumberU64(), "hash", epoch.Hash)

			// no need use FetchReceipts
			if l2Parent.L1Origin.Hash != info.ParentHash() {
				return nil, NewResetError(
					fmt.Errorf("cannot create new block with L1 origin %s (parent %s) on top of L1 origin %s",
						epoch, info.ParentHash(), l2Parent.L1Origin))
			}

			// is no deposit so just use a nil
			deposits, err := DeriveDeposits([]*types.Receipt{}, ba.rollupCfg.DepositContractAddress)
			if err != nil {
				// deposits may never be ignored. Failing to process them is a critical error.
				return nil, NewCriticalError(fmt.Errorf("failed to derive some deposits: %w", err))
			}
			// apply sysCfg changes
			// no changes

			l1Info = info
			depositTxs = deposits
			seqNumber = 0
		} else {
			// original logic
			info, receipts, err := ba.l1.FetchReceipts(ctx, epoch.Hash)
			if err != nil {
				return nil, NewTemporaryError(fmt.Errorf("failed to fetch L1 block info and receipts: %w", err))
			}
			if l2Parent.L1Origin.Hash != info.ParentHash() {
				return nil, NewResetError(
					fmt.Errorf("cannot create new block with L1 origin %s (parent %s) on top of L1 origin %s",
						epoch, info.ParentHash(), l2Parent.L1Origin))
			}

			deposits, err := DeriveDeposits(receipts, ba.rollupCfg.DepositContractAddress)
			if err != nil {
				// deposits may never be ignored. Failing to process them is a critical error.
				return nil, NewCriticalError(fmt.Errorf("failed to derive some deposits: %w", err))
			}
			// apply sysCfg changes
			if err := UpdateSystemConfigWithL1Receipts(&sysConfig, receipts, ba.rollupCfg, info.Time()); err != nil {
				return nil, NewCriticalError(fmt.Errorf("failed to apply derived L1 sysCfg updates: %w", err))
			}

			l1Info = info
			depositTxs = deposits
			seqNumber = 0
		}
	} else {
		if l2Parent.L1Origin.Hash != epoch.Hash {
			return nil, NewResetError(fmt.Errorf("cannot create new block with L1 origin %s in conflict with L1 origin %s", epoch, l2Parent.L1Origin))
		}
		info, err := ba.l1.InfoByHash(ctx, epoch.Hash)
		if err != nil {
			return nil, NewTemporaryError(fmt.Errorf("failed to fetch L1 block info: %w", err))
		}
		l1Info = info
		depositTxs = nil
		seqNumber = l2Parent.SequenceNumber + 1
	}

	// Sanity check the L1 origin was correctly selected to maintain the time invariant between L1 and L2
	nextL2Time := l2Parent.Time + ba.rollupCfg.BlockTime
	if nextL2Time < l1Info.Time() {
		return nil, NewResetError(fmt.Errorf("cannot build L2 block on top %s for time %d before L1 origin %s at time %d",
			l2Parent, nextL2Time, eth.ToBlockID(l1Info), l1Info.Time()))
	}

	var upgradeTxs []hexutil.Bytes
	if ba.rollupCfg.IsEcotoneActivationBlock(nextL2Time) {
		upgradeTxs, err = EcotoneNetworkUpgradeTransactions()
		if err != nil {
			return nil, NewCriticalError(fmt.Errorf("failed to build ecotone network upgrade txs: %w", err))
		}
	}

	l1InfoTx, err := L1InfoDepositBytes(ba.rollupCfg, sysConfig, seqNumber, l1Info, nextL2Time)
	if err != nil {
		return nil, NewCriticalError(fmt.Errorf("failed to create l1InfoTx: %w", err))
	}

	txs := make([]hexutil.Bytes, 0, 1+len(depositTxs)+len(upgradeTxs))
	txs = append(txs, l1InfoTx)
	txs = append(txs, depositTxs...)
	txs = append(txs, upgradeTxs...)

	var withdrawals *types.Withdrawals
	if ba.rollupCfg.IsCanyon(nextL2Time) {
		withdrawals = &types.Withdrawals{}
	}

	var parentBeaconRoot *common.Hash
	if ba.rollupCfg.IsEcotone(nextL2Time) {
		parentBeaconRoot = l1Info.ParentBeaconRoot()
		if parentBeaconRoot == nil { // default to zero hash if there is no beacon-block-root available
			parentBeaconRoot = new(common.Hash)
		}
	}

	return &eth.PayloadAttributes{
		Timestamp:             hexutil.Uint64(nextL2Time),
		PrevRandao:            eth.Bytes32(l1Info.MixDigest()),
		SuggestedFeeRecipient: predeploys.SequencerFeeVaultAddr,
		Transactions:          txs,
		NoTxPool:              true,
		GasLimit:              (*eth.Uint64Quantity)(&sysConfig.GasLimit),
		Withdrawals:           withdrawals,
		ParentBeaconBlockRoot: parentBeaconRoot,
	}, nil
}
