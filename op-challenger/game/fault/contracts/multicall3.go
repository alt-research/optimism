package contracts

import (
	"context"
	"fmt"
	"reflect"

	"github.com/ethereum-optimism/optimism/op-service/sources/batching"
	"github.com/ethereum-optimism/optimism/op-service/sources/batching/rpcblock"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
)

// CanonicalMulticall3Address is the deterministic CREATE2 deployment address of the Multicall3
// contract. It is identical across Ethereum mainnet, Sepolia and virtually every other EVM
// chain. See https://www.multicall3.com / https://github.com/mds1/multicall.
var CanonicalMulticall3Address = common.HexToAddress("0xcA11bde05977b3631167028862bE2a173976CA11")

// multicall3ABI only declares aggregate3 — the single method needed to batch read-only calls
// into one eth_call.
const multicall3ABI = `[{"inputs":[{"components":[{"internalType":"address","name":"target","type":"address"},{"internalType":"bool","name":"allowFailure","type":"bool"},{"internalType":"bytes","name":"callData","type":"bytes"}],"internalType":"struct Multicall3.Call3[]","name":"calls","type":"tuple[]"}],"name":"aggregate3","outputs":[{"components":[{"internalType":"bool","name":"success","type":"bool"},{"internalType":"bytes","name":"returnData","type":"bytes"}],"internalType":"struct Multicall3.Result[]","name":"returnData","type":"tuple[]"}],"stateMutability":"payable","type":"function"}]`

var multicall3Abi = mustParseAbi([]byte(multicall3ABI))

// multicall3Call3 mirrors Multicall3.Call3 for abi packing.
type multicall3Call3 struct {
	Target       common.Address
	AllowFailure bool
	CallData     []byte
}

// aggregateContractCalls executes the given homogeneous, non-reverting contract calls in a
// single eth_call to the Multicall3 contract (aggregate3), instead of issuing one eth_call per
// element. It collapses N provider requests into 1 — the difference between hundreds of metered
// calls per cycle and a handful — which is what keeps a rate-limited L1 provider from throttling
// the challenger while it loads games.
//
// allowFailure is false: if any sub-call reverts, the whole eth_call reverts and an error is
// returned. This is deliberate. Every caller here loads valid, always-succeeding reads (e.g.
// gameAtIndex over [0,count)), so a revert signals a real problem and we must never silently
// drop a result.
func aggregateContractCalls(ctx context.Context, caller *batching.MultiCaller, multicall3 common.Address, block rpcblock.Block, calls []*batching.ContractCall) ([]*batching.CallResult, error) {
	if len(calls) == 0 {
		return nil, nil
	}
	packed := make([]multicall3Call3, len(calls))
	for i, call := range calls {
		data, err := call.Pack()
		if err != nil {
			return nil, fmt.Errorf("failed to pack aggregated call %d: %w", i, err)
		}
		packed[i] = multicall3Call3{Target: call.Addr, AllowFailure: false, CallData: data}
	}

	mc3 := batching.NewBoundContract(multicall3Abi, multicall3)
	aggResult, err := caller.SingleCall(ctx, block, mc3.Call("aggregate3", packed))
	if err != nil {
		return nil, fmt.Errorf("failed to execute multicall3 aggregate3: %w", err)
	}

	// aggregate3 returns Result[]{ bool success, bytes returnData }. Decode by field index via
	// reflection so we don't depend on abi.ConvertType's (unreliable) handling of slices of
	// anonymous structs. geth guarantees field order matches the ABI component order.
	raw := reflect.ValueOf(aggResult.Get(0))
	if raw.Kind() != reflect.Slice {
		return nil, fmt.Errorf("unexpected multicall3 result kind: %s", raw.Kind())
	}
	if raw.Len() != len(calls) {
		return nil, fmt.Errorf("multicall3 returned %d results for %d calls", raw.Len(), len(calls))
	}

	results := make([]*batching.CallResult, len(calls))
	for i, call := range calls {
		elem := raw.Index(i)
		success := elem.Field(0).Bool()
		returnData := elem.Field(1).Bytes()
		if !success {
			return nil, fmt.Errorf("aggregated call %d to %s reverted", i, call.Addr)
		}
		out, err := call.Unpack(hexutil.Bytes(returnData))
		if err != nil {
			return nil, fmt.Errorf("failed to unpack aggregated call %d: %w", i, err)
		}
		results[i] = out
	}
	return results, nil
}
