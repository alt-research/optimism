package client

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/rpc"
	"github.com/stretchr/testify/require"
)

// This header triggered Myshell's challenger failure: omitting blockAccessListHash
// when rehashing it produced 0x9770e5e0a9f042c794cd0e4c30ed28cab9967c5a045be641bb198519780125fd.
func TestPollingClientLatestHeaderPreservesBlockHash(t *testing.T) {
	t.Parallel()

	headerJSON, err := os.ReadFile("testdata/sepolia-header-11861725.json")
	require.NoError(t, err)
	var advertised struct {
		Hash common.Hash `json:"hash"`
	}
	require.NoError(t, json.Unmarshal(headerJSON, &advertised))
	require.NotEqual(t, common.Hash{}, advertised.Hash)

	server := rpc.NewServer()
	defer server.Stop()
	require.NoError(t, server.RegisterName("eth", &headerFixtureAPI{header: headerJSON}))
	base := NewBaseRPCClient(rpc.DialInProc(server))
	polling := NewPollingClient(t.Context(), log.NewLogger(log.DiscardHandler()), base, WithPollRate(0))
	defer polling.Close()

	header, err := polling.getLatestHeader()
	require.NoError(t, err)
	require.NotNil(t, header)
	require.Equal(t, advertised.Hash, header.Hash(), "the challenger uses the locally computed hash for L1 eth_call")
}

type headerFixtureAPI struct {
	header json.RawMessage
}

func (api *headerFixtureAPI) GetBlockByNumber(_ context.Context, label string, fullTransactions bool) (json.RawMessage, error) {
	if label != "latest" || fullTransactions {
		return nil, fmt.Errorf("unexpected block request: label=%s fullTransactions=%t", label, fullTransactions)
	}
	return api.header, nil
}
