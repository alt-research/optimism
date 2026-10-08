package sequencing

import (
	"context"
	"math/big"
	"testing"
	"time"

	"github.com/ethereum-optimism/optimism/op-node/rollup"
	"github.com/ethereum-optimism/optimism/op-node/rollup/confdepth"
	"github.com/ethereum-optimism/optimism/op-node/rollup/derive"
	"github.com/ethereum-optimism/optimism/op-node/rollup/engine"
	"github.com/ethereum-optimism/optimism/op-service/eth"
	"github.com/ethereum-optimism/optimism/op-service/event"
	"github.com/ethereum-optimism/optimism/op-service/log"
	"github.com/ethereum-optimism/optimism/op-service/testlog"
	"github.com/ethereum-optimism/optimism/op-service/testutils"
	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/stretchr/testify/require"
)

type readinessEmitter struct{}

func (readinessEmitter) Emit(context.Context, event.Event) {}

// TestSequencerAdvancesOriginBeforeForkchoicePrefetch covers the direct engine
// insertion path: the next build can run before the shared event dispatcher
// delivers the inserted head's forkchoice update to the origin selector.
func TestSequencerAdvancesOriginBeforeForkchoicePrefetch(t *testing.T) {
	ctx := context.Background()
	logger := testlog.Logger(t, log.LevelCrit)
	seq, deps := createSequencer(logger)
	deps.cfg.BlockTime = 10
	deps.cfg.IsthmusTime = nil
	deps.cfg.JovianTime = nil
	l1 := &testutils.MockL1Source{}
	t.Cleanup(func() { l1.AssertExpectations(t) })
	selector := NewL1OriginSelector(ctx, logger, deps.cfg, l1)
	seq.l1OriginSelector = selector
	seq.AttachEmitter(readinessEmitter{})

	a := eth.L1BlockRef{Hash: common.Hash{0xa}, Number: 818, Time: 9816}
	b := eth.L1BlockRef{Hash: common.Hash{0xb}, ParentHash: a.Hash, Number: 819, Time: 9828}
	c := eth.L1BlockRef{Hash: common.Hash{0xc}, ParentHash: b.Hash, Number: 820, Time: 9840}
	parent := eth.L2BlockRef{Hash: common.Hash{1}, Number: 100, Time: 10000, L1Origin: a.ID()}
	child := eth.L2BlockRef{Hash: common.Hash{2}, ParentHash: parent.Hash, Number: 101, Time: 10010, L1Origin: b.ID()}

	l1.ExpectL1BlockRefByHash(a.Hash, a, nil)
	l1.ExpectL1BlockRefByNumber(b.Number, b, nil)
	require.True(t, selector.OnEvent(ctx, engine.ForkchoiceUpdateEvent{UnsafeL2Head: parent}))

	seq.unsafeHead = parent
	seq.active.Store(true)
	seq.nextActionArmed = true
	seq.timeNow = func() time.Time { return time.Unix(int64(child.Time), 0) }
	seq.building = BuildingState{Onto: parent, Info: eth.PayloadInfo{ID: eth.PayloadID{1}, Timestamp: child.Time}}
	envelope := &eth.ExecutionPayloadEnvelope{ExecutionPayload: &eth.ExecutionPayload{
		BlockHash: child.Hash, ParentHash: parent.Hash,
		BlockNumber: eth.Uint64Quantity(child.Number), Timestamp: eth.Uint64Quantity(child.Time),
	}}
	deps.eng.sealBuildFn = func(context.Context, eth.PayloadInfo, time.Time) (*engine.SealResult, error) {
		return &engine.SealResult{Envelope: envelope, Ref: child}, nil
	}
	deps.eng.processPayloadFn = func(context.Context, *eth.ExecutionPayloadEnvelope, eth.L2BlockRef, time.Time) error {
		// The engine has accepted the block, but its forkchoice event is queued.
		return nil
	}
	seq.RunAction()
	require.Equal(t, child, seq.unsafeHead)
	_, armed := seq.NextAction()
	require.True(t, armed)

	l1.ExpectL1BlockRefByNumber(c.Number, c, nil)
	var selected eth.BlockID
	deps.eng.startBuildFn = func(_ context.Context, attrs *derive.AttributesWithParent) (*engine.BuildStartResult, error) {
		selected = decodeID(attrs.Attributes.Transactions[0])
		return &engine.BuildStartResult{Parent: child, Info: eth.PayloadInfo{ID: eth.PayloadID{2}, Timestamp: 10020}}, nil
	}
	// Do not deliver a forkchoice update: the next confirmed origin must not
	// depend on shared event-loop timing, even below the maximum drift limit.
	seq.RunAction()
	require.Equal(t, c.ID(), selected)
}

type readinessSource struct {
	L1Blocks
	byNumber func(context.Context, uint64) (eth.L1BlockRef, error)
}

func (s *readinessSource) L1BlockRefByNumber(ctx context.Context, n uint64) (eth.L1BlockRef, error) {
	return s.byNumber(ctx, n)
}

func TestOriginReadinessLookupIsBounded(t *testing.T) {
	for _, mode := range []string{"default", "short-parent", "canceled-parent"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			if mode == "short-parent" {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, 20*time.Millisecond)
				t.Cleanup(cancel)
			} else if mode == "canceled-parent" {
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}
			a := eth.L1BlockRef{Hash: common.Hash{1}, Number: 10, Time: 20}
			head := eth.L2BlockRef{L1Origin: a.ID(), Time: 24}
			base := &testutils.MockL1Source{}
			base.ExpectL1BlockRefByHash(a.Hash, a, nil)
			t.Cleanup(func() { base.AssertExpectations(t) })
			called := false
			source := &readinessSource{L1Blocks: base, byNumber: func(fetchCtx context.Context, n uint64) (eth.L1BlockRef, error) {
				called = true
				require.Equal(t, a.Number+1, n)
				deadline, ok := fetchCtx.Deadline()
				require.True(t, ok, "an optional readiness lookup must have a deadline")
				require.LessOrEqual(t, time.Until(deadline), 500*time.Millisecond)
				if parentDeadline, ok := ctx.Deadline(); ok {
					require.Equal(t, parentDeadline, deadline, "do not extend the caller's earlier deadline")
				}
				if mode == "canceled-parent" {
					require.ErrorIs(t, fetchCtx.Err(), context.Canceled)
				}
				return eth.L1BlockRef{}, context.DeadlineExceeded
			}}
			selector := NewL1OriginSelector(ctx, testlog.Logger(t, log.LevelCrit), &rollup.Config{BlockTime: 10, MaxSequencerDrift: 500}, source)
			got, err := selector.FindL1Origin(ctx, head)
			require.NoError(t, err)
			require.Equal(t, a, got, "a timed-out optional lookup may keep the current valid origin")
			require.True(t, called)
		})
	}
}

func TestOriginReadinessPreservesAdoptionConstraints(t *testing.T) {
	for _, mode := range []string{"unconfirmed", "future", "orphan"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			a := eth.L1BlockRef{Hash: common.Hash{1}, Number: 10, Time: 20}
			b := eth.L1BlockRef{Hash: common.Hash{2}, ParentHash: a.Hash, Number: 11, Time: 22}
			head := eth.L2BlockRef{L1Origin: a.ID(), Time: 24}
			base := &testutils.MockL1Source{}
			t.Cleanup(func() { base.AssertExpectations(t) })
			base.ExpectL1BlockRefByHash(a.Hash, a, nil)
			l1Head := eth.L1BlockRef{Number: a.Number + 15}
			if mode != "unconfirmed" {
				l1Head.Number++
				if mode == "future" {
					b.Time = 100
				}
				if mode == "orphan" {
					b.ParentHash = common.Hash{3}
				}
				base.ExpectL1BlockRefByNumber(b.Number, b, nil)
			}
			source := confdepth.NewConfDepth(15, func() eth.L1BlockRef { return l1Head }, base)
			selector := NewL1OriginSelector(ctx, testlog.Logger(t, log.LevelCrit), &rollup.Config{BlockTime: 10, MaxSequencerDrift: 500}, source)
			got, err := selector.FindL1Origin(ctx, head)
			if mode == "orphan" {
				require.ErrorIs(t, err, ErrNextL1OriginOrphaned)
			} else {
				require.NoError(t, err)
				require.Equal(t, a, got)
			}
		})
	}
}

func TestOriginReadinessTracksConfirmedHeadWithoutEvents(t *testing.T) {
	for _, blockTime := range []uint64{2, 10} {
		t.Run((time.Duration(blockTime) * time.Second).String(), func(t *testing.T) {
			ctx := context.Background()
			block := func(n uint64) eth.L1BlockRef {
				return eth.L1BlockRef{Number: n, Time: n * 12,
					Hash:       common.BigToHash(new(big.Int).SetUint64(n)),
					ParentHash: common.BigToHash(new(big.Int).SetUint64(n - 1))}
			}
			head := eth.L2BlockRef{Number: 100, Time: 10000, L1Origin: block(10000/12 - 15).ID()}
			base := &testutils.MockL1Source{}
			base.ExpectL1BlockRefByHash(head.L1Origin.Hash, block(head.L1Origin.Number), nil)
			t.Cleanup(func() { base.AssertExpectations(t) })
			source := &readinessSource{L1Blocks: base, byNumber: func(_ context.Context, n uint64) (eth.L1BlockRef, error) {
				if n+15 > head.Time/12 {
					return eth.L1BlockRef{}, ethereum.NotFound
				}
				return block(n), nil
			}}
			selector := NewL1OriginSelector(ctx, testlog.Logger(t, log.LevelCrit), &rollup.Config{BlockTime: blockTime, MaxSequencerDrift: 1800}, source)
			for range 600 {
				origin, err := selector.FindL1Origin(ctx, head)
				require.NoError(t, err)
				head = eth.L2BlockRef{Number: head.Number + 1, Time: head.Time + blockTime, L1Origin: origin.ID()}
			}
			require.LessOrEqual(t, head.Time/12-head.L1Origin.Number, uint64(16))
		})
	}
}
