package derive

import (
	"context"
	"encoding/hex"
	"fmt"

	"github.com/ethereum-optimism/optimism/op-service/da"
	"github.com/ethereum-optimism/optimism/op-service/da/pb/calldata"
	"github.com/ethereum-optimism/optimism/op-service/eth"
	"github.com/ethereum/go-ethereum/log"
	"google.golang.org/protobuf/proto"
)

type DaDataSource struct {
	log log.Logger
	src DataIter
	id  eth.BlockID
	// keep track of a pending commitment so we can keep trying to fetch the input.
	comm []byte
}

func NewDaDataSource(log log.Logger, src DataIter, id eth.BlockID) *DaDataSource {
	return &DaDataSource{
		log: log,
		src: src,
		id:  id,
	}
}

func (s *DaDataSource) Next(ctx context.Context) (eth.Data, error) {
	if s.comm == nil {
		var err error
		// the l1 source returns the input commitment for the batch.
		s.comm, err = s.src.Next(ctx)
		if err != nil {
			return nil, err
		}
	}

	var data eth.Data
	c := calldata.Calldata{}
	if err := proto.Unmarshal(s.comm, &c); err != nil {
		log.Warn("tx data is not a protobuf calldata, probably raw data", "txDataHex", hex.EncodeToString(s.comm), "block", s.id.Number)
		data = s.comm
	} else {
		data, err = da.Get(context.Background(), s.log, &c)
		if err != nil {
			log.Error("failed to retrieve data from DA", "err", err, "block", s.id.Number)
			return nil, NewTemporaryError(fmt.Errorf("failed to retrieve data from DA: %v", err))
		}
	}

	// reset the commitment so we can fetch the next one from the source at the next iteration.
	s.comm = nil
	return data, nil
}
