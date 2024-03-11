package plasma

import (
	"context"
	"crypto/tls"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/Layr-Labs/eigenda/api/grpc/disperser"
	"github.com/ethereum-optimism/optimism/op-plasma/pb/ref"
	"github.com/ethereum/go-ethereum/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
)

type EigenDAConfig struct {
	EigenDARpc                      string        `env:"EIGEN_DA_RPC"`
	EigenDAQuorumID                 uint64        `env:"EIGEN_DA_QUORUM_ID"`
	EigenDAAdversaryThreshold       uint64        `env:"EIGEN_DA_ADVERSARY_THRESHOLD"`
	EigenDAQuorumThreshold          uint64        `env:"EIGEN_DA_QUORUM_THRESHOLD"`
	EigenDAStatusQueryRetryInterval time.Duration `env:"EIGEN_DA_STATUS_QUERY_RETRY_INTERVAL"`
	EigenDAStatusQueryTimeout       time.Duration `env:"EIGEN_DA_STATUS_QUERY_TIMEOUT"`
	EigenDAInsecure                 bool          `env:"EIGEN_DA_INSECURE"`
}

func (c EigenDAConfig) sanitize() error {
	if c.EigenDARpc == "" {
		return errors.New("invalid rpc endpoint")
	}
	if c.EigenDAAdversaryThreshold == 0 || c.EigenDAAdversaryThreshold >= 100 {
		return errors.New("invalid adversary threshold, must in range (0, 100)")
	}
	if c.EigenDAQuorumThreshold == 0 || c.EigenDAQuorumThreshold >= 100 {
		return errors.New("invalid quorum threshold, must in range (0, 100)")
	}
	if c.EigenDAStatusQueryTimeout == 0 {
		return errors.New("invalid status query timeout, must be greater than 0")
	}
	if c.EigenDAStatusQueryRetryInterval == 0 {
		return errors.New("invalid status query retry interval, must be greater than 0")
	}
	return nil
}

type EigenDA struct {
	client disperser.DisperserClient
	c      *EigenDAConfig
	log    log.Logger
}

func NewEigenDA(c *EigenDAConfig, log log.Logger) (*EigenDA, error) {
	if err := c.sanitize(); err != nil {
		return nil, err
	}
	var opts []grpc.DialOption
	if c.EigenDAInsecure {
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	} else {
		creds := credentials.NewTLS(&tls.Config{
			InsecureSkipVerify: true,
		})
		opts = append(opts, grpc.WithTransportCredentials(creds))
	}
	conn, err := grpc.Dial(c.EigenDARpc, opts...)
	if err != nil {
		return nil, err
	}
	client := disperser.NewDisperserClient(conn)
	return &EigenDA{
		client: client,
		c:      c,
		log:    log,
	}, nil
}

func (e *EigenDA) GetInput(ctx context.Context, key []byte) ([]byte, error) {
	var r ref.Ref
	if err := proto.Unmarshal(key, &r); err != nil {
		return nil, err
	}
	req := &disperser.RetrieveBlobRequest{
		BatchHeaderHash: r.GetEigenda().GetBatchHeaderHash(),
		BlobIndex:       r.GetEigenda().GetBlobIndex(),
	}

	e.log.Debug(
		"trying to get data from eigenda",
		"blobIndex", req.BlobIndex,
		"headerHash", hex.EncodeToString(req.BatchHeaderHash),
	)
	reply, err := e.client.RetrieveBlob(ctx, req)
	if err != nil {
		return nil, err
	}
	e.log.Debug(
		"successfully get data from eigenda",
		"blobIndex", req.BlobIndex,
		"headerHash", hex.EncodeToString(req.BatchHeaderHash),
	)
	return reply.GetData(), nil
}

func (e *EigenDA) SetInput(ctx context.Context, img []byte) ([]byte, error) {
	e.log.Debug("trying to put data to eigenda")
	disperseReq := &disperser.DisperseBlobRequest{
		Data: img,
		SecurityParams: []*disperser.SecurityParams{
			{
				QuorumId:           uint32(e.c.EigenDAQuorumID),
				AdversaryThreshold: uint32(e.c.EigenDAAdversaryThreshold),
				QuorumThreshold:    uint32(e.c.EigenDAQuorumThreshold),
			},
		},
	}
	disperseRes, err := e.client.DisperseBlob(ctx, disperseReq)
	if err != nil {
		return nil, err
	}
	base64RequestID := base64.StdEncoding.EncodeToString(disperseRes.RequestId)
	ticker := time.NewTicker(e.c.EigenDAStatusQueryRetryInterval)
	defer ticker.Stop()
	c, cancel := context.WithTimeout(ctx, e.c.EigenDAStatusQueryTimeout)
	defer cancel()
	for {
		select {
		case <-ticker.C:
			statusReply, err := e.client.GetBlobStatus(ctx, &disperser.BlobStatusRequest{
				RequestId: disperseRes.RequestId,
			})
			if err != nil {
				log.Warn("failed to get blob status from eigenda, will retry", "error", err)
				continue
			}
			switch statusReply.GetStatus() {
			case disperser.BlobStatus_CONFIRMED, disperser.BlobStatus_FINALIZED:
				log.Info(
					"successfully put data to eigenda",
					"requestID", base64RequestID,
					"blobIndex", statusReply.Info.BlobVerificationProof.BlobIndex,
					"headerHash", hex.EncodeToString(statusReply.Info.BlobVerificationProof.BatchMetadata.BatchHeaderHash),
				)
				r := &ref.Ref{
					Value: &ref.Ref_Eigenda{
						Eigenda: &ref.EigenDA{
							BatchHeaderHash: statusReply.Info.BlobVerificationProof.BatchMetadata.BatchHeaderHash,
							BlobIndex:       statusReply.Info.BlobVerificationProof.BlobIndex,
						},
					},
				}
				return proto.Marshal(r)
			case disperser.BlobStatus_FAILED:
				return nil, fmt.Errorf("blob dispersal failed with reply status %v", statusReply.Status)
			default:
				e.log.Info("waiting for blob dispersal to be confirmed", "requestID", base64RequestID)
				continue
			}
		case <-c.Done():
			return nil, fmt.Errorf("blob dispersal timed out requestID: %s", base64RequestID)
		}
	}
}
