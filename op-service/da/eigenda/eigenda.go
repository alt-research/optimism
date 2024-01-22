package eigenda

import (
	"context"
	"crypto/tls"
	"encoding/base64"
	"errors"
	"fmt"
	"time"

	"github.com/Layr-Labs/eigenda/api/grpc/disperser"
	"github.com/ethereum-optimism/optimism/op-service/pb/calldata"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

var (
	client disperser.DisperserClient
	conf   *EigenDAConfig

	metrics = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "da",
		Subsystem: "eigenda",
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

type EigenDAConfig struct {
	EigenDAEnable                   bool          `env:"EIGEN_DA_ENABLE"`
	EigenDARpc                      string        `env:"EIGEN_DA_RPC"`
	EigenDAQuorumID                 uint32        `env:"EIGEN_DA_QUORUM_ID"`
	EigenDAAdversaryThreshold       uint32        `env:"EIGEN_DA_ADVERSARY_THRESHOLD"`
	EigenDAQuorumThreshold          uint32        `env:"EIGEN_DA_QUORUM_THRESHOLD"`
	EigenDAStatusQueryRetryInterval time.Duration `env:"EIGEN_DA_STATUS_QUERY_RETRY_INTERVAL"`
	EigenDAStatusQueryTimeout       time.Duration `env:"EIGEN_DA_STATUS_QUERY_TIMEOUT"`
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

func Init(c *EigenDAConfig) error {
	if err := c.sanitize(); err != nil {
		return err
	}
	conf = c

	creds := credentials.NewTLS(&tls.Config{
		InsecureSkipVerify: true,
	})
	conn, err := grpc.Dial(conf.EigenDARpc, grpc.WithTransportCredentials(creds))
	if err != nil {
		return err
	}
	client = disperser.NewDisperserClient(conn)
	return nil
}

func Get(ctx context.Context, ref *calldata.EigenDARef) ([]byte, error) {
	reply, err := client.RetrieveBlob(ctx, &disperser.RetrieveBlobRequest{
		BatchHeaderHash: ref.GetBatchHeaderHash(),
		BlobIndex:       ref.GetBlobIndex(),
	})
	if err != nil {
		metrics.WithLabelValues(kindGet, stateFailure).Inc()
		return nil, err
	}
	metrics.WithLabelValues(kindPut, stateSuccess).Inc()
	return reply.Data, nil
}

func Put(ctx context.Context, data []byte) (*calldata.Calldata, error) {
	disperseReq := &disperser.DisperseBlobRequest{
		Data: data,
		SecurityParams: []*disperser.SecurityParams{
			{
				QuorumId:           conf.EigenDAQuorumID,
				AdversaryThreshold: conf.EigenDAAdversaryThreshold,
				QuorumThreshold:    conf.EigenDAQuorumThreshold,
			},
		},
	}
	disperseRes, err := client.DisperseBlob(ctx, disperseReq)
	if err != nil {
		return nil, err
	}
	base64RequestID := base64.StdEncoding.EncodeToString(disperseRes.RequestId)
	ticker := time.NewTicker(conf.EigenDAStatusQueryRetryInterval)
	defer ticker.Stop()
	c, cancel := context.WithTimeout(ctx, conf.EigenDAStatusQueryTimeout)
	defer cancel()
	for {
		select {
		case <-ticker.C:
			statusReply, err := client.GetBlobStatus(ctx, &disperser.BlobStatusRequest{
				RequestId: disperseRes.RequestId,
			})
			if err != nil {
				continue
			}
			switch statusReply.GetStatus() {
			case disperser.BlobStatus_CONFIRMED, disperser.BlobStatus_FINALIZED:
				metrics.WithLabelValues(kindPut, stateSuccess).Inc()
				return &calldata.Calldata{
					Value: &calldata.Calldata_EigendaRef{
						EigendaRef: &calldata.EigenDARef{
							BatchHeaderHash: statusReply.Info.BlobVerificationProof.BatchMetadata.BatchHeaderHash,
							BlobIndex:       statusReply.Info.BlobVerificationProof.BlobIndex,
						},
					},
				}, nil
			case disperser.BlobStatus_FAILED:
				metrics.WithLabelValues(kindPut, stateFailure).Inc()
				return nil, fmt.Errorf("blob dispersal failed with reply status %v", statusReply.Status)
			default:
				continue
			}
		case <-c.Done():
			metrics.WithLabelValues(kindPut, stateFailure).Inc()
			return nil, fmt.Errorf("blob dispersal timed out requestID: %s", base64RequestID)
		}
	}
}
