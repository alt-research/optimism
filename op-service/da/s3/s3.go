package s3

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"io"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/ethereum-optimism/optimism/op-service/da/pb/calldata"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/log"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	client *awss3.Client
	conf   *S3Config

	metrics = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "da",
		Subsystem: "s3",
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

type S3Config struct {
	Enable   bool   `env:"S3_ENABLE"`
	Bucket   string `env:"AWS_S3_BUCKET"`
	Endpoint string `env:"AWS_ENDPOINT"`
	Region   string `env:"AWS_REGION"`
}

func (s S3Config) sanitize() error {
	if s.Bucket == "" {
		return errors.New("missing required env var AWS_S3_BUCKET")
	}
	return nil
}

func Init(c *S3Config) error {
	if err := c.sanitize(); err != nil {
		return err
	}
	conf = c

	var opts []func(*config.LoadOptions) error

	customResolver := aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...interface{}) (aws.Endpoint, error) {
		if conf.Endpoint != "" && conf.Region != "" {
			return aws.Endpoint{
				PartitionID:   "aws",
				URL:           conf.Endpoint,
				SigningRegion: conf.Region,
			}, nil
		}
		// returning EndpointNotFoundError will allow the service to fallback to it's default resolution
		return aws.Endpoint{}, &aws.EndpointNotFoundError{}
	})

	opts = append(opts, config.WithEndpointResolverWithOptions(customResolver))

	if conf.Region != "" {
		opts = append(opts, config.WithRegion(conf.Region))
	}

	awsConf, err := config.LoadDefaultConfig(context.TODO(), opts...)
	if err != nil {
		return err
	}

	client = awss3.NewFromConfig(awsConf, func(o *awss3.Options) {
		o.UsePathStyle = true
	})

	prometheus.MustRegister(metrics)
	return nil
}

func Put(ctx context.Context, log log.Logger, data []byte) (*calldata.Calldata, error) {
	log.Info("trying to put data to s3")
	digest := crypto.Keccak256Hash(data)
	key := hex.EncodeToString(digest.Bytes())

	_, err := client.PutObject(ctx, &awss3.PutObjectInput{
		Bucket: aws.String(conf.Bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(data),
	})
	if err != nil {
		metrics.WithLabelValues(kindPut, stateFailure).Inc()
		return nil, err
	}
	log.Info(
		"successfully put data to s3",
		"digest", key,
	)
	metrics.WithLabelValues(kindPut, stateSuccess).Inc()
	return &calldata.Calldata{
		Value: &calldata.Calldata_Digest{
			Digest: &calldata.Digest{
				Payload: digest.Bytes(),
			},
		},
	}, nil
}

func Get(ctx context.Context, log log.Logger, d *calldata.Digest) ([]byte, error) {
	log.Info(
		"trying to get data from s3",
		"digest", hex.EncodeToString(d.GetPayload()),
	)
	key := hex.EncodeToString(d.GetPayload())
	result, err := client.GetObject(ctx, &awss3.GetObjectInput{
		Bucket: aws.String(conf.Bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, err
	}
	defer result.Body.Close()
	v, err := io.ReadAll(result.Body)
	if err != nil {
		metrics.WithLabelValues(kindGet, stateFailure).Inc()
		return nil, err
	}
	log.Info(
		"successfully get data from s3",
		"digest", hex.EncodeToString(d.GetPayload()),
	)
	metrics.WithLabelValues(kindGet, stateSuccess).Inc()
	return v, nil
}