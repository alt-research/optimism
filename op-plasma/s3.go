package plasma

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"io"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/log"
)

type S3Config struct {
	Bucket   string
	Endpoint string
	Region   string
}

func (s S3Config) sanitize() error {
	if s.Bucket == "" {
		return errors.New("missing required s3 bucket config")
	}
	return nil
}

type S3 struct {
	client *awss3.Client
	c      *S3Config
	log    log.Logger
}

func NewS3(c *S3Config, log log.Logger) (*S3, error) {
	if err := c.sanitize(); err != nil {
		return nil, err
	}

	var opts []func(*config.LoadOptions) error

	customResolver := aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...interface{}) (aws.Endpoint, error) {
		if c.Endpoint != "" && c.Region != "" {
			return aws.Endpoint{
				PartitionID:   "aws",
				URL:           c.Endpoint,
				SigningRegion: c.Region,
			}, nil
		}
		// returning EndpointNotFoundError will allow the service to fallback to it's default resolution
		return aws.Endpoint{}, &aws.EndpointNotFoundError{}
	})

	opts = append(opts, config.WithEndpointResolverWithOptions(customResolver))

	if c.Region != "" {
		opts = append(opts, config.WithRegion(c.Region))
	}

	awsConf, err := config.LoadDefaultConfig(context.TODO(), opts...)
	if err != nil {
		return nil, err
	}

	client := awss3.NewFromConfig(awsConf, func(o *awss3.Options) {
		o.UsePathStyle = true
	})

	return &S3{
		client: client,
		c:      c,
		log:    log,
	}, nil
}
func (s *S3) GetInput(ctx context.Context, key []byte) ([]byte, error) {
	hkey := hex.EncodeToString(key)
	log.Debug(
		"trying to get data from s3",
		"key", hkey,
	)
	result, err := s.client.GetObject(ctx, &awss3.GetObjectInput{
		Bucket: aws.String(s.c.Bucket),
		Key:    aws.String(hkey),
	})
	if err != nil {
		return nil, err
	}
	defer result.Body.Close()
	v, err := io.ReadAll(result.Body)
	if err != nil {
		return nil, err
	}
	log.Debug(
		"successfully get data from s3",
		"key", hkey,
	)
	return v, nil
}

func (s *S3) SetInput(ctx context.Context, img []byte) ([]byte, error) {
	digest := crypto.Keccak256Hash(img).Bytes()
	key := hex.EncodeToString(digest)
	log.Debug("trying to put data to s3", "key", key)
	_, err := s.client.PutObject(ctx, &awss3.PutObjectInput{
		Bucket: aws.String(s.c.Bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(img),
	})
	if err != nil {
		return nil, err
	}
	log.Debug(
		"successfully put data to s3",
		"key", key,
	)
	return digest, nil
}
