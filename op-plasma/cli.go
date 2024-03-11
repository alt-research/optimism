package plasma

import (
	"fmt"
	"net/url"

	"github.com/ethereum/go-ethereum/log"
	"github.com/urfave/cli/v2"
)

const (
	EnabledFlagName   = "plasma.enabled"
	DABackendFlagName = "plasma.da-backend"
	// da server flag names
	DaServerAddressFlagName = "plasma.da-server"
	VerifyOnReadFlagName    = "plasma.verify-on-read"
	// eigenda flag names
	EigenDARPCFlagName                       = "plasma.eigenda-rpc"
	EigenDAPrimaryQuorumIDFlagName           = "plasma.eigenda-primary-quorum-id"
	EigenDAPrimaryAdversaryThresholdFlagName = "plasma.eigenda-primary-adversary-threshold"
	EigenDAPrimaryQuorumThresholdFlagName    = "plasma.eigenda-primary-quorum-threshold"
	EigenDAStatusQueryRetryIntervalFlagName  = "plasma.eigenda-status-query-retry-interval"
	EigenDAStatusQueryTimeoutFlagName        = "plasma.eigenda-status-query-timeout"
	EigenDAInsecure                          = "plasma.eigenda-insecure"
	// s3 flag names
	S3BucketFlagName   = "plasma.s3-bucket"
	S3RegionFlagName   = "plasma.s3-region"
	S3EndpointFlagName = "plasma.s3-endpoint"
	// celestia flag names
	CelestiaRPCFlagName       = "plasma.celestia-rpc"
	CelestiaNamespaceFlagName = "plasma.celestia-namespace"
	CelestiaTimeoutFlagName   = "plasma.celestia-timeout"
	// avail flag names
	AvailSeedFlagName         = "plasma.avail-seed"
	AvailApiURLFlagName       = "plasma.avail-api-url"
	AvailAppIDFlagName        = "plasma.avail-app-id"
	AvailWriteTimeoutFlagName = "plasma.avail-write-timeout"
)

func plasmaEnv(envprefix, v string) []string {
	return []string{envprefix + "_PLASMA_" + v}
}

func CLIFlags(envPrefix string, category string) []cli.Flag {
	return []cli.Flag{
		&cli.BoolFlag{
			Name:     EnabledFlagName,
			Usage:    "Enable plasma mode",
			Value:    false,
			EnvVars:  plasmaEnv(envPrefix, "ENABLED"),
			Category: category,
		},
		&cli.StringFlag{
			Name:     DABackendFlagName,
			Usage:    "DA backend",
			EnvVars:  plasmaEnv(envPrefix, "DA_BACKEND"),
			Category: category,
		},
		// flag for da server
		&cli.StringFlag{
			Name:     DaServerAddressFlagName,
			Usage:    "HTTP address of a DA Server",
			EnvVars:  plasmaEnv(envPrefix, "DA_SERVER"),
			Category: category,
		},
		&cli.BoolFlag{
			Name:     VerifyOnReadFlagName,
			Usage:    "Verify input data matches the commitments from the DA storage service",
			Value:    true,
			EnvVars:  plasmaEnv(envPrefix, "VERIFY_ON_READ"),
			Category: category,
		},
		// flag for eigenda
		&cli.StringFlag{
			Name:     EigenDAInsecure,
			Usage:    "eigenda insecure",
			EnvVars:  plasmaEnv(envPrefix, "EIGENDA_INSECURE"),
			Category: category,
		},
		&cli.StringFlag{
			Name:     EigenDARPCFlagName,
			Usage:    "eigenda rpc endpoint",
			EnvVars:  plasmaEnv(envPrefix, "EIGENDA_RPC"),
			Category: category,
		},
		&cli.Uint64Flag{
			Name:     EigenDAPrimaryQuorumIDFlagName,
			Usage:    "eigenda primary quorum id",
			EnvVars:  plasmaEnv(envPrefix, "EIGENDA_PRIMARY_QUORUM_ID"),
			Category: category,
		},
		&cli.Uint64Flag{
			Name:     EigenDAPrimaryAdversaryThresholdFlagName,
			Usage:    "eigenda primary adversary threshold",
			EnvVars:  plasmaEnv(envPrefix, "EIGENDA_PRIMARY_ADVERSARY_THRESHOLD"),
			Category: category,
		},
		&cli.Uint64Flag{
			Name:     EigenDAPrimaryQuorumThresholdFlagName,
			Usage:    "eigenda primary quorum threshold",
			EnvVars:  plasmaEnv(envPrefix, "EIGENDA_PRIMARY_QUORUM_THRESHOLD"),
			Category: category,
		},
		&cli.DurationFlag{
			Name:     EigenDAStatusQueryTimeoutFlagName,
			Usage:    "eigenda status query timeout",
			EnvVars:  plasmaEnv(envPrefix, "EIGENDA_STATUS_QUERY_TIMEOUT"),
			Category: category,
		},
		&cli.DurationFlag{
			Name:     EigenDAStatusQueryRetryIntervalFlagName,
			Usage:    "eigenda status query retry interval",
			EnvVars:  plasmaEnv(envPrefix, "EIGENDA_STATUS_QUERY_RETRY_INTERVAL"),
			Category: category,
		},
		// s3 flag names
		&cli.StringFlag{
			Name:     S3BucketFlagName,
			Usage:    "s3 bucket",
			EnvVars:  plasmaEnv(envPrefix, "S3_BUCKET"),
			Category: category,
		},
		&cli.StringFlag{
			Name:     S3RegionFlagName,
			Usage:    "s3 region",
			EnvVars:  plasmaEnv(envPrefix, "S3_REGION"),
			Category: category,
		},
		&cli.StringFlag{
			Name:     S3EndpointFlagName,
			Usage:    "s3 endpoint",
			EnvVars:  plasmaEnv(envPrefix, "S3_ENDPOINT"),
			Category: category,
		},
		// celestia flag names
		&cli.StringFlag{
			Name:     CelestiaRPCFlagName,
			Usage:    "celestia rpc",
			EnvVars:  plasmaEnv(envPrefix, "CELESTIA_RPC"),
			Category: category,
		},
		&cli.StringFlag{
			Name:     CelestiaNamespaceFlagName,
			Usage:    "celestia namespace",
			EnvVars:  plasmaEnv(envPrefix, "CELESTIA_NAMESPACE"),
			Category: category,
		},
		&cli.DurationFlag{
			Name:     CelestiaTimeoutFlagName,
			Usage:    "celestia timeout",
			EnvVars:  plasmaEnv(envPrefix, "CELESTIA_TIMEOUT"),
			Category: category,
		},
		// avail flag names
		&cli.StringFlag{
			Name:     AvailSeedFlagName,
			Usage:    "avail seed",
			EnvVars:  plasmaEnv(envPrefix, "AVAIL_SEED"),
			Category: category,
		},
		&cli.StringFlag{
			Name:     AvailApiURLFlagName,
			Usage:    "avail api url",
			EnvVars:  plasmaEnv(envPrefix, "AVAIL_APIURL"),
			Category: category,
		},
		&cli.IntFlag{
			Name:     AvailAppIDFlagName,
			Usage:    "avail app id",
			EnvVars:  plasmaEnv(envPrefix, "AVAIL_APPID"),
			Category: category,
		},
		&cli.DurationFlag{
			Name:     AvailWriteTimeoutFlagName,
			Usage:    "avail write timeout",
			EnvVars:  plasmaEnv(envPrefix, "AVAIL_WRITETIMEOUT"),
			Category: category,
		},
	}
}

type CLIConfig struct {
	Enabled      bool
	DAServerURL  string
	VerifyOnRead bool
	DABackend    string
	S3Config
	EigenDAConfig
	AvailConfig
	CelestiaConfig
}

func (c CLIConfig) Check() error {
	if !c.Enabled {
		return nil
	}
	switch c.DABackend {
	case "s3":
		return c.S3Config.sanitize()
	case "eigenda":
		return c.EigenDAConfig.sanitize()
	case "avail":
		return c.AvailConfig.sanitize()
	case "celestia":
		return c.CelestiaConfig.sanitize()
	default:
		if c.DAServerURL == "" {
			return fmt.Errorf("DA server URL is required when plasma da is enabled")
		}
		if _, err := url.Parse(c.DAServerURL); err != nil {
			return fmt.Errorf("DA server URL is invalid: %w", err)
		}
	}
	return nil
}

func (c CLIConfig) NewDAClient(log log.Logger) (DAStorage, error) {
	switch c.DABackend {
	case "s3":
		return NewS3(&c.S3Config, log)
	case "eigenda":
		return NewEigenDA(&c.EigenDAConfig, log)
	case "avail":
		return NewAvail(&c.AvailConfig, log)
	case "celestia":
		return NewCelestia(&c.CelestiaConfig, log)
	default:
		return &DAClient{url: c.DAServerURL, verify: c.VerifyOnRead}, nil
	}
}

func ReadCLIConfig(c *cli.Context) CLIConfig {
	return CLIConfig{
		Enabled:      c.Bool(EnabledFlagName),
		DAServerURL:  c.String(DaServerAddressFlagName),
		VerifyOnRead: c.Bool(VerifyOnReadFlagName),
		DABackend:    c.String(DABackendFlagName),
		EigenDAConfig: EigenDAConfig{
			EigenDARpc:                      c.String(EigenDARPCFlagName),
			EigenDAQuorumID:                 c.Uint64(EigenDAPrimaryQuorumIDFlagName),
			EigenDAAdversaryThreshold:       c.Uint64(EigenDAPrimaryAdversaryThresholdFlagName),
			EigenDAQuorumThreshold:          c.Uint64(EigenDAPrimaryQuorumThresholdFlagName),
			EigenDAStatusQueryRetryInterval: c.Duration(EigenDAStatusQueryRetryIntervalFlagName),
			EigenDAStatusQueryTimeout:       c.Duration(EigenDAStatusQueryTimeoutFlagName),
			EigenDAInsecure:                 c.Bool(EigenDAInsecure),
		},
		S3Config: S3Config{
			Bucket:   c.String(S3BucketFlagName),
			Endpoint: c.String(S3EndpointFlagName),
			Region:   c.String(S3RegionFlagName),
		},
		CelestiaConfig: CelestiaConfig{
			RPC:       c.String(CelestiaRPCFlagName),
			Namespace: c.String(CelestiaNamespaceFlagName),
			Timeout:   c.Duration(CelestiaTimeoutFlagName),
		},
		AvailConfig: AvailConfig{
			Seed:         c.String(AvailSeedFlagName),
			ApiURL:       c.String(AvailApiURLFlagName),
			AppID:        c.Int(AvailAppIDFlagName),
			WriteTimeout: c.Duration(AvailWriteTimeoutFlagName),
		},
	}
}
