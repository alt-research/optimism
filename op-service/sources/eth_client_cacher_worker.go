package sources

import (
	"context"
	"os"
	"strconv"
	"time"

	layer1cacher "github.com/ethereum-optimism/optimism/op-node/rollup/derive/layer1-cacher"
	"github.com/ethereum/go-ethereum/log"
)

func TryBootWarmers(logger log.Logger, client *EthClient) {
	layer1cacher.CacheWarmCacheWorkMu.Lock()
	defer layer1cacher.CacheWarmCacheWorkMu.Unlock()

	if !layer1cacher.CacheWarmerHadBoot {
		warmWorkCount := 8
		{
			envVar := os.Getenv("WARM_WORK_COUNT")
			if len(envVar) != 0 {
				var err error
				warmWorkCount, err = strconv.Atoi(envVar)
				if err != nil {
					panic(err)
				}
			}
		}

		layer1cacher.CacheWarmerHadBoot = true
		startWarmer(logger, client, warmWorkCount)
	}
}

func startWarmer(logger log.Logger, client *EthClient, workCount int) {
	log.Info("Start warmer", "count", workCount)
	for i := 0; i < workCount; i++ {
		go (func(id int) {
			for {
				if !layer1cacher.IsHasWarmWork() {
					logger.Info("no works just sleep 1 second", "id", id)
					time.Sleep(time.Second)
					continue
				}

				blockNum := layer1cacher.GetWarmWork(logger)
				if blockNum == 0 {
					continue
				}

				if _, ok := client.headersCacheByNumber.Get(blockNum); ok {
					if _, ok := client.transactionsCacheByNumber.Get(blockNum); ok {
						continue
					}
				}

				if blockNum%100 == 0 {
					logger.Info("got work", "blockNum", blockNum, "worker", id)
				}
				client.headerCall(context.TODO(), "eth_getBlockByNumber", numberID(blockNum))
			}
		})(i)
	}
}
