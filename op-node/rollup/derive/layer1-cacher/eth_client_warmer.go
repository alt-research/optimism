package layer1cacher

import (
	"sync"

	"github.com/ethereum/go-ethereum/log"
)

var (
	CacheWarmCacheWorkMu sync.Mutex
	CacheWarmCacheWorks  []uint64
	CacheWarmerHadBoot   bool
	// if not means current is insert block to geth
	// so we can warm up the cache now
	isCurrentIsFetchingBatch bool
)

const (
	EachBatchWorkCount = 2048
)

func init() {
	CacheWarmCacheWorks = make([]uint64, 0, 8*1024)
	CacheWarmerHadBoot = false
	isCurrentIsFetchingBatch = true
}

func TryUpdateStateWhenFetchingBatch(logger log.Logger, currentL1BlockNum uint64) {
	CacheWarmCacheWorkMu.Lock()
	defer CacheWarmCacheWorkMu.Unlock()

	// update works, if useless, just update for future
	if len(CacheWarmCacheWorks) > 0 {
		if currentL1BlockNum >= CacheWarmCacheWorks[0] {
			setWarmWorkImpl(logger, currentL1BlockNum+256, EachBatchWorkCount)
		}
	}

	if !isCurrentIsFetchingBatch {
		// Now is enter the fetching batch step, so we just need fetch more
		logger.Info("Enter fetching Batch Step", "l1Block", currentL1BlockNum)
		isCurrentIsFetchingBatch = true
	}
}

func TryUpdateStateWhenInsertToGeth(logger log.Logger, currentL1BlockNum uint64) {
	CacheWarmCacheWorkMu.Lock()
	defer CacheWarmCacheWorkMu.Unlock()

	if isCurrentIsFetchingBatch {
		logger.Info("Enter inserting geth Step", "l1Block", currentL1BlockNum)
		isCurrentIsFetchingBatch = false

		// update works, for futures
		setWarmWorkImpl(logger, currentL1BlockNum+256, EachBatchWorkCount)
	} else {
		// update works, just update for futures, it will be next 512
		if len(CacheWarmCacheWorks) > 0 {
			if currentL1BlockNum >= CacheWarmCacheWorks[0] {
				setWarmWorkImpl(logger, currentL1BlockNum, EachBatchWorkCount)
			}
		}
	}
}

func IsHasWarmWork() bool {
	CacheWarmCacheWorkMu.Lock()
	defer CacheWarmCacheWorkMu.Unlock()

	return len(CacheWarmCacheWorks) > 0
}

func GetWarmWork(logger log.Logger) uint64 {
	CacheWarmCacheWorkMu.Lock()
	defer CacheWarmCacheWorkMu.Unlock()

	blockNum := uint64(0)

	if len(CacheWarmCacheWorks) > 0 {
		blockNum = CacheWarmCacheWorks[0]
		CacheWarmCacheWorks = CacheWarmCacheWorks[1:]

		// auto fill next when free
		if len(CacheWarmCacheWorks) == 0 {
			logger.Info("append warm work by free", "start", blockNum)
			for i := 0; i < 128; i++ {
				CacheWarmCacheWorks = append(CacheWarmCacheWorks, blockNum+uint64(i))
			}
		}
	}

	return blockNum
}

func SetWarmWork(logger log.Logger, begin uint64, count uint64) {
	CacheWarmCacheWorkMu.Lock()
	defer CacheWarmCacheWorkMu.Unlock()

	setWarmWorkImpl(logger, begin, count)
}

func setWarmWorkImpl(logger log.Logger, begin uint64, count uint64) {
	logger.Info("set warm work", "begin", begin, "count", count)

	CacheWarmCacheWorks = make([]uint64, 0, 8*1024)
	for i := uint64(0); i < count; i++ {
		CacheWarmCacheWorks = append(CacheWarmCacheWorks, begin+i)
	}
}

func AppendWarmWork(logger log.Logger, begin uint64, count uint64) {
	CacheWarmCacheWorkMu.Lock()
	defer CacheWarmCacheWorkMu.Unlock()

	logger.Info("append warm work", "begin", begin, "count", count)

	for i := uint64(0); i < count; i++ {
		CacheWarmCacheWorks = append(CacheWarmCacheWorks, begin+i)
	}
}
