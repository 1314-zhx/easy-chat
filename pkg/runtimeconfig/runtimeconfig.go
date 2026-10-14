package runtimeconfig

import (
	"sync/atomic"
	"time"
)

type HotConfig struct {
	GroupMsgReadDelayTime  atomic.Int64
	GroupMsgReadDelayCount atomic.Int64
	RAGTopK                atomic.Int64
	RAGSemanticLimit       atomic.Int64
	RAGRecentHistory       atomic.Int64
	RAGSimilarityThreshold atomic.Uint64
	QueryRewriteEnabled    atomic.Bool
	MemoryExtractEnabled   atomic.Bool
}

func (c *HotConfig) SetDuration(d time.Duration) {
	c.GroupMsgReadDelayTime.Store(int64(d))
}

func (c *HotConfig) GetDuration() time.Duration {
	return time.Duration(c.GroupMsgReadDelayTime.Load())
}
