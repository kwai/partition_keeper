package utils

import (
	"sync/atomic"
	"time"
)

type TokenBucket struct {
	remainTokens    int64
	refillGapMillis int64
	maxTokens       int64
	gapId           int64
}

func timeNowMillis() int64 {
	return time.Now().UnixNano() / 1000000
}

func NewTokenBucket(tokensPerSecond, gapsPerSecond int64) *TokenBucket {
	if gapsPerSecond <= 0 || gapsPerSecond > 100 {
		gapsPerSecond = 100
	}
	if tokensPerSecond < gapsPerSecond {
		gapsPerSecond = tokensPerSecond
	}

	maxTokens := (tokensPerSecond + gapsPerSecond - 1) / gapsPerSecond
	refillGapMillis := 1000 / gapsPerSecond
	return &TokenBucket{
		remainTokens:    maxTokens,
		refillGapMillis: refillGapMillis,
		maxTokens:       maxTokens,
		gapId:           timeNowMillis() / refillGapMillis,
	}
}

func (t *TokenBucket) AcquireToken() {
	for {
		nowMillis := timeNowMillis()
		currentId := nowMillis / t.refillGapMillis
		tokenId := atomic.LoadInt64(&t.gapId)
		if currentId == tokenId {
			if atomic.LoadInt64(&t.remainTokens) > 0 {
				ans := atomic.AddInt64(&t.remainTokens, -1)
				if ans >= 0 {
					return
				}
			}
			waitMillis := (currentId+1)*t.refillGapMillis - nowMillis
			time.Sleep(time.Millisecond * time.Duration(waitMillis))
		} else if currentId > tokenId {
			if atomic.CompareAndSwapInt64(&t.gapId, tokenId, currentId) {
				atomic.StoreInt64(&t.remainTokens, t.maxTokens)
			}
		} else {
			waitMillis := tokenId*t.refillGapMillis - nowMillis
			time.Sleep(time.Millisecond * time.Duration(waitMillis))
		}
	}
}
