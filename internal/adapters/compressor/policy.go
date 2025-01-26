package compressor

import (
	"LogDb/internal/ports"
	"context"
	log "github.com/sirupsen/logrus"
	"time"
)

var _ ports.CompressionPolicy = (*IntervalCompressPolicy)(nil)

type IntervalCompressPolicy struct {
	interval time.Duration
	ctx      context.Context
}

// Apply applies the daily compress policy.
func (dcp *IntervalCompressPolicy) Apply(target ports.Compressible) {
	go dcp.execute(target)
}

// execute
func (dcp *IntervalCompressPolicy) execute(target ports.Compressible) {
	for {
		// Compress the data
		select {
		case <-dcp.ctx.Done():
			return
		default:
			<-time.After(dcp.interval)
		}
		err := target.Compress()
		if err != nil {
			log.Error(err)
		}
	}
}

// NewIntervalCompressPolicy creates a new daily compress policy.
func NewIntervalCompressPolicy(ctx context.Context, interval time.Duration) *IntervalCompressPolicy {
	return &IntervalCompressPolicy{
		interval: interval,
		ctx:      ctx,
	}
}
