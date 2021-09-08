// The MIT License (MIT)
//
// Copyright (c) 2017-2020 Uber Technologies Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

package shardscanner

import (
	"context"
	"fmt"

	"github.com/pborman/uuid"

	"github.com/uber/cadence/common/blobstore"
	"github.com/uber/cadence/common/cache"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/pagination"
	"github.com/uber/cadence/common/reconciliation/entity"
	"github.com/uber/cadence/common/reconciliation/invariant"
	"github.com/uber/cadence/common/reconciliation/store"
)

// Scanner is used to scan over given iterator. It is responsible for three things:
// 1. Checking invariants for each entity.
// 2. Recording corruption and failures to durable store.
// 3. Producing a ScanReport
type Scanner interface {
	Scan() ScanReport
}

type (
	// ShardScanner is a generic scanner which iterates over entities provided by iterator
	// implementations of this scanner have to provided invariant manager and iterator
	ShardScanner struct {
		shardID          int
		itr              pagination.Iterator
		failedWriter     store.ExecutionWriter
		corruptedWriter  store.ExecutionWriter
		invariantManager invariant.Manager
		progressReportFn func()
		scope            metrics.Scope
		domainCache      cache.DomainCache
	}
)

// NewScanner constructs a new ShardScanner
func NewScanner(
	shardID int,
	iterator pagination.Iterator,
	blobstoreClient blobstore.Client,
	blobstoreFlushThreshold int,
	manager invariant.Manager,
	progressReportFn func(),
	scope metrics.Scope,
	domainCache cache.DomainCache,

) *ShardScanner {
	id := uuid.New()

	return &ShardScanner{
		shardID:          shardID,
		itr:              iterator,
		failedWriter:     store.NewBlobstoreWriter(id, store.FailedExtension, blobstoreClient, blobstoreFlushThreshold),
		corruptedWriter:  store.NewBlobstoreWriter(id, store.CorruptedExtension, blobstoreClient, blobstoreFlushThreshold),
		invariantManager: manager,
		progressReportFn: progressReportFn,
		scope:            scope,
		domainCache:      domainCache,
	}
}

// Scan scans over all executions in shard and runs invariant checks per execution.
func (s *ShardScanner) Scan(ctx context.Context) ScanReport {
	result := ScanReport{
		ShardID: s.shardID,
	}
	for s.itr.HasNext() {
		s.progressReportFn()
		execution, err := s.itr.Next()
		if err != nil {
			result.Result.ControlFlowFailure = &ControlFlowFailure{
				Info:        "persistence iterator returned error",
				InfoDetails: err.Error(),
			}
			return result
		}

		domainID := execution.(entity.Entity).GetDomainID()
		domainName, err := s.domainCache.GetDomainName(domainID)
		if err != nil {
			result.Result.ControlFlowFailure = &ControlFlowFailure{
				Info:        "failed to get domain name",
				InfoDetails: err.Error(),
			}
			return result
		}

		checkResult := s.invariantManager.RunChecks(ctx, execution)

		s.scope.Tagged(
			metrics.DomainTag(domainName),
			metrics.InvariantTypeTag(string(*checkResult.DeterminingInvariantType)),
			metrics.ShardscannerScanResult(string(checkResult.CheckResultType)),
		).IncCounter(1)

		switch checkResult.CheckResultType {
		case invariant.CheckResultTypeHealthy:
			// do nothing if execution is healthy
		case invariant.CheckResultTypeCorrupted:
			if err := s.corruptedWriter.Add(store.ScanOutputEntity{
				Execution: execution,
				Result:    checkResult,
			}); err != nil {
				result.Result.ControlFlowFailure = &ControlFlowFailure{
					Info:        "blobstore add failed for corrupted execution check",
					InfoDetails: err.Error(),
				}
				return result
			}
		case invariant.CheckResultTypeFailed:
			if err := s.failedWriter.Add(store.ScanOutputEntity{
				Execution: execution,
				Result:    checkResult,
			}); err != nil {
				result.Result.ControlFlowFailure = &ControlFlowFailure{
					Info:        "blobstore add failed for failed execution check",
					InfoDetails: err.Error(),
				}
				return result
			}
		default:
			panic(fmt.Sprintf("unknown CheckResultType: %v", checkResult.CheckResultType))
		}
	}

	if err := s.failedWriter.Flush(); err != nil {
		result.Result.ControlFlowFailure = &ControlFlowFailure{
			Info:        "failed to flush for failed execution checks",
			InfoDetails: err.Error(),
		}
		return result
	}
	if err := s.corruptedWriter.Flush(); err != nil {
		result.Result.ControlFlowFailure = &ControlFlowFailure{
			Info:        "failed to flush for corrupted execution checks",
			InfoDetails: err.Error(),
		}
		return result
	}

	result.Result.ShardScanKeys = &ScanKeys{
		Corrupt: s.corruptedWriter.FlushedKeys(),
		Failed:  s.failedWriter.FlushedKeys(),
	}
	return result
}
