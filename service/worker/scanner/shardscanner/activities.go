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
	"encoding/json"
	"errors"

	"go.uber.org/cadence"
	"go.uber.org/cadence/.gen/go/shared"
	"go.uber.org/cadence/activity"

	c "github.com/uber/cadence/common"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/reconciliation/store"
)

const (
	// ActivityScannerEmitMetrics is the activity name for ScannerEmitMetricsActivity
	ActivityScannerEmitMetrics = "cadence-sys-executions-shardscanner-emit-metrics-activity"
	// ActivityScannerConfig is the activity name ScannerConfigActivity
	ActivityScannerConfig = "cadence-sys-executions-shardscanner-config-activity"
	// ActivityScanShard is the activity name for ScanShardActivity
	ActivityScanShard = "cadence-sys-executions-shardscanner-scan-shard-activity"
	// ActivityFixerCorruptedKeys is the activity name for FixerCorruptedKeysActivity
	ActivityFixerCorruptedKeys = "cadence-sys-executions-shardfixer-corrupted-keys-activity"
	// ActivityFixShard is the activity name for FixShardActivity
	ActivityFixShard = "cadence-sys-executions-shardfixer-fix-shard-activity"
	// ShardCorruptKeysQuery is the query name for the query used to get all completed shards with at least one corruption
	ShardCorruptKeysQuery = "shard_corrupt_keys"
)

// ScannerConfigActivity will read dynamic config, apply overwrites and return a resolved config.
func ScannerConfigActivity(
	activityCtx context.Context,
	params ScannerConfigActivityParams,
) (ResolvedScannerWorkflowConfig, error) {
	ctx := activityCtx.Value(params.ContextKey).(Context)
	dc := ctx.Config.DynamicParams

	result := ResolvedScannerWorkflowConfig{
		Enabled:                 dc.Enabled(),
		Concurrency:             dc.Concurrency(),
		PageSize:                dc.PageSize(),
		BlobstoreFlushThreshold: dc.BlobstoreFlushThreshold(),
		ActivityBatchSize:       dc.ActivityBatchSize(),
	}

	if ctx.Hooks != nil && ctx.Hooks.Config != nil {
		result.CustomScannerConfig = ctx.Hooks.Config(ctx)
	}

	overwrites := params.Overwrites
	if overwrites.Enabled != nil {
		result.Enabled = *overwrites.Enabled
	}
	if overwrites.Concurrency != nil {
		result.Concurrency = *overwrites.Concurrency
	}
	if overwrites.PageSize != nil {
		result.PageSize = *overwrites.PageSize
	}
	if overwrites.BlobstoreFlushThreshold != nil {
		result.BlobstoreFlushThreshold = *overwrites.BlobstoreFlushThreshold
	}

	if overwrites.ActivityBatchSize != nil {
		result.ActivityBatchSize = *overwrites.ActivityBatchSize
	}

	if overwrites.CustomScannerConfig != nil {
		result.CustomScannerConfig = *overwrites.CustomScannerConfig
	}

	return result, nil
}

// ScanShardActivity will scan a collection of shards for invariant violations.
func ScanShardActivity(
	activityCtx context.Context,
	params ScanShardActivityParams,
) ([]ScanReport, error) {
	heartbeatDetails := ScanShardHeartbeatDetails{
		LastShardIndexHandled: -1,
		Reports:               nil,
	}
	if activity.HasHeartbeatDetails(activityCtx) {
		if err := activity.GetHeartbeatDetails(activityCtx, &heartbeatDetails); err != nil {
			return nil, err
		}
	}
	for i := heartbeatDetails.LastShardIndexHandled + 1; i < len(params.Shards); i++ {
		currentShardID := params.Shards[i]
		shardReport, err := scanShard(activityCtx, params, currentShardID, heartbeatDetails)
		if err != nil {
			return nil, err
		}
		heartbeatDetails = ScanShardHeartbeatDetails{
			LastShardIndexHandled: i,
			Reports:               append(heartbeatDetails.Reports, *shardReport),
		}
	}
	return heartbeatDetails.Reports, nil
}

func scanShard(
	activityCtx context.Context,
	params ScanShardActivityParams,
	shardID int,
	heartbeatDetails ScanShardHeartbeatDetails,
) (*ScanReport, error) {
	ctx := activityCtx.Value(params.ContextKey).(Context)
	scope := ctx.Scope.Tagged(metrics.ActivityTypeTag(params.ContextKey.String()))
	sw := scope.StartTimer(metrics.CadenceLatency)
	defer sw.Stop()

	if ctx.Hooks == nil {
		return nil, errors.New("scanner hooks are not provided")
	}

	resources := ctx.Resource
	execManager, err := resources.GetExecutionManager(shardID)
	if err != nil {
		scope.IncCounter(metrics.CadenceFailures)
		return nil, err
	}

	pr := persistence.NewPersistenceRetryer(execManager, resources.GetHistoryManager(), c.CreatePersistenceRetryPolicy())

	scanner := NewScanner(
		shardID,
		ctx.Hooks.Iterator(pr, params, *ctx.Config),
		resources.GetBlobstoreClient(),
		params.BlobstoreFlushThreshold,
		ctx.Hooks.Manager(pr, params, *ctx.Config),
		func() { activity.RecordHeartbeat(activityCtx, heartbeatDetails) },
	)
	report := scanner.Scan()
	if report.Result.ControlFlowFailure != nil {
		scope.IncCounter(metrics.CadenceFailures)
	}
	return &report, nil
}

// FixerCorruptedKeysActivity will fetch the keys of blobs from shards with corruptions from a completed scan workflow.
// If scan workflow is not closed or if query fails activity will return an error.
// Accepts as input the shard to start query at and returns a next page token, therefore this activity can
// be used to do pagination.
func FixerCorruptedKeysActivity(
	activityCtx context.Context,
	params FixerCorruptedKeysActivityParams,
) (*FixerCorruptedKeysActivityResult, error) {
	resource := activityCtx.Value(params.ContextKey).(FixerContext).Resource
	client := resource.GetSDKClient()
	descResp, err := client.DescribeWorkflowExecution(activityCtx, &shared.DescribeWorkflowExecutionRequest{
		Domain: c.StringPtr(c.SystemLocalDomainName),
		Execution: &shared.WorkflowExecution{
			WorkflowId: c.StringPtr(params.ScannerWorkflowWorkflowID),
			RunId:      c.StringPtr(params.ScannerWorkflowRunID),
		},
	})
	if err != nil {
		return nil, err
	}
	if descResp.WorkflowExecutionInfo.CloseStatus == nil {
		return nil, cadence.NewCustomError(ErrScanWorkflowNotClosed)
	}
	queryArgs := PaginatedShardQueryRequest{
		StartingShardID: params.StartingShardID,
	}
	queryArgsBytes, err := json.Marshal(queryArgs)
	if err != nil {
		return nil, cadence.NewCustomError(ErrSerialization)
	}
	queryResp, err := client.QueryWorkflow(activityCtx, &shared.QueryWorkflowRequest{
		Domain: c.StringPtr(c.SystemLocalDomainName),
		Execution: &shared.WorkflowExecution{
			WorkflowId: c.StringPtr(params.ScannerWorkflowWorkflowID),
			RunId:      c.StringPtr(params.ScannerWorkflowRunID),
		},
		Query: &shared.WorkflowQuery{
			QueryType: c.StringPtr(ShardCorruptKeysQuery),
			QueryArgs: queryArgsBytes,
		},
	})
	if err != nil {
		return nil, err
	}
	queryResult := &ShardCorruptKeysQueryResult{}
	if err := json.Unmarshal(queryResp.QueryResult, &queryResult); err != nil {
		return nil, cadence.NewCustomError(ErrSerialization)
	}
	var corrupted []CorruptedKeysEntry
	var minShardID *int
	var maxShardID *int
	for sid, keys := range queryResult.Result {
		if minShardID == nil || *minShardID > sid {
			minShardID = c.IntPtr(sid)
		}
		if maxShardID == nil || *maxShardID < sid {
			maxShardID = c.IntPtr(sid)
		}
		corrupted = append(corrupted, CorruptedKeysEntry{
			ShardID:       sid,
			CorruptedKeys: keys,
		})
	}
	return &FixerCorruptedKeysActivityResult{
		CorruptedKeys:             corrupted,
		MinShard:                  minShardID,
		MaxShard:                  maxShardID,
		ShardQueryPaginationToken: queryResult.ShardQueryPaginationToken,
	}, nil
}

// FixShardActivity will fix a collection of shards.
func FixShardActivity(
	activityCtx context.Context,
	params FixShardActivityParams,
) ([]FixReport, error) {
	heartbeatDetails := FixShardHeartbeatDetails{
		LastShardIndexHandled: -1,
		Reports:               nil,
	}
	if activity.HasHeartbeatDetails(activityCtx) {
		if err := activity.GetHeartbeatDetails(activityCtx, &heartbeatDetails); err != nil {
			return nil, err
		}
	}
	for i := heartbeatDetails.LastShardIndexHandled + 1; i < len(params.CorruptedKeysEntries); i++ {
		currentShardID := params.CorruptedKeysEntries[i].ShardID
		currentKeys := params.CorruptedKeysEntries[i].CorruptedKeys
		shardReport, err := fixShard(activityCtx, params, currentShardID, currentKeys, heartbeatDetails)
		if err != nil {
			return nil, err
		}
		heartbeatDetails = FixShardHeartbeatDetails{
			LastShardIndexHandled: i,
			Reports:               append(heartbeatDetails.Reports, *shardReport),
		}
	}
	return heartbeatDetails.Reports, nil
}

func fixShard(
	activityCtx context.Context,
	params FixShardActivityParams,
	shardID int,
	corruptedKeys store.Keys,
	heartbeatDetails FixShardHeartbeatDetails,
) (*FixReport, error) {
	ctx := activityCtx.Value(params.ContextKey).(FixerContext)
	resources := ctx.Resource
	scope := ctx.Scope.Tagged(metrics.ActivityTypeTag(ActivityFixShard))
	sw := scope.StartTimer(metrics.CadenceLatency)
	defer sw.Stop()

	if ctx.Hooks == nil {
		return nil, errors.New("fixer hooks are not provided")
	}

	execManager, err := resources.GetExecutionManager(shardID)
	if err != nil {
		scope.IncCounter(metrics.CadenceFailures)
		return nil, err
	}

	pr := persistence.NewPersistenceRetryer(execManager, resources.GetHistoryManager(), c.CreatePersistenceRetryPolicy())

	fixer := NewFixer(
		shardID,
		ctx.Hooks.InvariantManager(pr, params, *ctx.Config),
		ctx.Hooks.Iterator(resources.GetBlobstoreClient(), corruptedKeys, params, *ctx.Config),
		resources.GetBlobstoreClient(),
		params.ResolvedFixerWorkflowConfig.BlobstoreFlushThreshold,
		func() { activity.RecordHeartbeat(activityCtx, heartbeatDetails) },
	)
	report := fixer.Fix()
	if report.Result.ControlFlowFailure != nil {
		scope.IncCounter(metrics.CadenceFailures)
	}
	return &report, nil
}

// ScannerEmitMetricsActivity will emit metrics for a complete run of ShardScanner
func ScannerEmitMetricsActivity(
	activityCtx context.Context,
	params ScannerEmitMetricsActivityParams,
) error {
	scope := activityCtx.Value(params.ContextKey).(Context).Scope.Tagged(
		metrics.ActivityTypeTag(ActivityScannerEmitMetrics),
	)
	scope.UpdateGauge(metrics.CadenceShardSuccessGauge, float64(params.ShardSuccessCount))
	scope.UpdateGauge(metrics.CadenceShardFailureGauge, float64(params.ShardControlFlowFailureCount))

	agg := params.AggregateReportResult
	scope.UpdateGauge(metrics.ScannerExecutionsGauge, float64(agg.EntitiesCount))
	scope.UpdateGauge(metrics.ScannerCorruptedGauge, float64(agg.CorruptedCount))
	scope.UpdateGauge(metrics.ScannerCheckFailedGauge, float64(agg.CheckFailedCount))
	for k, v := range agg.CorruptionByType {
		scope.Tagged(metrics.InvariantTypeTag(string(k))).UpdateGauge(metrics.ScannerCorruptionByTypeGauge, float64(v))
	}
	shardStats := params.ShardDistributionStats
	scope.UpdateGauge(metrics.ScannerShardSizeMaxGauge, float64(shardStats.Max))
	scope.UpdateGauge(metrics.ScannerShardSizeMedianGauge, float64(shardStats.Median))
	scope.UpdateGauge(metrics.ScannerShardSizeMinGauge, float64(shardStats.Min))
	scope.UpdateGauge(metrics.ScannerShardSizeNinetyGauge, float64(shardStats.P90))
	scope.UpdateGauge(metrics.ScannerShardSizeSeventyFiveGauge, float64(shardStats.P75))
	scope.UpdateGauge(metrics.ScannerShardSizeTwentyFiveGauge, float64(shardStats.P25))
	scope.UpdateGauge(metrics.ScannerShardSizeTenGauge, float64(shardStats.P10))
	return nil
}
