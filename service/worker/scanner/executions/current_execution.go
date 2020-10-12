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

package executions

import (
	"strconv"
	"time"

	cclient "go.uber.org/cadence/client"
	"go.uber.org/cadence/workflow"

	"github.com/uber/cadence/common/blobstore"
	"github.com/uber/cadence/common/pagination"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/reconciliation/invariant"
	"github.com/uber/cadence/common/reconciliation/store"
	"github.com/uber/cadence/common/service/dynamicconfig"
	"github.com/uber/cadence/service/worker/scanner/shardscanner"
)

const (
	CurrentExecutionsScannerWFID = "cadence-sys-current-executions-scanner"

	CurrentExecutionsScannerWFTypeName   = "cadence-sys-current-executions-scanner-workflow"
	CurrentExecutionsScannerTaskListName = "cadence-sys-current-executions-scanner-tasklist-0"

	CurrentExecutionsFixerWFTypeName   = "cadence-sys-current-executions-fixer-workflow"
	CurrentExecutionsFixerTaskListName = "cadence-sys-current-executions-fixer-tasklist-0"
)

// CurrentScannerWorkflow is the workflow that scans over all current executions
func CurrentScannerWorkflow(
	ctx workflow.Context,
	params shardscanner.ScannerWorkflowParams,
) error {
	wf, err := shardscanner.NewScannerWorkflow(ctx, CurrentExecutionsScannerWFTypeName, params)
	if err != nil {
		return err
	}

	return wf.Start(ctx)
}

// CurrentExecutionsHooks provides hooks for current executions scanner.
func CurrentExecutionsHooks() *shardscanner.ScannerHooks {
	wf := shardscanner.NewScannerHooks(CurrentExecutionManager, CurrentExecutionIterator)
	wf.SetConfig(CurrentExecutionConfig)

	return wf
}

func CurrentExecutionManager(
	pr persistence.Retryer,
	params shardscanner.ScanShardActivityParams,
	_ shardscanner.ScannerConfig,
) invariant.Manager {
	var ivs []invariant.Invariant
	collections := ParseCollections(params.ScannerConfig)
	for _, fn := range CurrentExecutionType.ToInvariants(collections) {
		ivs = append(ivs, fn(pr))
	}
	return invariant.NewInvariantManager(ivs)
}

// CurrentFixerWorkflow starts current executions fixer.
func CurrentFixerWorkflow(
	ctx workflow.Context,
	params shardscanner.FixerWorkflowParams,
) error {
	wf, err := shardscanner.NewFixerWorkflow(ctx, CurrentExecutionsFixerWFTypeName, params)
	if err != nil {
		return err
	}
	return wf.Start(ctx)
}

// CurrentExecutionConfig resolves dynamic config for current executions scanner.
func CurrentExecutionConfig(ctx shardscanner.Context) shardscanner.CustomScannerConfig {
	res := shardscanner.CustomScannerConfig{}

	if ctx.Config.DynamicCollection.GetBoolProperty(dynamicconfig.CurrentExecutionsScannerInvariantCollectionHistory, true)() {
		res[invariant.CollectionHistory.String()] = strconv.FormatBool(true)
	}
	if ctx.Config.DynamicCollection.GetBoolProperty(dynamicconfig.CurrentExecutionsScannerInvariantCollectionMutableState, true)() {
		res[invariant.CollectionMutableState.String()] = strconv.FormatBool(true)
	}

	return res
}

// CurrentExecutionFixerHooks provides hooks for current executions fixer.
func CurrentExecutionFixerHooks() *shardscanner.FixerHooks {
	return shardscanner.NewFixerHooks(FixerManager, CurrentExecutionFixerIterator)
}

// CurrentConfig configures current execution scanner
func CurrentConfig(dc *dynamicconfig.Collection) *shardscanner.ScannerConfig {
	return &shardscanner.ScannerConfig{
		ScannerWFTypeName: CurrentExecutionsScannerWFTypeName,
		FixerWFTypeName:   CurrentExecutionsFixerWFTypeName,
		DynamicCollection: dc,
		DynamicParams: shardscanner.DynamicParams{
			Enabled:                 dc.GetBoolProperty(dynamicconfig.CurrentExecutionsScannerEnabled, true),
			Concurrency:             dc.GetIntProperty(dynamicconfig.CurrentExecutionsScannerConcurrency, 25),
			PageSize:                dc.GetIntProperty(dynamicconfig.CurrentExecutionsScannerPersistencePageSize, 1000),
			BlobstoreFlushThreshold: dc.GetIntProperty(dynamicconfig.CurrentExecutionsScannerBlobstoreFlushThreshold, 100),
			ActivityBatchSize:       dc.GetIntProperty(dynamicconfig.CurrentExecutionsScannerActivityBatchSize, 25),
		},
		ScannerHooks: CurrentExecutionsHooks,
		FixerHooks:   CurrentExecutionFixerHooks,
		FixerTLName:  CurrentExecutionsFixerTaskListName,
		StartWorkflowOptions: cclient.StartWorkflowOptions{
			ID:                           CurrentExecutionsScannerWFID,
			TaskList:                     CurrentExecutionsScannerTaskListName,
			ExecutionStartToCloseTimeout: 20 * 365 * 24 * time.Hour,
			WorkflowIDReusePolicy:        cclient.WorkflowIDReusePolicyAllowDuplicate,
			CronSchedule:                 "* * * * *",
		},
	}
}

func CurrentExecutionIterator(
	pr persistence.Retryer,
	params shardscanner.ScanShardActivityParams,
	_ shardscanner.ScannerConfig,
) pagination.Iterator {
	return CurrentExecutionType.ToIterator()(pr, params.PageSize)
}

func CurrentExecutionFixerIterator(
	client blobstore.Client,
	keys store.Keys,
	_ shardscanner.FixShardActivityParams,
	_ shardscanner.ScannerConfig,
) store.ScanOutputIterator {
	return store.NewBlobstoreIterator(client, keys, CurrentExecutionType.ToBlobstoreEntity())
}