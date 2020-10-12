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
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/suite"
	"go.uber.org/cadence/.gen/go/shared"
	"go.uber.org/cadence/activity"
	"go.uber.org/cadence/testsuite"
	"go.uber.org/cadence/worker"

	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/pagination"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/reconciliation/entity"
	"github.com/uber/cadence/common/reconciliation/invariant"
	"github.com/uber/cadence/common/reconciliation/store"
	"github.com/uber/cadence/common/resource"
	"github.com/uber/cadence/common/service/dynamicconfig"
)

var TestContextKey ScannerContextKey = "test-context"

type activitiesSuite struct {
	suite.Suite
	testsuite.WorkflowTestSuite

	controller   *gomock.Controller
	mockResource *resource.Test
}

func TestActivitiesSuite(t *testing.T) {
	suite.Run(t, new(activitiesSuite))
}

func (s *activitiesSuite) SetupSuite() {
	activity.Register(ScannerConfigActivity)
	activity.Register(FixerCorruptedKeysActivity)
	activity.Register(ScanShardActivity)
	activity.Register(FixShardActivity)
}

func (s *activitiesSuite) SetupTest() {
	s.controller = gomock.NewController(s.T())
	s.mockResource = resource.NewTest(s.controller, metrics.Worker)
	defer s.controller.Finish()
}

func (s *activitiesSuite) TestScanShardActivity() {

	testCases := []struct {
		params      ScanShardActivityParams
		wantErr     bool
		managerHook func(pr persistence.Retryer, params ScanShardActivityParams, config ScannerConfig) invariant.Manager
		itHook      func(pr persistence.Retryer, params ScanShardActivityParams, config ScannerConfig) pagination.Iterator
	}{
		{
			params: ScanShardActivityParams{
				Shards:     []int{0},
				ContextKey: TestContextKey,
			},
			managerHook: func(pr persistence.Retryer, params ScanShardActivityParams, config ScannerConfig) invariant.Manager {
				manager := invariant.NewMockManager(s.controller)
				manager.EXPECT().RunChecks(gomock.Any()).
					AnyTimes().
					Return(
						invariant.ManagerCheckResult{CheckResultType: invariant.CheckResultTypeHealthy},
					)
				return manager
			},
			itHook: func(pr persistence.Retryer, params ScanShardActivityParams, config ScannerConfig) pagination.Iterator {
				it := pagination.NewMockIterator(s.controller)
				calls := 0
				it.EXPECT().HasNext().DoAndReturn(
					func() bool {
						if calls > 1 {
							return false
						}
						calls++
						return true
					},
				).AnyTimes()
				it.EXPECT().Next().Return(&entity.ConcreteExecution{}, nil).Times(2)
				return it
			},
			wantErr: false,
		},
	}

	for _, tc := range testCases {

		env := s.NewTestActivityEnvironment()
		env.SetWorkerOptions(worker.Options{
			BackgroundActivityContext: context.WithValue(context.Background(), TestContextKey, Context{
				ContextKey: TestContextKey,
				Config:     &ScannerConfig{},
				Scope:      metrics.NoopScope(metrics.Worker),
				Resource:   s.mockResource,
				Hooks:      NewScannerHooks(tc.managerHook, tc.itHook),
			}),
		})
		report, err := env.ExecuteActivity(ScanShardActivity, tc.params)
		if tc.wantErr {
			s.Error(err)
		} else {
			s.NoError(err)
		}
		var reports []ScanReport
		s.NoError(report.Get(&reports))
	}
}

func (s *activitiesSuite) TestScannerConfigActivity() {
	testCases := []struct {
		dynamicParams *DynamicParams
		params        ScannerConfigActivityParams
		resolved      ResolvedScannerWorkflowConfig
		addHook       bool
	}{
		{
			dynamicParams: &DynamicParams{
				Enabled:                 dynamicconfig.GetBoolPropertyFn(true),
				Concurrency:             dynamicconfig.GetIntPropertyFn(10),
				PageSize:                dynamicconfig.GetIntPropertyFn(100),
				ActivityBatchSize:       dynamicconfig.GetIntPropertyFn(10),
				BlobstoreFlushThreshold: dynamicconfig.GetIntPropertyFn(1000),
			},
			params: ScannerConfigActivityParams{
				Overwrites: ScannerWorkflowConfigOverwrites{},
				ContextKey: TestContextKey,
			},
			addHook: true,
			resolved: ResolvedScannerWorkflowConfig{
				Enabled:                 true,
				Concurrency:             10,
				ActivityBatchSize:       10,
				PageSize:                100,
				BlobstoreFlushThreshold: 1000,
				CustomScannerConfig: CustomScannerConfig{
					"test-key": "test-value",
				},
			},
		},
		{
			dynamicParams: &DynamicParams{
				Enabled:                 dynamicconfig.GetBoolPropertyFn(true),
				Concurrency:             dynamicconfig.GetIntPropertyFn(10),
				PageSize:                dynamicconfig.GetIntPropertyFn(100),
				ActivityBatchSize:       dynamicconfig.GetIntPropertyFn(10),
				BlobstoreFlushThreshold: dynamicconfig.GetIntPropertyFn(1000),
			},
			params: ScannerConfigActivityParams{
				Overwrites: ScannerWorkflowConfigOverwrites{},
				ContextKey: TestContextKey,
			},
			resolved: ResolvedScannerWorkflowConfig{
				Enabled:                 true,
				Concurrency:             10,
				ActivityBatchSize:       10,
				PageSize:                100,
				BlobstoreFlushThreshold: 1000,
			},
		},
		{
			dynamicParams: &DynamicParams{
				Enabled:                 dynamicconfig.GetBoolPropertyFn(true),
				Concurrency:             dynamicconfig.GetIntPropertyFn(10),
				ActivityBatchSize:       dynamicconfig.GetIntPropertyFn(100),
				PageSize:                dynamicconfig.GetIntPropertyFn(100),
				BlobstoreFlushThreshold: dynamicconfig.GetIntPropertyFn(1000),
			},
			params: ScannerConfigActivityParams{
				Overwrites: ScannerWorkflowConfigOverwrites{
					Enabled:                 common.BoolPtr(false),
					ActivityBatchSize:       common.IntPtr(1),
					BlobstoreFlushThreshold: common.IntPtr(100),
					CustomScannerConfig: &CustomScannerConfig{
						"test": "test",
					},
				},
				ContextKey: TestContextKey,
			},
			resolved: ResolvedScannerWorkflowConfig{
				Enabled:                 false,
				Concurrency:             10,
				ActivityBatchSize:       1,
				PageSize:                100,
				BlobstoreFlushThreshold: 100,
				CustomScannerConfig: CustomScannerConfig{
					"test": "test",
				},
			},
		},
	}

	for _, tc := range testCases {
		env := s.NewTestActivityEnvironment()

		configHook := NewScannerHooks(nil, nil)
		if tc.addHook {
			configHook.Config = func(scanner Context) CustomScannerConfig {
				return map[string]string{"test-key": "test-value"}
			}
		}

		env.SetWorkerOptions(worker.Options{
			BackgroundActivityContext: context.WithValue(context.Background(), TestContextKey, Context{
				ContextKey: TestContextKey,
				Config: &ScannerConfig{
					DynamicParams: *tc.dynamicParams,
				},
				Hooks: configHook,
			}),
		})
		resolvedValue, err := env.ExecuteActivity(ScannerConfigActivity, tc.params)
		s.NoError(err)
		var resolved ResolvedScannerWorkflowConfig
		s.NoError(resolvedValue.Get(&resolved))
		s.Equal(tc.resolved, resolved)
	}
}

func (s *activitiesSuite) TestFixerCorruptedKeysActivity() {
	s.mockResource.SDKClient.EXPECT().DescribeWorkflowExecution(gomock.Any(), gomock.Any()).Return(&shared.DescribeWorkflowExecutionResponse{
		WorkflowExecutionInfo: &shared.WorkflowExecutionInfo{
			CloseStatus: shared.WorkflowExecutionCloseStatusCompleted.Ptr(),
		},
	}, nil)
	queryResult := &ShardCorruptKeysQueryResult{
		Result: map[int]store.Keys{
			1: {UUID: "first"},
			2: {UUID: "second"},
			3: {UUID: "third"},
		},
		ShardQueryPaginationToken: ShardQueryPaginationToken{
			NextShardID: common.IntPtr(4),
			IsDone:      false,
		},
	}
	queryResultData, err := json.Marshal(queryResult)
	s.NoError(err)
	s.mockResource.SDKClient.EXPECT().QueryWorkflow(gomock.Any(), gomock.Any()).Return(&shared.QueryWorkflowResponse{
		QueryResult: queryResultData,
	}, nil)
	env := s.NewTestActivityEnvironment()
	env.SetWorkerOptions(worker.Options{
		BackgroundActivityContext: context.WithValue(context.Background(), TestContextKey, FixerContext{
			Resource:   s.mockResource,
			ContextKey: TestContextKey,
		}),
	})
	fixerResultValue, err := env.ExecuteActivity(FixerCorruptedKeysActivity, FixerCorruptedKeysActivityParams{ContextKey: TestContextKey})
	s.NoError(err)
	fixerResult := &FixerCorruptedKeysActivityResult{}
	s.NoError(fixerResultValue.Get(&fixerResult))
	s.Equal(1, *fixerResult.MinShard)
	s.Equal(3, *fixerResult.MaxShard)
	s.Equal(ShardQueryPaginationToken{
		NextShardID: common.IntPtr(4),
		IsDone:      false,
	}, fixerResult.ShardQueryPaginationToken)
	s.Contains(fixerResult.CorruptedKeys, CorruptedKeysEntry{
		ShardID: 1,
		CorruptedKeys: store.Keys{
			UUID: "first",
		},
	})
	s.Contains(fixerResult.CorruptedKeys, CorruptedKeysEntry{
		ShardID: 2,
		CorruptedKeys: store.Keys{
			UUID: "second",
		},
	})
	s.Contains(fixerResult.CorruptedKeys, CorruptedKeysEntry{
		ShardID: 3,
		CorruptedKeys: store.Keys{
			UUID: "third",
		},
	})
}
