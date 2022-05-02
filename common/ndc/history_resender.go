// Copyright (c) 2019 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

//go:generate mockgen -package $GOPACKAGE -source $GOFILE -destination history_resender_mock.go

package ndc

import (
	"context"
	"errors"
	"time"

	adminClient "github.com/uber/cadence/client/admin"
	"github.com/uber/cadence/common/cache"
	"github.com/uber/cadence/common/collection"
	"github.com/uber/cadence/common/dynamicconfig"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/tag"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/reconciliation/entity"
	"github.com/uber/cadence/common/reconciliation/invariant"
	"github.com/uber/cadence/common/types"
)

var (
	// ErrSkipTask is the error to skip task due to absence of the workflow in the source cluster
	ErrSkipTask = errors.New("the source workflow does not exist")
)

const (
	resendContextTimeout = 30 * time.Second
	defaultPageSize      = int32(100)
)

type (
	// nDCHistoryReplicationFn provides the functionality to deliver replication raw history request to history
	// the provided func should be thread safe
	nDCHistoryReplicationFn func(ctx context.Context, request *types.ReplicateEventsV2Request) error

	// HistoryResender is the interface for resending history events to remote
	HistoryResender interface {
		// SendSingleWorkflowHistory sends multiple run IDs's history events to remote
		SendSingleWorkflowHistory(
			identifier WorkflowIdentifier,
			startEventID *int64,
			startEventVersion *int64,
			endEventID *int64,
			endEventVersion *int64,
		) error
	}

	WorkflowIdentifier interface {
		GetWorkflowID() string
		GetDomainID() string
		GetRunID() string
	}

	// HistoryResenderImpl is the implementation of NDCHistoryResender
	HistoryResenderImpl struct {
		domainCache           cache.DomainCache
		adminClient           adminClient.Client
		historyReplicationFn  nDCHistoryReplicationFn
		serializer            persistence.PayloadSerializer
		rereplicationTimeout  dynamicconfig.DurationPropertyFnWithDomainIDFilter
		currentExecutionCheck invariant.Invariant
		logger                log.Logger
	}

	historyBatch struct {
		versionHistory *types.VersionHistory
		rawEventBatch  *types.DataBlob
	}
)

// NewHistoryResender create a new NDCHistoryResenderImpl
func NewHistoryResender(
	domainCache cache.DomainCache,
	adminClient adminClient.Client,
	historyReplicationFn nDCHistoryReplicationFn,
	serializer persistence.PayloadSerializer,
	rereplicationTimeout dynamicconfig.DurationPropertyFnWithDomainIDFilter,
	currentExecutionCheck invariant.Invariant,
	logger log.Logger,
) *HistoryResenderImpl {

	return &HistoryResenderImpl{
		domainCache:           domainCache,
		adminClient:           adminClient,
		historyReplicationFn:  historyReplicationFn,
		serializer:            serializer,
		rereplicationTimeout:  rereplicationTimeout,
		currentExecutionCheck: currentExecutionCheck,
		logger:                logger,
	}
}

// SendSingleWorkflowHistory sends one run IDs's history events to remote
func (n *HistoryResenderImpl) SendSingleWorkflowHistory(
	identifier WorkflowIdentifier,
	startEventID *int64,
	startEventVersion *int64,
	endEventID *int64,
	endEventVersion *int64,
) error {

	ctx := context.Background()
	var cancel context.CancelFunc
	if n.rereplicationTimeout != nil {
		resendContextTimeout := n.rereplicationTimeout(identifier.GetDomainID())
		if resendContextTimeout > 0 {
			ctx, cancel = context.WithTimeout(ctx, resendContextTimeout)
			defer cancel()
		}
	}

	historyIterator := collection.NewPagingIterator(n.getPaginationFn(
		ctx,
		identifier,
		startEventID,
		startEventVersion,
		endEventID,
		endEventVersion))

	for historyIterator.HasNext() {
		result, err := historyIterator.Next()
		if err != nil {
			n.logger.Error("failed to get history events",
				tag.WorkflowDomainID(identifier.GetDomainID()),
				tag.WorkflowID(identifier.GetWorkflowID()),
				tag.WorkflowRunID(identifier.GetRunID()),
				tag.Error(err))
			return err
		}
		historyBatch := result.(*historyBatch)
		replicationRequest := n.createReplicationRawRequest(
			identifier,
			historyBatch.rawEventBatch,
			historyBatch.versionHistory.GetItems())

		err = n.sendReplicationRawRequest(ctx, replicationRequest)
		switch err.(type) {
		case nil:
			// continue to process the events
			break
		case *types.EntityNotExistsError:
			// Case 1: the workflow pass the retention period
			// Case 2: the workflow is corrupted
			if skipTask := n.fixCurrentExecution(
				ctx,
				identifier,
			); skipTask {
				return ErrSkipTask
			}
			return err
		default:
			n.logger.Error("failed to replicate events",
				tag.WorkflowDomainID(identifier.GetDomainID()),
				tag.WorkflowID(identifier.GetWorkflowID()),
				tag.WorkflowRunID(identifier.GetRunID()),
				tag.Error(err))
			return err
		}
	}
	return nil
}

func (n *HistoryResenderImpl) getPaginationFn(
	ctx context.Context,
	identifier WorkflowIdentifier,
	startEventID *int64,
	startEventVersion *int64,
	endEventID *int64,
	endEventVersion *int64,
) collection.PaginationFn {

	return func(paginationToken []byte) ([]interface{}, []byte, error) {

		response, err := n.getHistory(
			ctx,
			identifier,
			startEventID,
			startEventVersion,
			endEventID,
			endEventVersion,
			paginationToken,
			defaultPageSize,
		)
		if err != nil {
			return nil, nil, err
		}

		var paginateItems []interface{}
		versionHistory := response.GetVersionHistory()
		for _, history := range response.GetHistoryBatches() {
			batch := &historyBatch{
				versionHistory: versionHistory,
				rawEventBatch:  history,
			}
			paginateItems = append(paginateItems, batch)
		}
		return paginateItems, response.NextPageToken, nil
	}
}

func (n *HistoryResenderImpl) createReplicationRawRequest(
	identifier WorkflowIdentifier,
	historyBlob *types.DataBlob,
	versionHistoryItems []*types.VersionHistoryItem,
) *types.ReplicateEventsV2Request {

	request := &types.ReplicateEventsV2Request{
		DomainUUID: identifier.GetDomainID(),
		WorkflowExecution: &types.WorkflowExecution{
			WorkflowID: identifier.GetWorkflowID(),
			RunID:      identifier.GetRunID(),
		},
		Events:              historyBlob,
		VersionHistoryItems: versionHistoryItems,
	}
	return request
}

func (n *HistoryResenderImpl) sendReplicationRawRequest(
	ctx context.Context,
	request *types.ReplicateEventsV2Request,
) error {

	ctx, cancel := context.WithTimeout(ctx, resendContextTimeout)
	defer cancel()
	return n.historyReplicationFn(ctx, request)
}

func (n *HistoryResenderImpl) getHistory(
	ctx context.Context,
	identifier WorkflowIdentifier,
	startEventID *int64,
	startEventVersion *int64,
	endEventID *int64,
	endEventVersion *int64,
	token []byte,
	pageSize int32,
) (*types.GetWorkflowExecutionRawHistoryV2Response, error) {

	logger := n.logger.WithTags(tag.WorkflowRunID(identifier.GetRunID()))

	domainName, err := n.domainCache.GetDomainName(identifier.GetDomainID())
	if err != nil {
		logger.Error("error getting domain", tag.Error(err))
		return nil, err
	}

	ctx, cancel := context.WithTimeout(ctx, resendContextTimeout)
	defer cancel()
	response, err := n.adminClient.GetWorkflowExecutionRawHistoryV2(ctx, &types.GetWorkflowExecutionRawHistoryV2Request{
		Domain: domainName,
		Execution: &types.WorkflowExecution{
			WorkflowID: identifier.GetWorkflowID(),
			RunID:      identifier.GetRunID(),
		},
		StartEventID:      startEventID,
		StartEventVersion: startEventVersion,
		EndEventID:        endEventID,
		EndEventVersion:   endEventVersion,
		MaximumPageSize:   pageSize,
		NextPageToken:     token,
	})
	if err != nil {
		logger.Error("error getting history", tag.Error(err))
		return nil, err
	}

	return response, nil
}

func (n *HistoryResenderImpl) fixCurrentExecution(
	ctx context.Context,
	identifier WorkflowIdentifier,
) bool {

	if n.currentExecutionCheck == nil {
		return false
	}
	execution := &entity.CurrentExecution{
		Execution: entity.Execution{
			DomainID:   identifier.GetDomainID(),
			WorkflowID: identifier.GetRunID(),
			State:      persistence.WorkflowStateRunning,
		},
	}
	res := n.currentExecutionCheck.Check(ctx, execution)
	switch res.CheckResultType {
	case invariant.CheckResultTypeCorrupted:
		n.logger.Error(
			"Encounter corrupted workflow",
			tag.WorkflowDomainID(domainID),
			tag.WorkflowID(workflowID),
			tag.WorkflowRunID(runID),
		)
		n.currentExecutionCheck.Fix(ctx, execution)
		return false
	case invariant.CheckResultTypeFailed:
		return false
	default:
		return true
	}
}
