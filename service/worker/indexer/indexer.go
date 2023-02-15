// Copyright (c) 2017 Uber Technologies, Inc.
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

package indexer

import (
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/uber/cadence/.gen/go/indexer"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/codec"
	"github.com/uber/cadence/common/definition"
	"github.com/uber/cadence/common/dynamicconfig"
	es "github.com/uber/cadence/common/elasticsearch"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/tag"
	"github.com/uber/cadence/common/messaging"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/types"
)

const (
	versionTypeExternal = "external"
)

var (
	errUnknownMessageType = &types.BadRequestError{Message: "unknown message type"}
	defaultEncoder        = codec.NewThriftRWEncoder()
)

type (
	ESProcessor interface {
		common.Daemon
		Add(request *es.GenericBulkableAddRequest, key string, kafkaMsg messaging.Message)
	}
	// Indexer used to consumer data from kafka then send to ElasticSearch
	Indexer struct {
		esIndexName string
		consumer    messaging.Consumer
		esProcessor ESProcessor
		config      *Config
		logger      log.Logger
		scope       metrics.Scope
		msgEncoder  codec.BinaryEncoder

		isStarted  int32
		isStopped  int32
		shutdownWG sync.WaitGroup
		shutdownCh chan struct{}
	}

	// Config contains all configs for indexer
	Config struct {
		IndexerConcurrency             dynamicconfig.IntPropertyFn
		ESProcessorNumOfWorkers        dynamicconfig.IntPropertyFn
		ESProcessorBulkActions         dynamicconfig.IntPropertyFn // max number of requests in bulk
		ESProcessorBulkSize            dynamicconfig.IntPropertyFn // max total size of bytes in bulk
		ESProcessorFlushInterval       dynamicconfig.DurationPropertyFn
		ValidSearchAttributes          dynamicconfig.MapPropertyFn
		EnableQueryAttributeValidation dynamicconfig.BoolPropertyFn
	}
)

// NewIndexer create a new Indexer
func NewIndexer(
	config *Config,
	client messaging.Client,
	esClient es.GenericClient,
	visibilityName string,
	logger log.Logger,
	metricsClient metrics.Client,
) *Indexer {
	logger = logger.WithTags(tag.ComponentIndexer)

	esProcessor, err := newESProcessor(config, esClient, logger, metricsClient)
	if err != nil {
		logger.Fatal("Index ES processor state changed", tag.LifeCycleStartFailed, tag.Error(err))
	}

	consumer, err := client.NewConsumer(common.VisibilityAppName, getConsumerName(visibilityName))
	if err != nil {
		logger.Fatal("Index consumer state changed", tag.LifeCycleStartFailed, tag.Error(err))
	}

	i := &Indexer{
		config:      config,
		esIndexName: visibilityName,
		consumer:    consumer,
		logger:      logger.WithTags(tag.ComponentIndexerProcessor),
		scope:       metricsClient.Scope(metrics.IndexProcessorScope),
		shutdownCh:  make(chan struct{}),
		esProcessor: esProcessor,
		msgEncoder:  defaultEncoder,
	}
	return i
}

func getConsumerName(topic string) string {
	return fmt.Sprintf("%s-consumer", topic)
}

func (p *Indexer) Start() error {
	if !atomic.CompareAndSwapInt32(&p.isStarted, 0, 1) {
		return nil
	}

	if err := p.consumer.Start(); err != nil {
		p.logger.Info("Index consumer state changed", tag.LifeCycleStartFailed, tag.Error(err))
		return err
	}

	p.esProcessor.Start()

	p.shutdownWG.Add(1)
	go p.processorPump()

	p.logger.Info("Index bulkProcessor state changed", tag.LifeCycleStarted)
	return nil
}

func (p *Indexer) Stop() {
	if !atomic.CompareAndSwapInt32(&p.isStopped, 0, 1) {
		return
	}

	p.logger.Info("Index bulkProcessor state changed", tag.LifeCycleStopping)
	defer p.logger.Info("Index bulkProcessor state changed", tag.LifeCycleStopped)

	if atomic.LoadInt32(&p.isStarted) == 1 {
		close(p.shutdownCh)
	}

	if success := common.AwaitWaitGroup(&p.shutdownWG, time.Minute); !success {
		p.logger.Info("Index bulkProcessor state changed", tag.LifeCycleStopTimedout)
	}
}

func (p *Indexer) processorPump() {
	defer p.shutdownWG.Done()

	var workerWG sync.WaitGroup
	for workerID := 0; workerID < p.config.IndexerConcurrency(); workerID++ {
		workerWG.Add(1)
		go p.messageProcessLoop(&workerWG)
	}

	<-p.shutdownCh
	// Processor is shutting down, close the underlying consumer and esProcessor
	p.consumer.Stop()
	p.esProcessor.Stop()

	p.logger.Info("Index bulkProcessor pump shutting down.")
	if success := common.AwaitWaitGroup(&workerWG, 10*time.Second); !success {
		p.logger.Warn("Index bulkProcessor timed out on worker shutdown.")
	}
}

func (p *Indexer) messageProcessLoop(workerWG *sync.WaitGroup) {
	defer workerWG.Done()

	for msg := range p.consumer.Messages() {
		sw := p.scope.StartTimer(metrics.IndexProcessorProcessMsgLatency)
		err := p.process(msg)
		sw.Stop()
		if err != nil {
			msg.Nack() //nolint:errcheck
		}
	}
}

func (p *Indexer) process(kafkaMsg messaging.Message) error {
	logger := p.logger.WithTags(tag.KafkaPartition(kafkaMsg.Partition()), tag.KafkaOffset(kafkaMsg.Offset()), tag.AttemptStart(time.Now()))

	indexMsg, err := p.deserialize(kafkaMsg.Value())
	if err != nil {
		logger.Error("Failed to deserialize index messages.", tag.Error(err))
		p.scope.IncCounter(metrics.IndexProcessorCorruptedData)
		return err
	}

	return p.addMessageToES(indexMsg, kafkaMsg, logger)
}

func (p *Indexer) deserialize(payload []byte) (*indexer.Message, error) {
	var msg indexer.Message
	if err := p.msgEncoder.Decode(payload, &msg); err != nil {
		return nil, err
	}
	return &msg, nil
}

func (p *Indexer) addMessageToES(indexMsg *indexer.Message, kafkaMsg messaging.Message, logger log.Logger) error {
	docID := es.GenerateDocID(indexMsg.GetWorkflowID(), indexMsg.GetRunID())
	// check and skip invalid docID
	if len(docID) >= es.GetESDocIDSizeLimit() {
		logger.Error("Index message is too long",
			tag.WorkflowDomainID(indexMsg.GetDomainID()),
			tag.WorkflowID(indexMsg.GetWorkflowID()),
			tag.WorkflowRunID(indexMsg.GetRunID()))
		kafkaMsg.Nack()
		return nil
	}

	var keyToKafkaMsg string
	req := &es.GenericBulkableAddRequest{
		Index:       p.esIndexName,
		Type:        es.GetESDocType(),
		ID:          docID,
		VersionType: versionTypeExternal,
		Version:     indexMsg.GetVersion(),
	}
	switch indexMsg.GetMessageType() {
	case indexer.MessageTypeIndex:
		keyToKafkaMsg = fmt.Sprintf("%v-%v", kafkaMsg.Partition(), kafkaMsg.Offset())
		doc := p.generateESDoc(indexMsg, keyToKafkaMsg)
		req.Doc = doc
		req.RequestType = es.BulkableIndexRequest
	case indexer.MessageTypeDelete:
		keyToKafkaMsg = docID
		req.RequestType = es.BulkableDeleteRequest
	case indexer.MessageTypeCreate:
		keyToKafkaMsg = fmt.Sprintf("%v-%v", kafkaMsg.Partition(), kafkaMsg.Offset())
		doc := p.generateESDoc(indexMsg, keyToKafkaMsg)
		req.Doc = doc
		req.RequestType = es.BulkableCreateRequest
	default:
		logger.Error("Unknown message type")
		p.scope.IncCounter(metrics.IndexProcessorCorruptedData)
		return errUnknownMessageType
	}

	p.esProcessor.Add(req, keyToKafkaMsg, kafkaMsg)
	return nil
}

func (p *Indexer) generateESDoc(msg *indexer.Message, keyToKafkaMsg string) map[string]interface{} {
	doc := p.dumpFieldsToMap(msg.Fields, msg.GetDomainID())
	fulfillDoc(doc, msg, keyToKafkaMsg)
	return doc
}

func (p *Indexer) decodeSearchAttrBinary(bytes []byte, key string) interface{} {
	var val interface{}
	err := json.Unmarshal(bytes, &val)
	if err != nil {
		p.logger.Error("Error when decode search attributes values.", tag.Error(err), tag.ESField(key))
		p.scope.IncCounter(metrics.IndexProcessorCorruptedData)
	}
	return val
}

func (p *Indexer) dumpFieldsToMap(fields map[string]*indexer.Field, domainID string) map[string]interface{} {
	doc := make(map[string]interface{})
	attr := make(map[string]interface{})
	for k, v := range fields {
		if !p.isValidFieldToES(k) {
			p.logger.Error("Unregistered field.", tag.ESField(k), tag.WorkflowDomainID(domainID))
			p.scope.IncCounter(metrics.IndexProcessorCorruptedData)
			continue
		}

		// skip VisibilityOperation since it’s not being used for advanced visibility
		if k == es.VisibilityOperation {
			continue
		}

		switch v.GetType() {
		case indexer.FieldTypeString:
			doc[k] = v.GetStringData()
		case indexer.FieldTypeInt:
			doc[k] = v.GetIntData()
		case indexer.FieldTypeBool:
			doc[k] = v.GetBoolData()
		case indexer.FieldTypeBinary:
			if k == definition.Memo {
				doc[k] = v.GetBinaryData()
			} else { // custom search attributes
				attr[k] = p.decodeSearchAttrBinary(v.GetBinaryData(), k)
			}
		default:
			// there must be bug in code and bad deployment, check data sent from producer
			p.logger.Fatal("Unknown field type")
		}
	}
	doc[definition.Attr] = attr
	return doc
}

func (p *Indexer) isValidFieldToES(field string) bool {
	if !p.config.EnableQueryAttributeValidation() {
		return true
	}
	if _, ok := p.config.ValidSearchAttributes()[field]; ok {
		return true
	}
	if field == definition.Memo || field == definition.KafkaKey || field == definition.Encoding || field == es.VisibilityOperation {
		return true
	}
	return false
}

func fulfillDoc(doc map[string]interface{}, msg *indexer.Message, keyToKafkaMsg string) {
	doc[definition.DomainID] = msg.GetDomainID()
	doc[definition.WorkflowID] = msg.GetWorkflowID()
	doc[definition.RunID] = msg.GetRunID()
	doc[definition.KafkaKey] = keyToKafkaMsg
}
