package connector

import (
	"os"

	"github.com/apex/log"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
)

const (
	ShardIteratorAfterSequenceNumber = "AFTER_SEQUENCE_NUMBER"
	ShardIteratorAtSequenceNumber    = "AT_SEQUENCE_NUMBER"
	ShardIteratorAtTimestamp         = "AT_TIMESTAMP"
	ShardIteratorLatest              = "LATEST"
	ShardIteratorTrimHorizon         = "TRIM_HORIZON"
)

// NewConsumer creates a new consumer with initialied kinesis connection
func NewConsumer(config Config) *Consumer {
	config.setDefaults()

	awsConfig := aws.NewConfig().WithMaxRetries(config.MaxRetries)
	if config.KinesisURL != "" {
		awsConfig = awsConfig.WithEndpoint(config.KinesisURL)
	}

	svc := kinesis.New(
		session.New(awsConfig),
	)

	return &Consumer{
		svc:    svc,
		Config: config,
	}
}

type Consumer struct {
	svc *kinesis.Kinesis
	Config
}

// Start takes a handler and then loops over each of the shards
// processing each one with the handler.
func (c *Consumer) Start(handler Handler) {
	resp, err := c.svc.DescribeStream(
		&kinesis.DescribeStreamInput{
			StreamName: aws.String(c.StreamName),
		},
	)

	if err != nil {
		c.Logger.WithError(err).Error("DescribeStream")
		os.Exit(1)
	}

	for _, shard := range resp.StreamDescription.Shards {
		go c.handlerLoop(*shard.ShardId, handler)
	}
}

func (c *Consumer) handlerLoop(shardID string, handler Handler) {
	buf := &Buffer{
		MaxRecordCount: c.BufferSize,
		shardID:        shardID,
	}
	ctx := c.Logger.WithFields(log.Fields{
		"shard": shardID,
	})

	ctx.Info("processing")

	var shardIterator *string

	for {
		if shardIterator == nil {
			shardIterator = c.getShardIterator(shardID)
			log.Info("empty shard iterator, getting a new one")
			continue
		}

		resp, err := c.svc.GetRecords(
			&kinesis.GetRecordsInput{
				ShardIterator: shardIterator,
			},
		)

		if err != nil {
			ctx.WithField("iterator", shardIterator).WithError(err).Error("GetRecords")
			shardIterator = nil
			continue
		}

		if len(resp.Records) > 0 {
			for _, r := range resp.Records {
				buf.AddRecord(r)

				if buf.ShouldFlush() {
					handler.HandleRecords(*buf)
					ctx.WithField("count", buf.RecordCount()).Info("flushed")
					c.Checkpoint.SetCheckpoint(shardID, buf.LastSeq())
					buf.Flush()
				}
			}
		}

		if resp.NextShardIterator != nil && shardIterator != resp.NextShardIterator {
			shardIterator = resp.NextShardIterator
		}
	}
}

func (c *Consumer) getShardIterator(shardID string) *string {
	params := &kinesis.GetShardIteratorInput{
		ShardId:    aws.String(shardID),
		StreamName: aws.String(c.StreamName),
	}

	if c.Checkpoint.CheckpointExists(shardID) {
		params.ShardIteratorType = aws.String(string(ShardIteratorAfterSequenceNumber))
		params.StartingSequenceNumber = aws.String(c.Checkpoint.SequenceNumber(shardID))
	} else {
		params.ShardIteratorType = aws.String(string(c.ShardIteratorType))
	}

	log.WithFields(log.Fields{
		"type":            params.ShardIteratorType,
		"shard_id":        params.ShardId,
		"stream":          params.StreamName,
		"sequence_number": params.StartingSequenceNumber,
	}).Info("get shard iterator")

	resp, err := c.svc.GetShardIterator(params)

	if err != nil {
		c.Logger.WithError(err).Error("GetShardIterator")
		os.Exit(1)
	}

	return resp.ShardIterator
}
