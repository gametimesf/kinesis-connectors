package s3

import (
	"io"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

// Emitter stores data in S3 bucket.
//
// The use of  this struct requires the configuration of an S3 bucket/endpoint. When the buffer is full, this
// struct's Emit method adds the contents of the buffer to S3 as one file. The filename is generated
// from the first and last sequence numbers of the records contained in that file separated by a
// dash. This struct requires the configuration of an S3 bucket and endpoint.
type Emitter struct {
	Bucket string
	Region string

	uploader s3manager.Uploader
}

func NewEmitter(bucket, region string) *Emitter {
	client := s3.New(
		session.New(aws.NewConfig().WithMaxRetries(10)),
		aws.NewConfig().WithRegion(region),
	)
	return &Emitter{
		Bucket:   bucket,
		Region:   region,
		uploader: s3manager.NewUploaderWithClient(client),
	}
}

// Emit is invoked when the buffer is full. This method emits the set of filtered records.
func (e *Emitter) Emit(s3Key string, b io.ReadSeeker) error {
	params := &s3.PutObjectInput{
		Body:   b,
		Bucket: aws.String(e.Bucket),
		Key:    aws.String(s3Key),
	}

	_, err := e.uploader.Upload(params)

	if err != nil {
		return err
	}

	return nil
}
