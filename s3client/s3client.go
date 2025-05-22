package s3client

import (
	"bytes"
	"context"
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// S3Client handles interactions with AWS S3
type S3Client struct {
	svc *s3.Client
}

// S3Object represents an object in S3
type S3Object struct {
	Key  string
	Size int64
}

// NewS3Client creates a new S3 client using the default credential provider chain
func NewS3Client() (*S3Client, error) {
	// Load the default AWS configuration
	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS configuration: %w", err)
	}

	// Create S3 service client
	svc := s3.NewFromConfig(cfg)

	return &S3Client{svc: svc}, nil
}

// ListObjects lists objects in a bucket with a given prefix
func (c *S3Client) ListObjects(bucket, prefix string) ([]S3Object, error) {
	var objects []S3Object

	// Create the ListObjectsV2 input
	input := &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String(prefix),
	}

	// Use paginator to handle pagination
	paginator := s3.NewListObjectsV2Paginator(c.svc, input)

	// Iterate through pages
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(context.TODO())
		if err != nil {
			return nil, fmt.Errorf("error listing objects: %w", err)
		}

		for _, obj := range page.Contents {
			objects = append(objects, S3Object{
				Key:  *obj.Key,
				Size: *obj.Size,
			})
		}
	}

	return objects, nil
}

// GetObject retrieves an object from S3 and returns a reader
func (c *S3Client) GetObject(bucket, key string) (io.ReadCloser, error) {
	input := &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}

	result, err := c.svc.GetObject(context.TODO(), input)
	if err != nil {
		return nil, fmt.Errorf("error getting object %s/%s: %w", bucket, key, err)
	}

	return result.Body, nil
}

// UploadStream uploads data from a reader to S3
func (c *S3Client) UploadStream(reader io.Reader, bucket, key string) error {
	// Read the entire content into memory
	// Note: This is not ideal for very large files, but should work for most cases
	data, err := io.ReadAll(reader)
	if err != nil {
		return fmt.Errorf("error reading data: %w", err)
	}

	input := &s3.PutObjectInput{
		Bucket:        aws.String(bucket),
		Key:           aws.String(key),
		Body:          bytes.NewReader(data),
		ContentLength: aws.Int64(int64(len(data))),
	}

	_, err = c.svc.PutObject(context.TODO(), input)
	if err != nil {
		return fmt.Errorf("error uploading to %s/%s: %w", bucket, key, err)
	}

	return nil
}
