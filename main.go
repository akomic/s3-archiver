package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"github.com/akomic/s3-archiver/archiver"
	"github.com/akomic/s3-archiver/s3client"
)

func main() {
	// Define command-line flags
	awsProfile := flag.String("profile", "", "AWS profile to use")
	sourceBucket := flag.String("source-bucket", "", "Source S3 bucket name")
	sourcePrefix := flag.String("source-prefix", "", "Prefix for objects in source bucket")
	destBucket := flag.String("dest-bucket", "", "Destination S3 bucket name")
	destKey := flag.String("dest-key", "", "Destination key (path and filename) in destination bucket")
	
	flag.Parse()
	
	// Validate required arguments
	if *sourceBucket == "" || *destBucket == "" || *destKey == "" {
		flag.Usage()
		os.Exit(1)
	}
	
	// Set AWS profile if specified
	if *awsProfile != "" {
		os.Setenv("AWS_PROFILE", *awsProfile)
	}
	
	// Create S3 client
	s3Client, err := s3client.NewS3Client()
	if err != nil {
		log.Fatalf("Failed to create S3 client: %v", err)
	}
	
	// Create archiver
	arch := archiver.NewArchiver()
	
	// Process the transfer
	err = processTransfer(s3Client, arch, *sourceBucket, *sourcePrefix, *destBucket, *destKey)
	if err != nil {
		log.Fatalf("Transfer failed: %v", err)
	}
	
	fmt.Println("Transfer completed successfully")
}

func processTransfer(s3Client *s3client.S3Client, arch *archiver.Archiver, 
                    sourceBucket, sourcePrefix, destBucket, destKey string) error {
	// List objects in the source bucket with the given prefix
	objects, err := s3Client.ListObjects(sourceBucket, sourcePrefix)
	if err != nil {
		return fmt.Errorf("failed to list objects: %w", err)
	}
	
	// If no objects found, return an error
	if len(objects) == 0 {
		return fmt.Errorf("no objects found with prefix %s in bucket %s", sourcePrefix, sourceBucket)
	}
	
	fmt.Printf("Found %d objects to archive\n", len(objects))
	
	// Create a pipe to connect the archiver to the S3 uploader
	pr, pw := io.Pipe()
	
	// Start a goroutine to upload the archive to the destination bucket
	errCh := make(chan error, 1)
	go func() {
		defer close(errCh)
		err := s3Client.UploadStream(pr, destBucket, destKey)
		if err != nil {
			// Close the pipe reader to unblock any writers
			pr.CloseWithError(err)
			errCh <- err
		}
	}()
	
	// Create the archive with the objects in a separate goroutine
	go func() {
		err := arch.CreateArchive(pw, s3Client, sourceBucket, sourcePrefix, objects)
		// Always close the pipe writer when done, with or without error
		if err != nil {
			pw.CloseWithError(err)
		} else {
			pw.Close()
		}
	}()
	
	// Wait for the upload to complete
	if err := <-errCh; err != nil {
		return fmt.Errorf("failed to upload archive: %w", err)
	}
	
	return nil
}
