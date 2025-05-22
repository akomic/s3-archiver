package archiver

import (
	"archive/zip"
	"compress/flate"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/akomic/s3-archiver/s3client"
)

// Archiver handles archive creation
type Archiver struct{}

// NewArchiver creates a new archiver
func NewArchiver() *Archiver {
	return &Archiver{}
}

// CreateArchive creates a zip archive with minimal compression
func (a *Archiver) CreateArchive(w io.Writer, s3Client *s3client.S3Client, bucket string, sourcePrefix string, objects []s3client.S3Object) error {
	// Create a new zip writer
	zipWriter := zip.NewWriter(w)
	
	// Set compression level to lowest (fastest)
	zipWriter.RegisterCompressor(zip.Deflate, func(out io.Writer) (io.WriteCloser, error) {
		return flate.NewWriter(out, flate.BestSpeed) // BestSpeed is the lowest compression level
	})

	// Process each object
	for _, obj := range objects {
		// Get the object from S3
		reader, err := s3Client.GetObject(bucket, obj.Key)
		if err != nil {
			return fmt.Errorf("failed to get object %s: %w", obj.Key, err)
		}

		// Determine the path in the zip file
		// If the key starts with the sourcePrefix, remove it to make paths cleaner
		zipPath := obj.Key
		if sourcePrefix != "" && strings.HasPrefix(zipPath, sourcePrefix) {
			zipPath = zipPath[len(sourcePrefix):]
			// Remove leading slash if present
			zipPath = strings.TrimPrefix(zipPath, "/")
		}

		// Create a file header
		header := &zip.FileHeader{
			Name:     zipPath,
			Method:   zip.Deflate, // Use Deflate with lowest compression
			Modified: time.Now(),
		}

		// Create a file in the archive
		writer, err := zipWriter.CreateHeader(header)
		if err != nil {
			reader.Close()
			return fmt.Errorf("failed to create file in archive: %w", err)
		}

		// Copy the object data directly to the archive
		_, err = io.Copy(writer, reader)
		reader.Close()
		if err != nil {
			return fmt.Errorf("failed to copy data to archive: %w", err)
		}
	}

	// Close the archive to finalize it
	if err := zipWriter.Close(); err != nil {
		return fmt.Errorf("failed to close zip writer: %w", err)
	}

	return nil
}
