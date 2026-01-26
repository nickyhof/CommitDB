// Remote file I/O support for S3 and HTTP URLs.
package db

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// s3Config contains S3 authentication configuration
type s3Config struct {
	accessKey string
	secretKey string
	region    string
	endpoint  string // Optional: custom S3-compatible endpoint
}

// urlScheme represents the scheme of a URL
type urlScheme string

const (
	schemeFile  urlScheme = "file"
	schemeS3    urlScheme = "s3"
	schemeHTTP  urlScheme = "http"
	schemeHTTPS urlScheme = "https"
	schemeLocal urlScheme = "local" // no scheme, local path
)

// detectScheme detects the URL scheme from a path string
func detectScheme(path string) urlScheme {
	lowerPath := strings.ToLower(path)
	switch {
	case strings.HasPrefix(lowerPath, "s3://"):
		return schemeS3
	case strings.HasPrefix(lowerPath, "https://"):
		return schemeHTTPS
	case strings.HasPrefix(lowerPath, "http://"):
		return schemeHTTP
	case strings.HasPrefix(lowerPath, "file://"):
		return schemeFile
	default:
		return schemeLocal
	}
}

// openRemoteReader opens a reader for the given URL/path
func openRemoteReader(path string, cfg *s3Config) (io.ReadCloser, error) {
	scheme := detectScheme(path)

	switch scheme {
	case schemeLocal, schemeFile:
		localPath := path
		if scheme == schemeFile {
			localPath = strings.TrimPrefix(path, "file://")
		}
		return osOpen(localPath)

	case schemeHTTP, schemeHTTPS:
		return openHTTPReader(path)

	case schemeS3:
		return openS3Reader(path, cfg)

	default:
		return nil, fmt.Errorf("unsupported URL scheme: %s", path)
	}
}

// openRemoteWriter opens a writer for the given URL/path
func openRemoteWriter(path string, cfg *s3Config) (io.WriteCloser, error) {
	scheme := detectScheme(path)

	switch scheme {
	case schemeLocal, schemeFile:
		localPath := path
		if scheme == schemeFile {
			localPath = strings.TrimPrefix(path, "file://")
		}
		return osCreate(localPath)

	case schemeHTTP, schemeHTTPS:
		return nil, fmt.Errorf("HTTP/HTTPS does not support writing")

	case schemeS3:
		return openS3Writer(path, cfg)

	default:
		return nil, fmt.Errorf("unsupported URL scheme: %s", path)
	}
}

// openHTTPReader opens an HTTP GET reader
func openHTTPReader(url string) (io.ReadCloser, error) {
	client := &http.Client{
		Timeout: 5 * time.Minute, // generous timeout for large files
	}

	resp, err := client.Get(url)
	if err != nil {
		return nil, fmt.Errorf("HTTP request failed: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		return nil, fmt.Errorf("HTTP request returned status %d", resp.StatusCode)
	}

	return resp.Body, nil
}

// parseS3URL parses s3://bucket/key into bucket and key parts
func parseS3URL(url string) (bucket, key string, err error) {
	path := strings.TrimPrefix(url, "s3://")
	parts := strings.SplitN(path, "/", 2)
	if len(parts) < 2 {
		return "", "", fmt.Errorf("invalid S3 URL: %s", url)
	}
	return parts[0], parts[1], nil
}

// getS3Client creates an S3 client with the given configuration
func getS3Client(ctx context.Context, cfg *s3Config) (*s3.Client, error) {
	var opts []func(*config.LoadOptions) error

	// Set region if provided
	if cfg != nil && cfg.region != "" {
		opts = append(opts, config.WithRegion(cfg.region))
	}

	// Set explicit credentials if provided
	if cfg != nil && cfg.accessKey != "" && cfg.secretKey != "" {
		creds := credentials.NewStaticCredentialsProvider(cfg.accessKey, cfg.secretKey, "")
		opts = append(opts, config.WithCredentialsProvider(creds))
	}

	awsCfg, err := config.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	clientOpts := []func(*s3.Options){}
	if cfg != nil && cfg.endpoint != "" {
		clientOpts = append(clientOpts, func(o *s3.Options) {
			o.BaseEndpoint = aws.String(cfg.endpoint)
			o.UsePathStyle = true // For S3-compatible services
		})
	}

	return s3.NewFromConfig(awsCfg, clientOpts...), nil
}

// openS3Reader opens a reader for an S3 object
func openS3Reader(url string, cfg *s3Config) (io.ReadCloser, error) {
	bucket, key, err := parseS3URL(url)
	if err != nil {
		return nil, err
	}

	ctx := context.Background()
	client, err := getS3Client(ctx, cfg)
	if err != nil {
		return nil, err
	}

	resp, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get S3 object: %w", err)
	}

	return resp.Body, nil
}

// s3Writer wraps S3 upload in a WriteCloser interface
type s3Writer struct {
	ctx    context.Context
	client *s3.Client
	bucket string
	key    string
	buffer []byte
	closed bool
}

func (w *s3Writer) Write(p []byte) (n int, err error) {
	if w.closed {
		return 0, fmt.Errorf("writer is closed")
	}
	w.buffer = append(w.buffer, p...)
	return len(p), nil
}

func (w *s3Writer) Close() error {
	if w.closed {
		return nil
	}
	w.closed = true

	// Upload the buffered content
	_, err := w.client.PutObject(w.ctx, &s3.PutObjectInput{
		Bucket: aws.String(w.bucket),
		Key:    aws.String(w.key),
		Body:   strings.NewReader(string(w.buffer)),
	})
	if err != nil {
		return fmt.Errorf("failed to upload to S3: %w", err)
	}

	return nil
}

// openS3Writer opens a writer for an S3 object
func openS3Writer(url string, cfg *s3Config) (io.WriteCloser, error) {
	bucket, key, err := parseS3URL(url)
	if err != nil {
		return nil, err
	}

	ctx := context.Background()
	client, err := getS3Client(ctx, cfg)
	if err != nil {
		return nil, err
	}

	return &s3Writer{
		ctx:    ctx,
		client: client,
		bucket: bucket,
		key:    key,
		buffer: make([]byte, 0),
	}, nil
}

// osOpen wraps os.Open - used to allow the function to be swapped in tests
var osOpen = func(path string) (io.ReadCloser, error) {
	return os.Open(path)
}

// osCreate wraps os.Create - used to allow the function to be swapped in tests
var osCreate = func(path string) (io.WriteCloser, error) {
	return os.Create(path)
}
