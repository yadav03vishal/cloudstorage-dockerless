package cloudstorage

import (
	"context"
	"fmt"
	"io" 
	"os"
	"path/filepath"
	"time"

	"cloud.google.com/go/storage" 
	"github.com/comfforts/errors"
	"github.com/comfforts/logger"
	"go.uber.org/zap"
	"google.golang.org/api/iterator"
)

type CloudStorage interface {
	// UploadFile uploads file to given cloud bucket & filepath, creates a new one or replaces existing
	UploadFile(context.Context, io.Reader, CloudFileRequest) (int64, error)
	// DownloadFile copies content of file at given cloud bucket & filepath to given file
	DownloadFile(context.Context, io.Writer, CloudFileRequest) (int64, error)
	// Reads file data of givine length at given offset
	ReadAt(ctx context.Context, cfr CloudFileRequest, p []byte, off int64) (int, error)
	// ListObjects lists objects at given cloud bucket
	ListObjects(context.Context, CloudFileRequest) ([]string, error)
	// DeleteObject delete file at given cloud bucket & filepath
	DeleteObject(context.Context, CloudFileRequest) error
	// DeleteObjects delete files at given cloud bucket
	DeleteObjects(context.Context, CloudFileRequest) error
	// Close closes storage client connections
	Close() error
}

const (
	ERROR_CREATING_STORAGE_CLIENT string = "error creating storage client"
	ERROR_LISTING_OBJECTS         string = "error listing storage bucket objects"
	ERROR_DELETING_OBJECT         string = "error deleting storage bucket object"
	ERROR_DELETING_OBJECTS        string = "error deleting storage bucket objects"
	ERROR_MISSING_BUCKET_NAME     string = "bucket name missing"
	ERROR_MISSING_FILE_PATH       string = "file path missing"
	ERROR_MISSING_FILE_NAME       string = "file name missing"
	ERROR_STALE_UPLOAD            string = "storage bucket object has updates"
	ERROR_STALE_DOWNLOAD          string = "file object has updates"
)

var (
	ErrBucketNameMissing = errors.NewAppError(ERROR_MISSING_BUCKET_NAME)
	ErrFilePathMissing   = errors.NewAppError(ERROR_MISSING_FILE_PATH)
	ErrFileNameMissing   = errors.NewAppError(ERROR_MISSING_FILE_NAME)
)

type BufferSize int64

const (
	OneKB               BufferSize = 1024      // 1KB
	ThirtyTwoKB         BufferSize = 32 * 1024 // 32KB
	DEFAULT_BUFFER_SIZE            = OneKB
)

type CloudStorageClientConfig struct {
	CredsPath string `json:"creds_path"`
}

type cloudStorageClient struct {
	client *storage.Client
	config CloudStorageClientConfig
	logger logger.AppLogger
}

type GCPStorageReadAtAdaptor struct {
	Reader *storage.Reader
}

func (ra *GCPStorageReadAtAdaptor) ReadAt(p []byte, off int64) (n int, err error) {
	// Seek to the desired offset
	_, err = io.CopyN(io.Discard, ra.Reader, off)
	if err != nil {
		return 0, err
	}

	// Read the requested data
	return ra.Reader.Read(p)
}

// NewCloudStorageClient takes client config & logger, returns cloud storage client
func NewCloudStorageClient(cfg CloudStorageClientConfig, logger logger.AppLogger) (*cloudStorageClient, error) {
	if logger == nil {
		return nil, errors.NewAppError(errors.ERROR_MISSING_REQUIRED)
	}
	os.Setenv("GOOGLE_APPLICATION_CREDENTIALS", cfg.CredsPath)
	client, err := storage.NewClient(context.Background())
	if err != nil {
		logger.Error(ERROR_CREATING_STORAGE_CLIENT, zap.Error(err))
		return nil, errors.WrapError(err, ERROR_CREATING_STORAGE_CLIENT)
	}

	loaderClient := &cloudStorageClient{
		client: client,
		config: cfg,
		logger: logger,
	}

	return loaderClient, nil
}

type CloudFileRequest struct {
	bucket  string
	file    string
	path    string
	modTime int64
}

// NewCloudFileRequest takes bucket name, file name & filepath, return cloud storage request
func NewCloudFileRequest(bucketName, fileName, path string, modTime int64) (CloudFileRequest, error) {
	if bucketName == "" {
		return CloudFileRequest{}, ErrBucketNameMissing
	}
	return CloudFileRequest{
		bucket:  bucketName,
		file:    fileName,
		path:    path,
		modTime: modTime,
	}, nil
}

func (cs *cloudStorageClient) ReadAt(ctx context.Context, cfr CloudFileRequest, p []byte, off int64) (int, error) {
	if cfr.file == "" {
		return 0, ErrFileNameMissing
	}

	if cfr.bucket == "" {
		return 0, ErrBucketNameMissing
	}

	fPath := cfr.file
	if cfr.path != "" {
		fPath = filepath.Join(cfr.path, cfr.file)
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// check for object existence
	obj := cs.client.Bucket(cfr.bucket).Object(fPath)
	attrs, err := obj.Attrs(ctx)
	if err != nil {
		cs.logger.Error("cloud file inaccessible", zap.Error(err), zap.String("filepath", fPath))
		return 0, errors.WrapError(err, "cloud file inaccessible %s", fPath)
	}
	cs.logger.Debug("reading cloud file chunk", zap.String("filepath", fPath), zap.Int64("created", attrs.Created.Unix()), zap.Int64("updated", attrs.Updated.Unix()))

	// open a reader for the object in the bucket
	rc, err := obj.NewReader(ctx)
	if err != nil {
		cs.logger.Error("error reading cloud file", zap.Error(err), zap.String("filepath", fPath))
		return 0, errors.WrapError(err, "error reading cloud file %s", fPath)
	}
	rcReadAt := &GCPStorageReadAtAdaptor{rc}
	defer func() {
		if err := rcReadAt.Reader.Close(); err != nil {
			cs.logger.Error("error closing cloud file reader", zap.Error(err), zap.String("filepath", fPath))
		}
	}()

	return rcReadAt.ReadAt(p, off)
}

func (cs *cloudStorageClient) UploadFile(ct context.Context, file io.Reader, cfr CloudFileRequest) (int64, error) {
	if cfr.file == "" {
		return 0, ErrFileNameMissing
	}
	fPath := cfr.file
	if cfr.path != "" {
		fPath = filepath.Join(cfr.path, cfr.file)
	}

	ctx, cancel := context.WithTimeout(ct, time.Second*50)
	defer cancel()

	// Upload an object with storage.Writer.
	obj := cs.client.Bucket(cfr.bucket).Object(fPath)
	attrs, err := obj.Attrs(ctx)
	if err != nil {
		cs.logger.Debug("cloud file doesn't exist, will create new", zap.String("filepath", fPath))
	} else {
		cs.logger.Debug("cloud file exists", zap.Int64("created", attrs.Created.Unix()), zap.Int64("updated", attrs.Updated.Unix()), zap.String("filepath", fPath))
	}

	wc := obj.NewWriter(ctx)
	defer func() {
		if err := wc.Close(); err != nil {
			cs.logger.Error("error closing cloud file", zap.Error(err), zap.String("filepath", fPath))
		}
	}()

	nBytes, err := io.Copy(wc, file)
	if err != nil {
		cs.logger.Error("error uploading file", zap.Error(err), zap.String("filepath", fPath))
		return 0, errors.WrapError(err, "error uploading file %s", fPath)
	}
	cs.logger.Debug("cloud file created/updated", zap.String("filepath", fPath))
	return nBytes, nil
}

func (cs *cloudStorageClient) DownloadFile(ct context.Context, file io.Writer, cfr CloudFileRequest) (int64, error) {
	if cfr.file == "" {
		return 0, ErrFileNameMissing
	}
	fPath := cfr.file
	if cfr.path != "" {
		fPath = filepath.Join(cfr.path, cfr.file)
	}

	ctx, cancel := context.WithTimeout(ct, time.Second*50)
	defer cancel()

	// download an object with storage.Reader.
	obj := cs.client.Bucket(cfr.bucket).Object(fPath)
	attrs, err := obj.Attrs(ctx)
	if err != nil {
		cs.logger.Error("cloud file inaccessible", zap.Error(err), zap.String("filepath", fPath))
		return 0, errors.WrapError(err, "cloud file inaccessible %s", fPath)
	}
	cs.logger.Debug("downloading cloud file", zap.String("filepath", fPath), zap.Int64("created", attrs.Created.Unix()), zap.Int64("updated", attrs.Updated.Unix()))

	rc, err := obj.NewReader(ctx)
	if err != nil {
		cs.logger.Error("error reading cloud file", zap.Error(err), zap.String("filepath", fPath))
		return 0, errors.WrapError(err, "error reading cloud file %s", fPath)
	}
	defer func() {
		if err := rc.Close(); err != nil {
			cs.logger.Error("error closing cloud file", zap.Error(err), zap.String("filepath", fPath))
		}
	}()

	nBytes, err := io.Copy(file, rc)
	if err != nil {
		cs.logger.Error("error copying cloud file", zap.Error(err), zap.String("filepath", fPath))
		return 0, errors.WrapError(err, "error copying cloud file %s", fPath)
	}

	return nBytes, nil
}

func (cs *cloudStorageClient) ListObjects(ctx context.Context, req CloudFileRequest) ([]string, error) {
	if req.bucket == "" {
		return nil, ErrBucketNameMissing
	}

	bucket := cs.client.Bucket(req.bucket)
	it := bucket.Objects(ctx, nil)
	names := []string{}
	for {
		objAttrs, err := it.Next()
		if err != nil {
			if err == iterator.Done {
				break
			} else {
				cs.logger.Error(ERROR_LISTING_OBJECTS, zap.Error(err))
				return names, errors.WrapError(err, ERROR_LISTING_OBJECTS)
			}
		}
		names = append(names, objAttrs.Name)
	}
	return names, nil
}

func (cs *cloudStorageClient) DeleteObject(ctx context.Context, req CloudFileRequest) error {
	if req.bucket == "" {
		return ErrBucketNameMissing
	}
	if req.path == "" {
		return ErrFilePathMissing
	}
	if req.file == "" {
		return ErrFileNameMissing
	}

	bucket := cs.client.Bucket(req.bucket)
	objName := fmt.Sprintf("%s/%s", req.path, req.file)

	if err := bucket.Object(objName).Delete(ctx); err != nil {
		cs.logger.Error(ERROR_DELETING_OBJECT, zap.Error(err))
		return errors.WrapError(err, ERROR_DELETING_OBJECT)
	}
	return nil
}

func (cs *cloudStorageClient) DeleteObjects(ctx context.Context, req CloudFileRequest) error {
	if req.bucket == "" {
		return ErrBucketNameMissing
	}
	bucket := cs.client.Bucket(req.bucket)
	it := bucket.Objects(ctx, nil)
	for {
		objAttrs, err := it.Next()
		if err != nil {
			if err == iterator.Done {
				break
			} else {
				cs.logger.Error(ERROR_LISTING_OBJECTS, zap.Error(err))
				return errors.WrapError(err, ERROR_LISTING_OBJECTS)
			}
		}
		cs.logger.Info("object attributes", zap.Any("objAttrs", objAttrs))
		if err := bucket.Object(objAttrs.Name).Delete(ctx); err != nil {
			cs.logger.Error(ERROR_DELETING_OBJECTS, zap.Error(err))
			return errors.WrapError(err, ERROR_DELETING_OBJECTS)
		}
	}
	return nil
}

func (cs *cloudStorageClient) Close() error {
	err := cs.client.Close()
	if err != nil {
		cs.logger.Error("error closing storage client", zap.Error(err))
		return errors.WrapError(err, "error closing storage client")
	}
	return nil
}
