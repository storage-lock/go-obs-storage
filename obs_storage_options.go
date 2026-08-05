package obs_storage

import (
	"context"
	"sync"

	"github.com/huaweicloud/huaweicloud-sdk-go-obs/obs"
	"github.com/storage-lock/go-storage"
)

// OBSStorageOptions 华为云 OBS 存储选项
type OBSStorageOptions struct {
	Endpoint         string
	AccessKey        string
	SecretKey        string
	BucketName       string
	KeyPrefix        string
	TimeProvider     storage.TimeProvider
	ConnectionManager storage.ConnectionManager[*obs.ObsClient]
}

func NewOBSStorageOptions() *OBSStorageOptions {
	return &OBSStorageOptions{
		KeyPrefix: "storage-lock",
	}
}

func (x *OBSStorageOptions) SetEndpoint(endpoint string) *OBSStorageOptions {
	x.Endpoint = endpoint
	return x
}

func (x *OBSStorageOptions) SetAccessKey(accessKey string) *OBSStorageOptions {
	x.AccessKey = accessKey
	return x
}

func (x *OBSStorageOptions) SetSecretKey(secretKey string) *OBSStorageOptions {
	x.SecretKey = secretKey
	return x
}

func (x *OBSStorageOptions) SetBucketName(bucketName string) *OBSStorageOptions {
	x.BucketName = bucketName
	return x
}

func (x *OBSStorageOptions) SetKeyPrefix(keyPrefix string) *OBSStorageOptions {
	x.KeyPrefix = keyPrefix
	return x
}

func (x *OBSStorageOptions) SetTimeProvider(timeProvider storage.TimeProvider) *OBSStorageOptions {
	x.TimeProvider = timeProvider
	return x
}

func (x *OBSStorageOptions) SetConnectionManager(cm storage.ConnectionManager[*obs.ObsClient]) *OBSStorageOptions {
	x.ConnectionManager = cm
	return x
}

func (x *OBSStorageOptions) Check() error {
	if x.ConnectionManager == nil && (x.Endpoint == "" || x.AccessKey == "" || x.SecretKey == "" || x.BucketName == "") {
		return ErrInvalidConfig
	}
	return nil
}

// OBSConnectionManager OBS 连接管理器
type OBSConnectionManager struct {
	Endpoint  string
	AccessKey string
	SecretKey string

	mu         sync.Mutex
	client     *obs.ObsClient
	initialized bool
}

var _ storage.ConnectionManager[*obs.ObsClient] = &OBSConnectionManager{}

func NewOBSConnectionManager(endpoint, accessKey, secretKey string) *OBSConnectionManager {
	return &OBSConnectionManager{
		Endpoint:  endpoint,
		AccessKey: accessKey,
		SecretKey: secretKey,
	}
}

func (x *OBSConnectionManager) Name() string {
	return "obs-connection-manager"
}

func (x *OBSConnectionManager) Take(ctx context.Context) (*obs.ObsClient, error) {
	x.mu.Lock()
	defer x.mu.Unlock()

	if x.initialized {
		return x.client, nil
	}

	client, err := obs.New(x.AccessKey, x.SecretKey, x.Endpoint)
	if err != nil {
		return nil, err
	}

	x.client = client
	x.initialized = true
	return x.client, nil
}

func (x *OBSConnectionManager) Return(ctx context.Context, conn *obs.ObsClient) error {
	return nil
}

func (x *OBSConnectionManager) Shutdown(ctx context.Context) error {
	x.mu.Lock()
	defer x.mu.Unlock()

	if x.client != nil {
		x.client.Close()
		x.client = nil
	}
	x.initialized = false
	return nil
}