package obs_storage

import (
	"context"
	"fmt"
	"sync"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/storage-lock/go-storage"
)

// OBSStorageOptions 华为云 OBS 存储选项
// OBS 兼容 S3 协议，通过 S3 兼容客户端访问
type OBSStorageOptions struct {
	// ConnectionManager 连接管理器
	ConnectionManager storage.ConnectionManager[*minio.Client]
	// Endpoint OBS/S3 兼容服务的端点
	Endpoint string
	// AccessKey 访问密钥
	AccessKey string
	// SecretKey 密钥
	SecretKey string
	// UseSSL 是否使用 HTTPS
	UseSSL bool
	// BucketName 存储锁对象的 Bucket
	BucketName string
	// KeyPrefix 对象 key 前缀，实际 key = <KeyPrefix>/lock/<lockId>
	KeyPrefix string
	// Region 区域（用于 MakeBucket，可选）
	Region string
	// TimeProvider 可选时间源（对象存储无服务端时钟，必须注入）
	TimeProvider storage.TimeProvider
}

func NewOBSStorageOptions() *OBSStorageOptions {
	return &OBSStorageOptions{
		KeyPrefix: storage.DefaultStorageTableName,
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

func (x *OBSStorageOptions) SetRegion(region string) *OBSStorageOptions {
	x.Region = region
	return x
}

func (x *OBSStorageOptions) SetUseSSL(useSSL bool) *OBSStorageOptions {
	x.UseSSL = useSSL
	return x
}

func (x *OBSStorageOptions) SetTimeProvider(timeProvider storage.TimeProvider) *OBSStorageOptions {
	x.TimeProvider = timeProvider
	return x
}

func (x *OBSStorageOptions) SetConnectionManager(cm storage.ConnectionManager[*minio.Client]) *OBSStorageOptions {
	x.ConnectionManager = cm
	return x
}

func (x *OBSStorageOptions) Check() error {
	if x.KeyPrefix == "" {
		return ErrKeyPrefixEmpty
	}
	if x.BucketName == "" {
		return ErrBucketNameEmpty
	}
	if x.ConnectionManager == nil {
		return ErrConnectionManagerNil
	}
	return nil
}

// OBSConnectionManager OBS 连接管理器（S3 协议兼容）
type OBSConnectionManager struct {
	// Endpoint OBS/S3 兼容服务的端点
	Endpoint string
	// AccessKey 访问密钥
	AccessKey string
	// SecretKey 密钥
	SecretKey string
	// UseSSL 是否使用 HTTPS
	UseSSL bool
	// Region 区域
	Region string

	// presetClient 预设客户端（FromClient 构造时使用）
	presetClient *minio.Client

	initMu      sync.Mutex
	initialized bool
	client      *minio.Client
}

var _ storage.ConnectionManager[*minio.Client] = &OBSConnectionManager{}

// NewOBSConnectionManager 从端点参数创建连接管理器
func NewOBSConnectionManager(endpoint, accessKey, secretKey string) *OBSConnectionManager {
	return &OBSConnectionManager{
		Endpoint:  endpoint,
		AccessKey: accessKey,
		SecretKey: secretKey,
	}
}

// NewOBSConnectionManagerFromEndpoint 从端点参数创建连接管理器
func NewOBSConnectionManagerFromEndpoint(endpoint, accessKey, secretKey string, useSSL bool) *OBSConnectionManager {
	return &OBSConnectionManager{
		Endpoint:  endpoint,
		AccessKey: accessKey,
		SecretKey: secretKey,
		UseSSL:    useSSL,
	}
}

// NewOBSConnectionManagerFromClient 从已有客户端创建连接管理器
func NewOBSConnectionManagerFromClient(client *minio.Client) *OBSConnectionManager {
	return &OBSConnectionManager{
		presetClient: client,
		initialized:  true,
		client:       client,
	}
}

func (x *OBSConnectionManager) Name() string {
	return "obs-connection-manager"
}

func (x *OBSConnectionManager) Take(ctx context.Context) (*minio.Client, error) {
	x.initMu.Lock()
	defer x.initMu.Unlock()

	if x.initialized {
		return x.client, nil
	}

	client, err := minio.New(x.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(x.AccessKey, x.SecretKey, ""),
		Secure: x.UseSSL,
		Region: x.Region,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create OBS client: %w", err)
	}

	x.client = client
	x.initialized = true
	return x.client, nil
}

func (x *OBSConnectionManager) Return(ctx context.Context, conn *minio.Client) error {
	return nil
}

func (x *OBSConnectionManager) Shutdown(ctx context.Context) error {
	x.initMu.Lock()
	defer x.initMu.Unlock()
	x.client = nil
	x.initialized = false
	return nil
}
