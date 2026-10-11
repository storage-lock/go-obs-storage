package obs_storage

import (
	"context"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/minio/minio-go/v7"
	go_iterator "github.com/golang-infrastructure/go-iterator"
	"github.com/storage-lock/go-storage"
	storage_lock "github.com/storage-lock/go-storage-lock"
)

// 存储在对象 value 中的分隔符
const valueSeparator = "\x00"

// isPreconditionError 检查是否为对象存储前置条件失败错误
func isPreconditionError(err error) bool {
	errStr := err.Error()
	return strings.Contains(errStr, "PreconditionFailed") ||
		strings.Contains(errStr, "412") ||
		strings.Contains(errStr, "pre-condition") ||
		strings.Contains(errStr, "Condition") ||
		strings.Contains(errStr, "409")
}

// OBSStorage 基于华为云 OBS 的存储引擎实现
//
// OBS 兼容 S3 协议，本实现通过 S3 兼容客户端访问 OBS。
// CAS 范式与 S3 一致：条件 PUT。
// - CreateWithVersion: PutObject + If-None-Match:*（对象不存在才写入）
// - UpdateWithVersion: 先 Get 取 ETag → PutObject + If-Match:<ETag>（ETag 匹配才覆盖）
// - DeleteWithVersion: 对象存储不支持条件删除，采用“读取-检查-无条件删除”降级方案
//
// 时间源由 options.TimeProvider 注入（生产 NTP，测试 LocalTimeProvider）。
type OBSStorage struct {
	options *OBSStorageOptions
	client  *minio.Client

	timeProvider storage.TimeProvider
}

var _ storage.Storage = &OBSStorage{}

// NewOBSStorage 创建 OBS 存储引擎
func NewOBSStorage(ctx context.Context, options *OBSStorageOptions) (*OBSStorage, error) {
	if err := options.Check(); err != nil {
		return nil, err
	}
	s := &OBSStorage{options: options}
	if err := s.Init(ctx); err != nil {
		return nil, err
	}
	return s, nil
}

const OBSStorageName = "obs-storage"

func (x *OBSStorage) GetName() string {
	return OBSStorageName
}

// Capabilities 声明 CAS 和 ReliableTime（通过注入 TimeProvider 满足）
// - AtomicDelete：对象存储 Delete 无条件删除，释放走墓碑降级
func (x *OBSStorage) Capabilities() []storage.StorageCapability {
	return []storage.StorageCapability{
		storage.CapabilityCAS,
		storage.CapabilityReliableTime,
		storage.CapabilityWatch,
	}
}

func (x *OBSStorage) Watch(ctx context.Context, lockId string) (<-chan *storage.LockInformation, error) {
	return storage.NewPollingLockWatcher(x, 500*time.Millisecond).Watch(ctx, lockId)
}

func (x *OBSStorage) Init(ctx context.Context) error {
	client, err := x.options.ConnectionManager.Take(ctx)
	if err != nil {
		return err
	}
	x.client = client

	// 确保 Bucket 存在
	exists, err := client.BucketExists(ctx, x.options.BucketName)
	if err != nil {
		return fmt.Errorf("failed to check bucket: %w", err)
	}
	if !exists {
		err = client.MakeBucket(ctx, x.options.BucketName, minio.MakeBucketOptions{Region: x.options.Region})
		if err != nil {
			return fmt.Errorf("failed to create bucket: %w", err)
		}
	}

	if x.options.TimeProvider != nil {
		x.timeProvider = x.options.TimeProvider
	} else {
		x.timeProvider = &LocalTimeProvider{}
	}
	return nil
}

// LocalTimeProvider 本地时间源（导出，供 locks 包注入）
type LocalTimeProvider struct{}

func (p *LocalTimeProvider) GetTime(ctx context.Context) (time.Time, error) {
	return time.Now(), nil
}

// objectKey 返回对象 key
func (x *OBSStorage) objectKey(lockId string) string {
	return fmt.Sprintf("%s/lock/%s", x.options.KeyPrefix, lockId)
}

// encodeValue 编码 version 和 json
func encodeValue(version storage.Version, jsonStr string) []byte {
	return []byte(strconv.FormatUint(uint64(version), 10) + valueSeparator + jsonStr)
}

// decodeValue 解码 version 和 json
func decodeValue(data []byte) (storage.Version, string, error) {
	parts := strings.SplitN(string(data), valueSeparator, 2)
	if len(parts) != 2 {
		return 0, "", fmt.Errorf("invalid obs value format")
	}
	version, err := strconv.ParseUint(parts[0], 10, 64)
	if err != nil {
		return 0, "", err
	}
	return storage.Version(version), parts[1], nil
}

// CreateWithVersion 原子创建锁记录
// PutObject + If-None-Match:*：对象不存在才写入，否则返回前置条件失败错误
func (x *OBSStorage) CreateWithVersion(ctx context.Context, lockId string, version storage.Version, lockInformation *storage.LockInformation) error {
	key := x.objectKey(lockId)
	value := encodeValue(version, lockInformation.ToJsonString())

	opts := minio.PutObjectOptions{}
	opts.SetMatchETagExcept("*") // 仅当对象不存在时写入
	_, err := x.client.PutObject(ctx, x.options.BucketName, key, strings.NewReader(string(value)), int64(len(value)), opts)
	if err != nil {
		if isPreconditionError(err) {
			return storage_lock.ErrLockAlreadyExists
		}
		return err
	}
	return nil
}

// UpdateWithVersion 原子更新锁记录
// 流程：Get 取 ETag → 检查业务 version → PutObject + If-Match:<ETag>
func (x *OBSStorage) UpdateWithVersion(ctx context.Context, lockId string, expectedVersion, newVersion storage.Version, lockInformation *storage.LockInformation) error {
	key := x.objectKey(lockId)

	// 取当前对象和 ETag
	obj, err := x.client.GetObject(ctx, x.options.BucketName, key, minio.GetObjectOptions{})
	if err != nil {
		errStr := err.Error()
		if strings.Contains(errStr, "NoSuchKey") || strings.Contains(errStr, "404") || strings.Contains(errStr, "does not exist") {
			// 记录不存在返回 ErrVersionMiss（可重试），对齐 S3 基类/memory 已修口径。
			return storage_lock.ErrVersionMiss
		}
		return err
	}
	defer obj.Close()
	data, err := io.ReadAll(obj)
	if err != nil {
		return err
	}
	stat, err := obj.Stat()
	if err != nil {
		if strings.Contains(err.Error(), "NoSuchKey") || strings.Contains(err.Error(), "404") {
			return storage_lock.ErrVersionMiss
		}
		return err
	}

	// 解析当前业务版本
	currentVersion, _, err := decodeValue(data)
	if err != nil {
		return err
	}
	if currentVersion != expectedVersion {
		return storage_lock.ErrVersionMiss
	}

	// 用 ETag 作 CAS 条件覆盖
	newValue := encodeValue(newVersion, lockInformation.ToJsonString())
	opts := minio.PutObjectOptions{}
	opts.SetMatchETag(stat.ETag) // 仅当 ETag 匹配时覆盖
	_, err = x.client.PutObject(ctx, x.options.BucketName, key, strings.NewReader(string(newValue)), int64(len(newValue)), opts)
	if err != nil {
		if isPreconditionError(err) {
			return storage_lock.ErrVersionMiss
		}
		return err
	}
	return nil
}

// DeleteWithVersion 对象存储不支持条件删除，采用“读取-检查-无条件删除”降级方案
// 注意：这不是原子操作，检查和删除之间可能有并发修改
// 但这是对象存储能做到的最佳方案，用于测试场景和清理操作
func (x *OBSStorage) DeleteWithVersion(ctx context.Context, lockId string, expectedVersion storage.Version, lockInformation *storage.LockInformation) error {
	key := x.objectKey(lockId)

	// 取当前对象
	obj, err := x.client.GetObject(ctx, x.options.BucketName, key, minio.GetObjectOptions{})
	if err != nil {
		errStr := err.Error()
		if strings.Contains(errStr, "NoSuchKey") || strings.Contains(errStr, "404") || strings.Contains(errStr, "does not exist") {
			return storage_lock.ErrLockNotFound
		}
		return err
	}
	defer obj.Close()
	data, err := io.ReadAll(obj)
	if err != nil {
		return err
	}

	// 检查版本是否匹配
	currentVersion, _, err := decodeValue(data)
	if err != nil {
		return err
	}
	if currentVersion != expectedVersion {
		return storage_lock.ErrVersionMiss
	}

	// 版本匹配，执行删除（注意：这不是原子的）
	return x.client.RemoveObject(ctx, x.options.BucketName, key, minio.RemoveObjectOptions{})
}

// Get 获取锁信息
func (x *OBSStorage) Get(ctx context.Context, lockId string) (string, error) {
	key := x.objectKey(lockId)
	obj, err := x.client.GetObject(ctx, x.options.BucketName, key, minio.GetObjectOptions{})
	if err != nil {
		errStr := err.Error()
		if strings.Contains(errStr, "NoSuchKey") || strings.Contains(errStr, "404") || strings.Contains(errStr, "does not exist") {
			return "", storage_lock.ErrLockNotFound
		}
		return "", err
	}
	defer obj.Close()
	data, err := io.ReadAll(obj)
	if err != nil {
		errStr := err.Error()
		if strings.Contains(errStr, "NoSuchKey") || strings.Contains(errStr, "404") || strings.Contains(errStr, "does not exist") {
			return "", storage_lock.ErrLockNotFound
		}
		return "", err
	}
	if len(data) == 0 {
		return "", storage_lock.ErrLockNotFound
	}
	_, jsonStr, err := decodeValue(data)
	if err != nil {
		return "", err
	}
	return jsonStr, nil
}

// GetTime 返回注入的时间源
func (x *OBSStorage) GetTime(ctx context.Context) (time.Time, error) {
	return x.timeProvider.GetTime(ctx)
}

// Close 仅清空引用，连接由 ConnectionManager 管理
func (x *OBSStorage) Close(ctx context.Context) error {
	x.client = nil
	return nil
}

// List 通过 ListObjects 枚举 <prefix>/lock/ 前缀下的所有对象
func (x *OBSStorage) List(ctx context.Context) (go_iterator.Iterator[*storage.LockInformation], error) {
	prefix := fmt.Sprintf("%s/lock/", x.options.KeyPrefix)
	slice := make([]*storage.LockInformation, 0)

	objectCh := x.client.ListObjects(ctx, x.options.BucketName, minio.ListObjectsOptions{
		Prefix:    prefix,
		Recursive: true,
	})
	for object := range objectCh {
		if object.Err != nil {
			return nil, object.Err
		}
		obj, err := x.client.GetObject(ctx, x.options.BucketName, object.Key, minio.GetObjectOptions{})
		if err != nil {
			continue
		}
		data, err := io.ReadAll(obj)
		obj.Close()
		if err != nil {
			continue
		}
		if len(data) == 0 {
			continue
		}
		_, jsonStr, err := decodeValue(data)
		if err != nil {
			continue
		}
		lockInfo, err := storage.LockInformationFromJsonString(jsonStr)
		if err != nil {
			continue
		}
		slice = append(slice, lockInfo)
	}
	return go_iterator.FromSlice(slice), nil
}
