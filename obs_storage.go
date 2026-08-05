package obs_storage

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/huaweicloud/huaweicloud-sdk-go-obs/obs"
	go_iterator "github.com/golang-infrastructure/go-iterator"
	"github.com/storage-lock/go-storage"
	storage_lock "github.com/storage-lock/go-storage-lock"
)

const valueSeparator = "\x00"

// OBSStorage 基于华为云 OBS 的存储引擎实现
// OBS 支持条件 PUT，但不支持原子条件删除，释放走墓碑降级路径
type OBSStorage struct {
	options      *OBSStorageOptions
	client       *obs.ObsClient
	bucket       string
	timeProvider storage.TimeProvider
}

var _ storage.Storage = &OBSStorage{}

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

func (x *OBSStorage) Capabilities() []storage.StorageCapability {
	return []storage.StorageCapability{
		storage.CapabilityCAS,
		storage.CapabilityReliableTime,
	}
}

func (x *OBSStorage) Init(ctx context.Context) error {
	client, err := x.options.ConnectionManager.Take(ctx)
	if err != nil {
		return err
	}
	x.client = client
	x.bucket = x.options.BucketName

	if x.options.TimeProvider != nil {
		x.timeProvider = x.options.TimeProvider
	} else {
		x.timeProvider = &LocalTimeProvider{}
	}
	return nil
}

type LocalTimeProvider struct{}

func (p *LocalTimeProvider) GetTime(ctx context.Context) (time.Time, error) {
	return time.Now(), nil
}

func (x *OBSStorage) objectKey(lockId string) string {
	return fmt.Sprintf("%s/lock/%s", x.options.KeyPrefix, lockId)
}

func encodeValue(version storage.Version, jsonStr string) []byte {
	return []byte(strconv.FormatUint(uint64(version), 10) + valueSeparator + jsonStr)
}

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
// OBS 条件 PUT：使用 If-None-Match: * 防止覆盖已存在的对象
func (x *OBSStorage) CreateWithVersion(ctx context.Context, lockId string, version storage.Version, lockInformation *storage.LockInformation) error {
	key := x.objectKey(lockId)
	value := encodeValue(version, lockInformation.ToJsonString())

	// OBS 条件 PUT：If-None-Match: * 表示对象不存在才写入
	input := &obs.PutObjectInput{
		Bucket: x.bucket,
		Key:    key,
		Body:   bytes.NewReader(value),
	}
	input.SetIfNoneMatch("*")

	_, err := x.client.PutObject(input)
	if err != nil {
		// OBS 返回 412 PreconditionFailed 或 409 Conflict 表示对象已存在
		if isPreconditionError(err) {
			return storage_lock.ErrLockAlreadyExists
		}
		return err
	}
	return nil
}

// UpdateWithVersion 原子更新锁记录
// OBS 条件 PUT：先 GetObject 获取 ETag，再 PutObject + If-Match:<ETag>
func (x *OBSStorage) UpdateWithVersion(ctx context.Context, lockId string, expectedVersion, newVersion storage.Version, lockInformation *storage.LockInformation) error {
	key := x.objectKey(lockId)

	// 1. GetObject 获取当前版本和 ETag
	getInput := &obs.GetObjectInput{
		Bucket: x.bucket,
		Key:    key,
	}
	getOutput, err := x.client.GetObject(getInput)
	if err != nil {
		if isNotFoundError(err) {
			return storage_lock.ErrLockNotFound
		}
		return err
	}
	defer getOutput.Body.Close()

	data, err := io.ReadAll(getOutput.Body)
	if err != nil {
		return err
	}

	currentVersion, _, err := decodeValue(data)
	if err != nil {
		return err
	}

	// 检查版本
	if currentVersion != expectedVersion {
		return storage_lock.ErrVersionMiss
	}

	etag := getOutput.ETag
	value := encodeValue(newVersion, lockInformation.ToJsonString())

	// 2. PutObject + If-Match:<ETag>
	putInput := &obs.PutObjectInput{
		Bucket: x.bucket,
		Key:    key,
		Body:   bytes.NewReader(value),
	}
	putInput.SetIfMatch(etag)

	_, err = x.client.PutObject(putInput)
	if err != nil {
		if isPreconditionError(err) {
			return storage_lock.ErrVersionMiss
		}
		return err
	}
	return nil
}

// DeleteWithVersion OBS 不支持条件删除，返回 ErrVersionMiss 触发墓碑路径
func (x *OBSStorage) DeleteWithVersion(ctx context.Context, lockId string, expectedVersion storage.Version, lockInformation *storage.LockInformation) error {
	return storage_lock.ErrVersionMiss
}

// Get 获取锁记录
func (x *OBSStorage) Get(ctx context.Context, lockId string) (string, error) {
	key := x.objectKey(lockId)

	input := &obs.GetObjectInput{
		Bucket: x.bucket,
		Key:    key,
	}
	output, err := x.client.GetObject(input)
	if err != nil {
		if isNotFoundError(err) {
			return "", storage_lock.ErrLockNotFound
		}
		return "", err
	}
	defer output.Body.Close()

	data, err := io.ReadAll(output.Body)
	if err != nil {
		return "", err
	}

	_, jsonStr, err := decodeValue(data)
	if err != nil {
		return "", err
	}
	return jsonStr, nil
}

func (x *OBSStorage) GetTime(ctx context.Context) (time.Time, error) {
	return x.timeProvider.GetTime(ctx)
}

func (x *OBSStorage) Close(ctx context.Context) error {
	if x.client != nil {
		x.client.Close()
	}
	return nil
}

// List 列出所有锁记录
func (x *OBSStorage) List(ctx context.Context) (go_iterator.Iterator[*storage.LockInformation], error) {
	prefix := x.options.KeyPrefix + "/lock/"

	input := &obs.ListObjectsInput{
		Bucket: x.bucket,
		Prefix: prefix,
	}

	result, err := x.client.ListObjects(input)
	if err != nil {
		return nil, err
	}

	slice := make([]*storage.LockInformation, 0)
	for _, obj := range result.Contents {
		// 获取对象内容
		getInput := &obs.GetObjectInput{
			Bucket: x.bucket,
			Key:    obj.Key,
		}
		getOutput, err := x.client.GetObject(getInput)
		if err != nil {
			continue
		}

		data, err := io.ReadAll(getOutput.Body)
		getOutput.Body.Close()
		if err != nil {
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

// isPreconditionError 检查是否为 OBS 前置条件失败错误
func isPreconditionError(err error) bool {
	errStr := err.Error()
	return strings.Contains(errStr, "PreconditionFailed") ||
		strings.Contains(errStr, "412") ||
		strings.Contains(errStr, "Condition") ||
		strings.Contains(errStr, "409")
}

// isNotFoundError 检查是否为对象不存在错误
func isNotFoundError(err error) bool {
	errStr := err.Error()
	return strings.Contains(errStr, "404") ||
		strings.Contains(errStr, "NoSuchKey") ||
		strings.Contains(errStr, "Not Found")
}