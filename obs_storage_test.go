package obs_storage

import (
	"fmt"

	storage_test_helper "github.com/storage-lock/go-storage-test-helper"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/stretchr/testify/assert"
	"context"
	"os"
	"os/exec"
	"testing"
	"time"
)

var minioCmd *exec.Cmd

func TestMain(m *testing.M) {
	envName := "STORAGE_LOCK_OBS_ENDPOINT"
	if os.Getenv(envName) != "" {
		os.Exit(m.Run())
	}

	// 尝试用 Docker 启动 MinIO（S3 兼容，可模拟 OBS）
	port := "9010"
	consolePort := "9011"
	minioCmd = exec.Command("docker", "run", "--rm",
		"-p", port+":9000",
		"-p", consolePort+":9001",
		"-e", "MINIO_ROOT_USER=minioadmin",
		"-e", "MINIO_ROOT_PASSWORD=minioadmin",
		"--name", "obs-storage-test",
		"minio/minio:RELEASE.2024-05-28T17-19-04Z", "server", "/data", "--console-address", ":9001",
	)
	if err := minioCmd.Start(); err != nil {
		fmt.Println("docker not available, skipping OBS storage test")
		os.Exit(0)
	}

	// 等待 MinIO 就绪
	time.Sleep(time.Second * 5)
	os.Setenv("STORAGE_LOCK_OBS_ENDPOINT", "localhost:"+port)
	os.Setenv("STORAGE_LOCK_OBS_ACCESS_KEY", "minioadmin")
	os.Setenv("STORAGE_LOCK_OBS_SECRET_KEY", "minioadmin")
	os.Setenv("STORAGE_LOCK_OBS_BUCKET", "storage-lock-test")
	exitCode := m.Run()

	minioCmd.Process.Kill()
	minioCmd.Wait()
	exec.Command("docker", "rm", "-f", "obs-storage-test").Run()
	os.Exit(exitCode)
}

func TestOBSStorage(t *testing.T) {
	endpoint := os.Getenv("STORAGE_LOCK_OBS_ENDPOINT")
	if endpoint == "" {
		t.Skip("STORAGE_LOCK_OBS_ENDPOINT not set and MinIO not available")
	}
	accessKey := os.Getenv("STORAGE_LOCK_OBS_ACCESS_KEY")
	if accessKey == "" {
		accessKey = "minioadmin"
	}
	secretKey := os.Getenv("STORAGE_LOCK_OBS_SECRET_KEY")
	if secretKey == "" {
		secretKey = "minioadmin"
	}

	client, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKey, secretKey, ""),
		Secure: false,
	})
	assert.Nil(t, err)

	// 确保 bucket 存在
	bucket := os.Getenv("STORAGE_LOCK_OBS_BUCKET")
	if bucket == "" {
		bucket = "storage-lock-test"
	}
	ctx := context.Background()
	exists, err := client.BucketExists(ctx, bucket)
	assert.Nil(t, err)
	if !exists {
		err = client.MakeBucket(ctx, bucket, minio.MakeBucketOptions{})
		assert.Nil(t, err)
	}
	// 清理旧数据
	prefix := storage_test_helper.TestTableName + "/lock/"
	ch := client.ListObjects(ctx, bucket, minio.ListObjectsOptions{Prefix: prefix, Recursive: true})
	for obj := range ch {
		if obj.Err != nil {
			continue
		}
		client.RemoveObject(ctx, bucket, obj.Key, minio.RemoveObjectOptions{})
	}

	cm := NewOBSConnectionManagerFromClient(client)
	s, err := NewOBSStorage(ctx, &OBSStorageOptions{
		ConnectionManager: cm,
		BucketName:        bucket,
		KeyPrefix:         storage_test_helper.TestTableName,
		TimeProvider:      &LocalTimeProvider{},
	})
	assert.Nil(t, err)
	storage_test_helper.TestStorage(t, s)
}
