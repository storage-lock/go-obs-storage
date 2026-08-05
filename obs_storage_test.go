package obs_storage

import (
	"context"
	"os"
	"testing"

	storage_test_helper "github.com/storage-lock/go-storage-test-helper"
	"github.com/stretchr/testify/assert"
)

func TestOBSStorage(t *testing.T) {
	endpoint := os.Getenv("STORAGE_LOCK_OBS_ENDPOINT")
	accessKey := os.Getenv("STORAGE_LOCK_OBS_ACCESS_KEY")
	secretKey := os.Getenv("STORAGE_LOCK_OBS_SECRET_KEY")
	bucket := os.Getenv("STORAGE_LOCK_OBS_BUCKET")

	if endpoint == "" || accessKey == "" || secretKey == "" || bucket == "" {
		t.Skip("OBS credentials not set, skipping test")
	}

	ctx := context.Background()
	cm := NewOBSConnectionManager(endpoint, accessKey, secretKey)
	s, err := NewOBSStorage(ctx, &OBSStorageOptions{
		ConnectionManager: cm,
		BucketName:        bucket,
		KeyPrefix:         storage_test_helper.TestTableName,
	})
	assert.Nil(t, err)
	storage_test_helper.TestStorage(t, s)
}
