package log

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func setup(driver *SeaweedFSLogDriver) {
}

func cleanup(driver *SeaweedFSLogDriver) {
	// cleanup
}

func TestNewSeaweedFSDriver(t *testing.T) {
	_, err := NewSeaweedFSLogDriver("test")
	require.Nil(t, err)
}

func TestSeaweedFSLogDriver_GetCurrentLogFilePath(t *testing.T) {
	driver, err := NewSeaweedFSLogDriver("test")
	require.Nil(t, err)

	filePath := driver.GetCurrentLogFilePath()
	require.Equal(t, "/logs/test/00000000", filePath)
}
