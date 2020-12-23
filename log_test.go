package log

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestNewLogDriver(t *testing.T) {
	// create driver (fs)
	driver, err := NewLogDriver(DriverTypeFs, nil)
	require.Nil(t, err)
	require.NotNil(t, driver)

	// invalid options (fs)
	driver, err = NewLogDriver(DriverTypeFs, "1")
	require.Equal(t, ErrInvalidType, err)

	// create driver (mongo)
	driver, err = NewLogDriver(DriverTypeMongo, nil)
	require.Equal(t, ErrNotImplemented, err)

	// create driver (es)
	driver, err = NewLogDriver(DriverTypeEs, nil)
	require.Equal(t, ErrNotImplemented, err)

	// create invalid-type driver
	driver, err = NewLogDriver("invalid", nil)
	require.Equal(t, ErrInvalidType, err)
}
