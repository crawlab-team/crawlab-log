package log

import (
	"fmt"
	"github.com/stretchr/testify/require"
	"math"
	"strings"
	"sync"
	"testing"
	"time"
)

func setup(driver *SeaweedFsLogDriver) {
	_ = driver.m.DeleteDir(fmt.Sprintf("/%s/%s", driver.opts.BaseDir, driver.opts.Prefix))
}

func cleanup(driver *SeaweedFsLogDriver) {
	_ = driver.m.DeleteDir(fmt.Sprintf("/%s/%s", driver.opts.BaseDir, driver.opts.Prefix))
}

func TestNewSeaweedFSDriver(t *testing.T) {
	_, err := NewSeaweedFsLogDriver(&SeaweedFsLogDriverOptions{
		BaseDir: "logs",
		Prefix:  "test",
	})
	require.Nil(t, err)
}

func TestSeaweedFSLogDriver_Write(t *testing.T) {
	d, err := NewSeaweedFsLogDriver(&SeaweedFsLogDriverOptions{
		BaseDir: "logs",
		Prefix:  "test",
	})
	require.Nil(t, err)
	driver := d.(*SeaweedFsLogDriver)

	setup(driver)

	content0 := ""
	content1 := ""
	for i := 0; i < 10001; i++ {
		line := fmt.Sprintf("line: %d", i+1)
		err = driver.WriteLine(line)
		require.Nil(t, err)
		if i < 1000 {
			content0 += line + "\n"
		} else if 1000 <= i && i < 2000 {
			content1 += line + "\n"
		}
	}

	time.Sleep(6 * time.Second)

	ok, err := driver.m.Exists("/logs/test/00000000")
	require.Nil(t, err)
	require.True(t, ok)

	data, err := driver.m.GetFile("/logs/test/00000000")
	require.Nil(t, err)
	require.Equal(t, strings.Trim(content0, "\n"), string(data))

	ok, err = driver.m.Exists("/logs/test/00000001")
	require.Nil(t, err)
	require.True(t, ok)

	data, err = driver.m.GetFile("/logs/test/00000001")
	require.Nil(t, err)
	require.Equal(t, strings.Trim(content1, "\n"), string(data))

	files, err := driver.GetLogFiles()
	require.Nil(t, err)
	require.Equal(t, 11, len(files))

	for i := 10001; i < 20001; i++ {
		line := fmt.Sprintf("line: %d", i+1)
		err = driver.WriteLine(line)
		require.Nil(t, err)
	}

	time.Sleep(6 * time.Second)

	files, err = driver.GetLogFiles()
	require.Nil(t, err)
	require.Equal(t, 21, len(files))

	cleanup(driver)
}

func TestSeaweedFSLogDriver_WriteLines(t *testing.T) {
	d, err := NewSeaweedFsLogDriver(&SeaweedFsLogDriverOptions{
		BaseDir: "logs",
		Prefix:  "test",
	})
	require.Nil(t, err)
	driver := d.(*SeaweedFsLogDriver)

	setup(driver)

	batch := 500
	var lines []string
	for i := 0; i < 10; i++ {
		for j := 0; j < batch; j++ {
			line := fmt.Sprintf("line: %d", i*batch+j+1)
			lines = append(lines, line)
		}
		err = driver.WriteLines(lines)
		require.Nil(t, err)
		lines = []string{}
		//time.Sleep(1 * time.Second)
		time.Sleep(50 * time.Millisecond)
	}

	time.Sleep(8 * time.Second)

	files, err := driver.GetLogFiles()
	require.Nil(t, err)
	require.Equal(t, 10*batch/1000, len(files))

	cleanup(driver)
}

func TestSeaweedFSLogDriver_WriteLines_Parallel(t *testing.T) {
	d, err := NewSeaweedFsLogDriver(&SeaweedFsLogDriverOptions{
		BaseDir: "logs",
		Prefix:  "test",
	})
	require.Nil(t, err)
	driver := d.(*SeaweedFsLogDriver)

	setup(driver)

	n := 5
	wg := sync.WaitGroup{}
	wg.Add(n)

	batch := 500
	for i := 0; i < n; i++ {
		go func(i int) {
			for k := 0; k < 10; k++ {
				var lines []string
				for j := 0; j < batch; j++ {
					line := fmt.Sprintf("[%d] line: %d", i, k*batch+j+1)
					lines = append(lines, line)
				}
				err = driver.WriteLines(lines)
				time.Sleep(50 * time.Millisecond)
			}
			wg.Done()
		}(i)
	}
	wg.Wait()

	time.Sleep(8 * time.Second)

	files, err := driver.GetLogFiles()
	require.Nil(t, err)
	require.Equal(t, 10*n*batch/1000, len(files))

	cleanup(driver)
}

func TestSeaweedFSLogDriver_Find(t *testing.T) {
	d, err := NewSeaweedFsLogDriver(&SeaweedFsLogDriverOptions{
		BaseDir: "logs",
		Prefix:  "test",
	})
	require.Nil(t, err)
	driver := d.(*SeaweedFsLogDriver)

	setup(driver)

	batch := 1000
	var lines []string
	for i := 0; i < 10; i++ {
		for j := 0; j < batch; j++ {
			line := fmt.Sprintf("line: %d", i*batch+j+1)
			lines = append(lines, line)
		}
		err = driver.WriteLines(lines)
		require.Nil(t, err)
		lines = []string{}
		time.Sleep(1 * time.Second)
	}

	time.Sleep(3 * time.Second)

	lines, err = driver.Find("", 0, 10)
	require.Nil(t, err)
	require.Equal(t, 10, len(lines))
	require.Equal(t, "line: 1", lines[0])
	require.Equal(t, "line: 10", lines[len(lines)-1])

	lines, err = driver.Find("", 0, 1)
	require.Nil(t, err)
	require.Equal(t, 1, len(lines))
	require.Equal(t, "line: 1", lines[0])
	require.Equal(t, "line: 1", lines[len(lines)-1])

	lines, err = driver.Find("", 0, 1000)
	require.Nil(t, err)
	require.Equal(t, 1000, len(lines))
	require.Equal(t, "line: 1", lines[0])
	require.Equal(t, "line: 1000", lines[len(lines)-1])

	lines, err = driver.Find("", 1000, 1000)
	require.Nil(t, err)
	require.Equal(t, 1000, len(lines))
	require.Equal(t, "line: 1001", lines[0])
	require.Equal(t, "line: 2000", lines[len(lines)-1])

	lines, err = driver.Find("", 1001, 1000)
	require.Nil(t, err)
	require.Equal(t, 1000, len(lines))
	require.Equal(t, "line: 1002", lines[0])
	require.Equal(t, "line: 2001", lines[len(lines)-1])

	lines, err = driver.Find("", 1001, 999)
	require.Nil(t, err)
	require.Equal(t, 999, len(lines))
	require.Equal(t, "line: 1002", lines[0])
	require.Equal(t, "line: 2000", lines[len(lines)-1])

	lines, err = driver.Find("", 999, 2001)
	require.Nil(t, err)
	require.Equal(t, 2001, len(lines))
	require.Equal(t, "line: 1000", lines[0])
	require.Equal(t, "line: 3000", lines[len(lines)-1])

	cleanup(driver)
}

func TestSeaweedFSLogDriver_GetMetadata(t *testing.T) {
	d, err := NewSeaweedFsLogDriver(&SeaweedFsLogDriverOptions{
		BaseDir: "logs",
		Prefix:  "test",
		Size:    1000,
	})
	require.Nil(t, err)
	driver := d.(*SeaweedFsLogDriver)

	setup(driver)

	// write lines
	batch := 500
	var lines []string
	for j := 0; j < batch; j++ {
		line := fmt.Sprintf("line: %d", j+1)
		lines = append(lines, line)
	}
	err = driver.WriteLines(lines)
	require.Nil(t, err)
	err = driver.Flush()
	require.Nil(t, err)

	// test get metadata
	data, err := driver.GetMetadata()
	require.Nil(t, err)
	require.Equal(t, 1000, int(data.Size))
	require.Greater(t, int(data.TotalBytes), 0)
	require.NotEmpty(t, data.Md5)

	cleanup(driver)
}

func TestSeaweedFSLogDriver_LargeLogLine(t *testing.T) {
	d, err := NewSeaweedFsLogDriver(&SeaweedFsLogDriverOptions{
		BaseDir: "logs",
		Prefix:  "test",
	})
	require.Nil(t, err)
	driver := d.(*SeaweedFsLogDriver)

	setup(driver)

	length := 10000
	bufSize := 4096

	line := strings.Repeat("a", length)
	lineBytes := []byte(line)
	require.Greater(t, len(lineBytes), bufSize)

	// write line
	err = driver.WriteLine(line)
	require.Nil(t, err)
	err = driver.Flush()

	time.Sleep(3 * time.Second)

	// test get log lines
	ok, err := driver.m.Exists("/logs/test/00000000")
	require.Nil(t, err)
	require.True(t, ok)
	lines, err := driver.Find("", 0, 1000)
	require.Nil(t, err)
	require.Equal(t, int(math.Ceil(float64(length)/float64(bufSize)))+1, len(lines))
}
