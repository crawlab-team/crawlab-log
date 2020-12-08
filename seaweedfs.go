package log

import (
	"bytes"
	"fmt"
	fs "github.com/crawlab-team/crawlab-fs"
	"github.com/spf13/viper"
	"strconv"
	"strings"
	"sync"
	"time"
)

// log driver for seaweedfs
// logs will be saved as chunks of files
// log chunk file remote path example: /<baseDir>/<prefix>/00000001
type SeaweedFSLogDriver struct {
	// settings
	baseDir          string // base directory path for log files, default: "logs"
	prefix           string // directory prefix, default: "test"
	size             int64  // number of lines per log chunk file, default: 1000
	padding          int64  // log file name padding, default: 8
	flushWaitSeconds int64  // wait time to flush buffer

	// internals
	count         int64                // internal count of lines logged
	buffer        bytes.Buffer         // buffer of log lines written
	m             *fs.SeaweedFSManager // SeaweedFSManager instance
	lock          sync.Mutex
	readyForFlush bool
}

func NewSeaweedFSLogDriver(prefix string) (driver *SeaweedFSLogDriver, err error) {
	if strings.HasPrefix(prefix, "/") {
		prefix = prefix[1:]
	}
	if strings.HasSuffix(prefix, "/") {
		prefix = prefix[:(len(prefix) - 1)]
	}
	baseDir := viper.GetString("log.seaweedfs.baseDir")
	if baseDir == "" {
		baseDir = "logs"
	}
	if strings.HasPrefix(baseDir, "/") {
		baseDir = baseDir[1:]
	}
	if strings.HasSuffix(baseDir, "/") {
		baseDir = baseDir[:(len(baseDir) - 1)]
	}
	size := viper.GetInt64("log.seaweedfs.size")
	if size == 0 {
		size = 1000
	}
	padding := viper.GetInt64("log.seaweedfs.padding")
	if padding == 0 {
		padding = 8
	}
	flushWaitSeconds := viper.GetInt64("log.seaweedfs.flushWaitSeconds")
	if flushWaitSeconds == 0 {
		flushWaitSeconds = 5
	}
	manager, err := fs.NewSeaweedFSManager()
	if err != nil {
		return driver, err
	}
	driver = &SeaweedFSLogDriver{
		baseDir:          baseDir,
		prefix:           prefix,
		size:             size,
		padding:          padding,
		flushWaitSeconds: flushWaitSeconds,
		count:            0,
		m:                manager,
		readyForFlush:    false,
	}
	if err := driver.Init(); err != nil {
		return driver, err
	}
	return
}

func (d *SeaweedFSLogDriver) Init() (err error) {
	return nil
}

func (d *SeaweedFSLogDriver) Close() (err error) {
	if err := d.m.Close(); err != nil {
		return err
	}
	return nil
}

func (d *SeaweedFSLogDriver) Write(line string) (err error) {
	d.lock.Lock()

	// write log line to buffer
	_, err = d.buffer.WriteString(line)
	if err != nil {
		return err
	}
	d.count++

	if d.readyForFlush {
		// if ready for flush, make a goroutine to flush buffer n seconds later
		d.readyForFlush = false
		chErr := make(chan error)
		go func() {
			// wait for a period of time before flushing
			time.Sleep(time.Duration(d.flushWaitSeconds) * time.Second)

			// flush buffer
			if err := d.Flush(); err != nil {
				chErr <- err
			}
		}()
		err = <-chErr // blocking
		if err != nil {
			return err
		}
	}

	d.lock.Unlock()

	return nil
}

func (d *SeaweedFSLogDriver) WriteLines(lines []string) (err error) {
	for _, line := range lines {
		if err := d.Write(line); err != nil {
			return err
		}
	}
	return nil
}

func (d *SeaweedFSLogDriver) Find(pattern string, skip int, limit int) (lines []string, err error) {
	return lines, nil
}

func (d *SeaweedFSLogDriver) Count(pattern string) (count int, err error) {
	return count, nil
}

func (d *SeaweedFSLogDriver) GetCurrentLogFilePath() (filePath string) {
	page := d.count / d.size
	fileName := fmt.Sprintf("%0"+strconv.FormatInt(d.padding, 10)+"d", page)
	filePath = fmt.Sprintf("/%s/%s/%s", d.baseDir, d.prefix, fileName)
	return
}

func (d *SeaweedFSLogDriver) IsReadyForFlush() (ok bool, err error) {
	return
}

func (d *SeaweedFSLogDriver) Flush() (err error) {
	// remote file path
	filePath := d.GetCurrentLogFilePath()

	// check whether it exists
	ok, err := d.m.Exists(filePath)
	if err != nil {
		return err
	}

	if !ok {
		// not exists
	}

	// reset count
	d.count = 0
	return nil
}
