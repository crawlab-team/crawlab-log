package log

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	fs "github.com/crawlab-team/crawlab-fs"
	"github.com/linxGnu/goseaweedfs"
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
	opts *SeaweedFSLogDriverOptions // options

	// internals
	count     int64                // internal count of lines logged
	buffer    bytes.Buffer         // buffer of log lines written
	m         *fs.SeaweedFSManager // SeaweedFSManager instance
	writeLock *sync.Mutex          // write lock
	flushLock *sync.Mutex          // flush lock
	ch        chan string          // channel
	flushing  bool                 // whether the log driver is flushing
}

type SeaweedFSLogDriverOptions struct {
	BaseDir          string // base directory path for log files, default: "logs"
	Prefix           string // directory prefix, default: "test"
	Size             int64  // number of lines per log chunk file, default: 1000
	Padding          int64  // log file name padding, default: 8
	FlushWaitSeconds int64  // wait time to flush buffer, default: 3
	MetadataName     string // metadata file name, set to "metadata.json"
}

func NewSeaweedFSLogDriver(options *SeaweedFSLogDriverOptions) (driver *SeaweedFSLogDriver, err error) {
	// normalize BaseDir
	baseDir := options.BaseDir
	if baseDir == "" {
		baseDir = "logs"
	}
	if strings.HasPrefix(baseDir, "/") {
		baseDir = baseDir[1:]
	}
	if strings.HasSuffix(baseDir, "/") {
		baseDir = baseDir[:(len(baseDir) - 1)]
	}
	options.BaseDir = baseDir

	// normalize Prefix
	prefix := options.Prefix
	if prefix == "" {
		prefix = "test"
	}
	if strings.HasPrefix(prefix, "/") {
		prefix = prefix[1:]
	}
	if strings.HasSuffix(prefix, "/") {
		prefix = prefix[:(len(prefix) - 1)]
	}
	options.Prefix = prefix

	// normalize Size
	size := options.Size
	if size == 0 {
		size = 1000
	}
	options.Size = size

	// normalize padding
	padding := options.Padding
	if padding == 0 {
		padding = 8
	}
	options.Padding = padding

	// normalize FlushWaitSeconds
	flushWaitSeconds := options.FlushWaitSeconds
	if flushWaitSeconds == 0 {
		flushWaitSeconds = 3
	}
	options.FlushWaitSeconds = flushWaitSeconds

	// fs manager
	manager, err := fs.NewSeaweedFSManager()
	if err != nil {
		return driver, err
	}

	// driver
	driver = &SeaweedFSLogDriver{
		opts:      options,
		count:     0,
		m:         manager,
		writeLock: &sync.Mutex{},
		flushLock: &sync.Mutex{},
		ch:        make(chan string),
		flushing:  false,
	}

	// init driver
	if err := driver.Init(); err != nil {
		return driver, err
	}

	return
}

func (d *SeaweedFSLogDriver) Init() (err error) {
	// flush handler
	go func() {
		for {
			select {
			case v := <-d.ch:
				if v == SignalFlush && !d.flushing {
					d.flushLock.Lock()
					d.flushing = true
					time.Sleep(time.Duration(d.opts.FlushWaitSeconds) * time.Second)
					_ = d.Flush()
					d.flushing = false
					d.flushLock.Unlock()
				}
			}
		}
	}()
	return nil
}

func (d *SeaweedFSLogDriver) Close() (err error) {
	if err := d.m.Close(); err != nil {
		return err
	}
	return nil
}

func (d *SeaweedFSLogDriver) Write(line string) (err error) {
	// lock
	d.writeLock.Lock()

	// write log line to buffer
	_, err = d.buffer.WriteString(line + "\n")
	//fmt.Println(fmt.Sprintf("written: %s", line))
	if err != nil {
		return err
	}
	d.count++

	// send flush signal
	go func() {
		d.ch <- SignalFlush
	}()

	// unlock
	d.writeLock.Unlock()

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

func (d *SeaweedFSLogDriver) Find(pattern string, skip, limit int) (lines []string, err error) {
	if pattern != "" {
		// TODO: implement
		return lines, errors.New("not implemented")
	}

	// get remote files paths
	filePaths := d.GetFilePathsFromSkipAndLimit(skip, limit)

	// iterate file paths
	k := 0
	for i, filePath := range filePaths {
		data, err := d.m.GetFile(filePath)
		if err != nil {
			return lines, err
		}
		dataLines := strings.Split(string(data), "\n")
		dataLines = dataLines[:(len(dataLines) - 1)]
		for j, line := range dataLines {
			if i == 0 && j < (skip%int(d.opts.Size)) {
				continue
			}

			lines = append(lines, line)
			k++

			if k == limit {
				return lines, nil
			}
		}
	}
	return
}

func (d *SeaweedFSLogDriver) Count(pattern string) (count int, err error) {
	return count, nil
}

func (d *SeaweedFSLogDriver) GetLastLogFilePage() (page int64, err error) {
	ok, err := d.m.Exists(fmt.Sprintf("/%s/%s", d.opts.BaseDir, d.opts.Prefix))
	if err != nil {
		return page, err
	}
	if !ok {
		return -1, nil
	}

	files, err := d.GetLogFiles()
	if err != nil {
		return page, err
	}
	lastFile := files[len(files)-1]
	page, err = strconv.ParseInt(lastFile.Name, 10, 64)
	//fmt.Println(fmt.Sprintf("page: %d, lastFile.Name: %s", page, lastFile.Name))
	if err != nil {
		return page, err
	}
	return
}

func (d *SeaweedFSLogDriver) GetLastFilePath() (filePath string, err error) {
	page, err := d.GetLastLogFilePage()
	if err != nil {
		return filePath, err
	}
	filePath = d.GetFilePathByPage(page)
	return
}

func (d *SeaweedFSLogDriver) GetFilePathByPage(page int64) (filePath string) {
	fileName := fmt.Sprintf("%0"+strconv.FormatInt(d.opts.Padding, 10)+"d", page)
	filePath = fmt.Sprintf("/%s/%s/%s", d.opts.BaseDir, d.opts.Prefix, fileName)
	return
}

func (d *SeaweedFSLogDriver) GetFilePathsFromSkipAndLimit(skip, limit int) (filePaths []string) {
	size := int(d.opts.Size)
	startPage := skip / size
	endPage := (skip+limit)/size + 1
	if ((skip + limit) % size) == 0 {
		endPage -= 1
	}
	for page := startPage; page < endPage; page++ {
		filePath := d.GetFilePathByPage(int64(page))
		filePaths = append(filePaths, filePath)
	}
	return
}

func (d *SeaweedFSLogDriver) GetLogFiles() (files []goseaweedfs.FilerFileInfo, err error) {
	_files, err := d.m.ListDir(fmt.Sprintf("/%s/%s", d.opts.BaseDir, d.opts.Prefix), false)
	if err != nil {
		return files, err
	}
	for _, file := range _files {
		if file.Name == MetadataName {
			continue
		}
		files = append(files, file)
	}
	return
}

func (d *SeaweedFSLogDriver) UpdateMetadata() (err error) {
	totalBytes := int64(0)
	files, err := d.GetLogFiles()
	for _, file := range files {
		totalBytes += file.FileSize
	}
	metadata := Metadata{
		Size:       d.opts.Size,
		TotalBytes: totalBytes,
	}
	metadataBytes, err := json.Marshal(&metadata)
	if err != nil {
		return err
	}
	if err := d.m.UpdateFile(fmt.Sprintf("/%s/%s/%s", d.opts.BaseDir, d.opts.Prefix, MetadataName), metadataBytes); err != nil {
		return err
	}
	return
}

func (d *SeaweedFSLogDriver) GetMetadata() (metadata Metadata, err error) {
	data, err := d.m.GetFile(fmt.Sprintf("/%s/%s/%s", d.opts.BaseDir, d.opts.Prefix, MetadataName))
	if err != nil {
		return metadata, err
	}
	if err := json.Unmarshal(data, &metadata); err != nil {
		return metadata, err
	}
	return
}

func (d *SeaweedFSLogDriver) Flush() (err error) {
	// skip if no data in buffer
	if d.buffer.Len() == 0 {
		return nil
	}

	// get remote file path
	filePath, err := d.GetLastFilePath()
	if err != nil {
		return err
	}

	// check if it exists
	ok, err := d.m.Exists(filePath)
	if err != nil {
		return err
	}

	// update log files
	i := int64(0)
	text := ""
	increment := false
	if ok {
		data, err := d.m.GetFile(filePath)
		if err != nil {
			return err
		}
		text = string(data)

		// lines count of last file
		dataLines := strings.Split(string(data), "\n")
		dataLines = dataLines[:(len(dataLines) - 1)]
		for _, line := range dataLines {
			text += line + "\n"
			i++
		}

		// If total lines count of last file equals to size,
		// which means it's not an incremental update,
		// and set text to empty and increment flag to false.
		// Otherwise, keep text and set increment flag to true.
		if len(dataLines) < int(d.opts.Size) {
			increment = true
		} else {
			text = ""
		}
	}
	bufferLines := strings.Split(d.buffer.String(), "\n")
	bufferLines = bufferLines[:(len(bufferLines) - 1)]
	for _, line := range bufferLines {
		text += line + "\n"
		i++
		if (i % d.opts.Size) == 0 {
			page, err := d.GetLastLogFilePage()
			if err != nil {
				return err
			}
			page += 1
			if increment {
				page -= 1
				increment = false
			}
			filePath = d.GetFilePathByPage(page)
			if err := d.m.UpdateFile(filePath, []byte(text)); err != nil {
				return err
			}
			text = ""
		}
	}
	if text != "" {
		page, err := d.GetLastLogFilePage()
		if err != nil {
			return err
		}
		page += 1
		if increment {
			page -= 1
			increment = false
		}
		filePath = d.GetFilePathByPage(page)
		if err := d.m.UpdateFile(filePath, []byte(text)); err != nil {
			return err
		}
	}

	// reset buffer
	d.buffer.Reset()

	// update metadata
	if err := d.UpdateMetadata(); err != nil {
		return err
	}

	return nil
}
