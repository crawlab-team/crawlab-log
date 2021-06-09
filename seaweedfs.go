package log

import (
	"bytes"
	"crypto/md5"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	fs "github.com/crawlab-team/crawlab-fs"
	"github.com/crawlab-team/go-trace"
	"github.com/crawlab-team/goseaweedfs"
	"math"
	"strconv"
	"strings"
	"sync"
	"time"
)

// SeaweedFsLogDriver log driver for seaweedfs
// logs will be saved as chunks of files
// log chunk file remote path example: /<baseDir>/<prefix>/00000001
type SeaweedFsLogDriver struct {
	// settings
	opts *SeaweedFsLogDriverOptions // options

	// internals
	total     int64        // total lines
	count     int64        // internal count of lines logged
	buffer    bytes.Buffer // buffer of log lines written
	m         fs.Manager   // SeaweedFSManager instance
	writeLock *sync.Mutex  // write lock
	flushLock *sync.Mutex  // flush lock
	ch        chan string  // channel
	flushing  bool         // whether the log driver is flushing
}

type SeaweedFsLogDriverOptions struct {
	BaseDir          string // base directory path for log files, default: "logs"
	Prefix           string // directory prefix, default: "test"
	Size             int64  // number of lines per log chunk file, default: 1000
	Padding          int64  // log file name padding, default: 8
	FlushWaitSeconds int64  // wait time to flush buffer, default: 3
	MetadataName     string // metadata file name, set to "metadata.json"
}

func NewSeaweedFsLogDriver(options *SeaweedFsLogDriverOptions) (driver Driver, err error) {
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
	manager, err := fs.NewSeaweedFsManager()
	if err != nil {
		return driver, err
	}

	// driver
	driver = &SeaweedFsLogDriver{
		opts:      options,
		count:     0,
		m:         manager,
		writeLock: &sync.Mutex{},
		flushLock: &sync.Mutex{},
		ch:        make(chan string),
	}

	// init driver
	if err := driver.Init(); err != nil {
		return driver, err
	}

	return
}

func (d *SeaweedFsLogDriver) Init() (err error) {
	// flush handler
	go func() {
		for {
			time.Sleep(time.Duration(d.opts.FlushWaitSeconds) * time.Second)
			d.flushLock.Lock()
			_ = d.Flush()
			d.flushLock.Unlock()
		}
	}()

	// get initial metadata
	go func() {
		metadata, err := d.GetMetadata()
		if err != nil {
			return
		}
		d.total = metadata.TotalLines
	}()

	return nil
}

func (d *SeaweedFsLogDriver) Close() (err error) {
	if err := d.m.Close(); err != nil {
		return err
	}
	return nil
}

func (d *SeaweedFsLogDriver) WriteLine(line string) (err error) {
	// lock
	d.writeLock.Lock()

	// write log line to buffer
	_, err = d.buffer.WriteString(line + "\n")
	//fmt.Println(fmt.Sprintf("written: %s", line))
	if err != nil {
		return err
	}
	d.count++

	// increment total lines
	d.total++

	// unlock
	d.writeLock.Unlock()

	return nil
}

func (d *SeaweedFsLogDriver) WriteLines(lines []string) (err error) {
	for _, line := range lines {
		if err := d.WriteLine(line); err != nil {
			return err
		}
	}
	return nil
}

func (d *SeaweedFsLogDriver) Find(pattern string, skip, limit int) (lines []string, err error) {
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
		text := string(data)
		dataLines := strings.Split(text, "\n")
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

func (d *SeaweedFsLogDriver) Count(pattern string) (count int, err error) {
	metadata, err := d.GetMetadata()
	if err != nil {
		return count, err
	}
	return int(metadata.TotalLines), nil
}

func (d *SeaweedFsLogDriver) GetLastLogFilePage() (page int64, err error) {
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

func (d *SeaweedFsLogDriver) GetLastFilePath() (filePath string, err error) {
	// attempt to get page number
	page, err := d.GetLastLogFilePage()
	if err != nil {
		return filePath, err
	}

	// no file exists
	if page == -1 {
		return filePath, nil
	}

	// file path
	filePath = d.GetFilePathByPage(page)
	return
}

func (d *SeaweedFsLogDriver) GetFilePathByPage(page int64) (filePath string) {
	fileName := fmt.Sprintf("%0"+strconv.FormatInt(d.opts.Padding, 10)+"d", page)
	filePath = fmt.Sprintf("/%s/%s/%s", d.opts.BaseDir, d.opts.Prefix, fileName)
	return
}

func (d *SeaweedFsLogDriver) GetFilePathsFromSkipAndLimit(skip, limit int) (filePaths []string) {
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

func (d *SeaweedFsLogDriver) GetLogFiles() (files []goseaweedfs.FilerFileInfo, err error) {
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

func (d *SeaweedFsLogDriver) UpdateMetadata() (err error) {
	totalBytes := int64(0)
	files, err := d.GetLogFiles()
	for _, file := range files {
		totalBytes += file.FileSize
		if err != nil {
			return err
		}
	}
	md5sum, err := d.getMd5(files)
	if err != nil {
		return err
	}
	metadata := Metadata{
		Size:       d.opts.Size,
		TotalLines: d.total,
		TotalBytes: totalBytes,
		Md5:        md5sum,
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

func (d *SeaweedFsLogDriver) GetMetadata() (metadata Metadata, err error) {
	data, err := d.m.GetFile(fmt.Sprintf("/%s/%s/%s", d.opts.BaseDir, d.opts.Prefix, MetadataName))
	if err != nil {
		return metadata, err
	}
	if err := json.Unmarshal(data, &metadata); err != nil {
		return metadata, err
	}
	return
}

func (d *SeaweedFsLogDriver) getMd5(files []goseaweedfs.FilerFileInfo) (md5sum string, err error) {
	h := md5.New()
	for _, file := range files {
		data, err := d.m.GetFile(file.FullPath)
		if err != nil {
			return md5sum, err
		}
		h.Write(data)
	}
	md5sum = base64.StdEncoding.EncodeToString(h.Sum(nil))
	return md5sum, nil
}

func (d *SeaweedFsLogDriver) Flush() (err error) {
	// skip if no data in buffer
	if d.buffer.Len() == 0 {
		return nil
	}

	// log lines
	var logLines []string

	// last page
	lastPage, err := d.GetLastLogFilePage()
	if err != nil {
		return err
	}

	// last file exists
	if lastPage > -1 {
		// last file path
		lastFilePath := d.GetFilePathByPage(lastPage)

		// last file data
		lastFileData, err := d.m.GetFile(lastFilePath)
		if err != nil {
			return trace.TraceError(err)
		}

		// append to log lines
		logLines = strings.Split(string(lastFileData), "\n")
	}

	// append buffer to log lines
	for _, line := range strings.Split(d.buffer.String(), "\n") {
		logLines = append(logLines, line)
	}

	// start page
	startPage := lastPage
	if startPage == -1 {
		startPage = 0
	}

	// end page
	endPage := startPage + int64(math.Ceil(float64(len(logLines))/float64(d.opts.Size)))

	i := 0
	for page := startPage; page < endPage; page++ {
		// size
		size := int(d.opts.Size)

		// start and end indexes
		start := i * size
		end := start + size
		if end > len(logLines) {
			end = len(logLines)
		}

		// data
		data := []byte(strings.Join(logLines[start:end], "\n"))

		// skip if data is empty
		if len(data) == 0 {
			continue
		}

		// file path
		filePath := d.GetFilePathByPage(page)

		// write to fs
		if err := d.m.UpdateFile(filePath, data); err != nil {
			return err
		}

		i++
	}

	// reset buffer
	d.buffer.Reset()

	// update metadata
	if err := d.UpdateMetadata(); err != nil {
		return err
	}

	return nil
}
