module github.com/crawlab-team/crawlab-log

go 1.15

replace (
	github.com/crawlab-team/crawlab-fs => /Users/marvzhang/projects/crawlab-team/crawlab-fs
	github.com/crawlab-team/goseaweedfs => /Users/marvzhang/projects/crawlab-team/goseaweedfs
)

require (
	github.com/crawlab-team/crawlab-fs v0.0.0
	github.com/crawlab-team/go-trace v0.1.0
	github.com/crawlab-team/goseaweedfs v0.1.6
	github.com/stretchr/testify v1.6.1
)
