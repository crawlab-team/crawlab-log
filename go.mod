module github.com/crawlab-team/crawlab-log

go 1.15

replace (
	github.com/crawlab-team/crawlab-fs => /Users/marvzhang/projects/crawlab-team/crawlab-fs
	github.com/linxGnu/goseaweedfs => /Users/marvzhang/projects/tikazyq/goseaweedfs
)

require (
	github.com/crawlab-team/crawlab-fs v0.0.0
	github.com/linxGnu/goseaweedfs v0.1.5
	github.com/spf13/viper v1.7.1
	github.com/stretchr/testify v1.6.1
)
