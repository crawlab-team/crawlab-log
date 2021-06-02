package log

import "time"

type Message struct {
	Id  int64     `json:"id" bson:"id"`
	Msg string    `json:"msg" bson:"msg"`
	Ts  time.Time `json:"ts" bson:"ts"`
}

type Metadata struct {
	Size       int64  `json:"size" bson:"size"`
	TotalLines int64  `json:"total_lines" bson:"total_lines"`
	TotalBytes int64  `json:"total_bytes" bson:"total_bytes"`
	Md5        string `json:"md5" bson:"md5"`
}
