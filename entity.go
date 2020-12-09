package log

import "time"

type Message struct {
	Id  int64     `json:"id" bson:"id"`
	Msg string    `json:"msg" bson:"msg"`
	Ts  time.Time `json:"ts" bson:"ts"`
}

type Metadata struct {
	Size       int64 `json:"size" bson:"size"`
	TotalBytes int64 `json:"total_bytes" bson:"total_bytes"`
}
