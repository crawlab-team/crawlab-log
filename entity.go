package log

import "time"

type Message struct {
	Id  int64     `json:"id" bson:"id"`
	Msg string    `json:"msg" bson:"msg"`
	Ts  time.Time `json:"ts" bson:"ts"`
}
