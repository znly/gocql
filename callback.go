package gocql

import "time"

type QueryCallback func(string, time.Duration)
