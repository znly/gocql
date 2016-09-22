package gocql

import (
	"time"

	"golang.org/x/net/context"
)

type QueryCallback func(context.Context, string, time.Duration)
