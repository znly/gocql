package gocql

import (
	"time"

	"golang.org/x/net/context"
)

type QueryCallback func(ctx context.Context, statement string, addr string, duration time.Duration)
