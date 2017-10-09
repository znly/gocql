package gocql

import (
	"time"

	"context"
)

type QueryCallback func(ctx context.Context, statement string, addr string, duration time.Duration, err error)
