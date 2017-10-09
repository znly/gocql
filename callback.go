package gocql

import (
	"time"

	"context"
)

type QueryCallback func(context.Context, string, *HostInfo, time.Duration, error)
