package gocql

import (
	"context"
	"time"
)

type ExecutableQuery interface {
	execute(conn *Conn) *Iter
	attempt(keyspace string, end, start time.Time, iter *Iter, host *HostInfo)
	retrier() Retrier
	GetRoutingKey() ([]byte, error)
	Keyspace() string
	RetryableQuery
}

type queryExecutor struct {
	pool   *policyConnPool
	policy HostSelectionPolicy
}

func (q *queryExecutor) attemptQuery(qry ExecutableQuery, conn *Conn) *Iter {
	start := time.Now()
	iter := qry.execute(conn)
	end := time.Now()

	qry.attempt(q.pool.keyspace, end, start, iter, conn.host)

	return iter
}

func filterMarkError(err error) error {
	switch err {
	case context.Canceled, context.DeadlineExceeded:
		return nil
	default:
		return err
	}
}

func (q *queryExecutor) executeQuery(qry ExecutableQuery) (*Iter, error) {
	rt := qry.retrier()
	hostIter := q.policy.Pick(qry)

	var iter *Iter

	for hostResponse := hostIter(); hostResponse != nil; hostResponse = hostIter() {
		host := hostResponse.Info()
		if host == nil || !host.IsUp() {
			continue
		}

		pool, ok := q.pool.getPool(host)
		if !ok {
			continue
		}

		conn := pool.Pick()
		if conn == nil {
			continue
		}

		iter = q.attemptQuery(qry, conn)
		// Update host
		hostResponse.Mark(filterMarkError(iter.err))
		if iter.err == nil {
			iter.host = host
			return iter, nil
		}

		if rt == nil {
			iter.host = host
			break
		}

	retry_loop:
		for {
			switch rt.Retry(qry, iter.err) {
			case Rethrow:
				return nil, iter.err
			case Ignore:
				return iter, nil
			case RetryNextHost:
				break retry_loop
			default:
				break retry_loop
			}

			iter = q.attemptQuery(qry, conn)
			hostResponse.Mark(filterMarkError(iter.err))
			if iter.err == nil {
				iter.host = host
				return iter, nil
			}
		}
	}

	if iter == nil {
		return nil, ErrNoConnections
	}

	return iter, nil
}
