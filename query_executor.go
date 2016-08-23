package gocql

import (
	"time"
)

type ExecutableQuery interface {
	execute(conn *Conn) *Iter
	attempt(time.Duration)
	retryPolicy() RetryPolicy
	GetRoutingKey() ([]byte, error)
	RetryableQuery
}

type queryExecutor struct {
	pool   *policyConnPool
	policy HostSelectionPolicy
}

func (q *queryExecutor) executeQuery(qry ExecutableQuery) (*Iter, error) {
	rt := qry.retryPolicy()
	hostIter := q.policy.Pick(qry)

	var iter *Iter
	hostResponse := hostIter()
	for hostResponse != nil {
		host := hostResponse.Info()
		if host == nil || !host.IsUp() {
			hostResponse = hostIter()
			continue
		}

		pool, ok := q.pool.getPool(host)
		if !ok {
			hostResponse = hostIter()
			continue
		}

		conn := pool.Pick()
		if conn == nil {
			hostResponse = hostIter()
			continue
		}

		start := time.Now()
		iter = qry.execute(conn)

		qry.attempt(time.Since(start))

		// Update host
		hostResponse.Mark(iter.err)

		switch iter.err {
		case nil:
			// Exit for loop if the query was successful
			iter.host = host
			return iter, nil
		case ErrNotFound:
			return nil, iter.err
		}

		if rt != nil {
			shouldRetry, nextHost := rt.Attempt(qry, iter.err)
			if !shouldRetry {
				break
			}
			if nextHost == false {
				// Do not iter over hosts
				continue
			}
		}
		hostResponse = hostIter()
	}

	if iter == nil {
		return nil, ErrNoConnections
	}

	return iter, nil
}
