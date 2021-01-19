package safepgx

import (
	"context"
	"fmt"
	"sync"

	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
)

// Unexported string-based type declaration required for passing to function only compile-time values (literals),
// it makes impossible to pass sql statement value builded by `fmt.Sprinf(...)` and other vulnerable to injections ways
type safeString string

func (ss safeString) String() string { return string(ss) }

/* Transaction */

type Transaction struct{ unsafe pgx.Tx }

func (tx Transaction) Unsafe() pgx.Tx { return tx.unsafe }

func (tx Transaction) Exec(ctx context.Context, sql safeString, args ...interface{}) (int64, error) {
	tag, err := tx.unsafe.Exec(ctx, sql.String(), args...)
	if err != nil {
		return 0, fmt.Errorf("exec failed: %w", err)
	}
	return int64(len(tag)), nil
}

func (tx Transaction) Query(ctx context.Context, cb func(row pgx.Row) error, sql safeString, args ...interface{}) error {
	rows, err := tx.unsafe.Query(ctx, sql.String(), args...)
	if err != nil {
		return fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		err = cb(rows)
		if err != nil {
			return fmt.Errorf("callback failed: %w", err)
		}
	}
	if err = rows.Err(); err != nil {
		return fmt.Errorf("read rows failed: %w", err)
	}
	return nil
}

/* Connection */

type Connection struct{ unsafe *pgxpool.Conn }

func (c Connection) Unsafe() *pgxpool.Conn { return c.unsafe }

func (c Connection) WithTransaction(ctx context.Context, isolation pgx.TxIsoLevel, cb func(tx Transaction) error) (err error) {
	unsafeTx, err := c.unsafe.BeginTx(ctx, pgx.TxOptions{IsoLevel: isolation})
	if err != nil {
		return fmt.Errorf("begin transaction failed: %w", err)
	}

	err = cb(Transaction{unsafeTx})
	if err != nil {
		if rollbackErr := unsafeTx.Rollback(ctx); rollbackErr != nil {
			return fmt.Errorf("rollback error: %v, cauzed by callback error: %w", rollbackErr, err)
		}
		return err
	}

	if err = unsafeTx.Commit(ctx); err != nil {
		return fmt.Errorf("commit transaction failed: %w", err)
	}
	return nil
}

/* Pool */

// Destructor - release all acquired resources, could be safely used multiple times.
// Required for impilicit way of telling user what object have some resources that should be released in the end of usage.
type Destructor func(context.Context) error

type Pool struct{ unsafe *pgxpool.Pool }

func New(unsafePool *pgxpool.Pool) (Pool, Destructor) {
	var once sync.Once

	var destructor = Destructor(func(ctx context.Context) (err error) {
		once.Do(func() {
			var closed = make(chan struct{}, 1)
			go func() {
				unsafePool.Close()
				closed <- struct{}{}
			}()

			select {
			case <-closed:
				return
			case <-ctx.Done():
				err = ctx.Err()
			}
		})
		return err
	})

	return Pool{unsafePool}, destructor
}

func (p Pool) Unsafe() *pgxpool.Pool { return p.unsafe }

func (p Pool) WithConnection(ctx context.Context, cb func(c Connection) error) error {
	unsafeConn, err := p.unsafe.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquire connection failed: %w", err)
	}
	defer unsafeConn.Release()
	return cb(Connection{unsafeConn})
}
