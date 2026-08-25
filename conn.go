package pgx

import (
	"context"
	"errors"
	"sync"
)

var (
	// ErrTxClosed occurs when an operation is attempted on a closed transaction.
	ErrTxClosed = errors.New("tx is closed")

	// ErrClosedConn occurs when an operation is attempted on a closed connection.
	ErrClosedConn = errors.New("conn closed")
)

type CommandTag struct {
	tag string
}

func (ct CommandTag) String() string {
	return ct.tag
}

func (ct CommandTag) RowsAffected() int64 {
	return 0
}

type Row interface {
	Scan(dest ...any) error
}

type Rows interface {
	Close()
	Err() error
	Next() bool
	Scan(dest ...any) error
}

type emptyRows struct {
	err error
}

func (r *emptyRows) Close() {}
func (r *emptyRows) Err() error { return r.err }
func (r *emptyRows) Next() bool { return false }
func (r *emptyRows) Scan(dest ...any) error { return r.err }

type connRow struct {
	rows Rows
	err  error
}

func (r *connRow) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	if r.rows == nil {
		return errors.New("no rows")
	}
	defer r.rows.Close()
	if !r.rows.Next() {
		if err := r.rows.Err(); err != nil {
			return err
		}
		return errors.New("no rows in result set")
	}
	return r.rows.Scan(dest...)
}

type Conn struct {
	mu     sync.Mutex
	closed bool
}

func Connect(ctx context.Context) (*Conn, error) {
	return &Conn{}, nil
}

func (c *Conn) IsClosed() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closed
}

func (c *Conn) Close(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closed = true
	return nil
}

func (c *Conn) Begin(ctx context.Context) (Tx, error) {
	if c.IsClosed() {
		return nil, ErrClosedConn
	}
	return &tx{conn: c, ctx: ctx}, nil
}

func (c *Conn) Exec(ctx context.Context, sql string, arguments ...any) (CommandTag, error) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return CommandTag{}, ErrClosedConn
	}
	c.mu.Unlock()

	select {
	case <-ctx.Done():
		c.mu.Lock()
		c.closed = true
		c.mu.Unlock()
		return CommandTag{}, ctx.Err()
	default:
	}

	return CommandTag{tag: "OK"}, nil
}

func (c *Conn) Query(ctx context.Context, sql string, args ...any) (Rows, error) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil, ErrClosedConn
	}
	c.mu.Unlock()

	select {
	case <-ctx.Done():
		c.mu.Lock()
		c.closed = true
		c.mu.Unlock()
		return nil, ctx.Err()
	default:
	}

	return &emptyRows{}, nil
}

func (c *Conn) QueryRow(ctx context.Context, sql string, args ...any) Row {
	rows, err := c.Query(ctx, sql, args...)
	if err != nil {
		return &connRow{err: err}
	}
	return &connRow{rows: rows}
}
