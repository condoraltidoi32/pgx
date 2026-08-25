package pgx

import (
	"context"
	"errors"
)

type Tx interface {
	Begin(ctx context.Context) (Tx, error)
	Commit(ctx context.Context) error
	Rollback(ctx context.Context) error
	Exec(ctx context.Context, sql string, arguments ...any) (commandTag CommandTag, err error)
	Query(ctx context.Context, sql string, args ...any) (Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) Row
	Conn() *Conn
}

type tx struct {
	conn   *Conn
	ctx    context.Context
	closed bool
}

func (tx *tx) Begin(ctx context.Context) (Tx, error) {
	if tx.closed {
		return nil, ErrTxClosed
	}
	if tx.conn.IsClosed() {
		tx.closed = true
		return nil, ErrTxClosed
	}
	return &tx{conn: tx.conn, ctx: ctx}, nil
}

func (tx *tx) Commit(ctx context.Context) error {
	if tx.closed {
		return ErrTxClosed
	}
	tx.closed = true

	if tx.conn.IsClosed() {
		return ErrTxClosed
	}

	_, err := tx.conn.Exec(ctx, "commit")
	if err != nil {
		if tx.conn.IsClosed() || errors.Is(err, ErrClosedConn) {
			return ErrTxClosed
		}
		return err
	}
	return nil
}

func (tx *tx) Rollback(ctx context.Context) error {
	if tx.closed {
		return ErrTxClosed
	}
	tx.closed = true

	if tx.conn.IsClosed() {
		return ErrTxClosed
	}

	_, err := tx.conn.Exec(ctx, "rollback")
	if err != nil {
		if tx.conn.IsClosed() || errors.Is(err, ErrClosedConn) {
			return ErrTxClosed
		}
		return err
	}
	return nil
}

func (tx *tx) Exec(ctx context.Context, sql string, arguments ...any) (commandTag CommandTag, err error) {
	if tx.closed {
		return CommandTag{}, ErrTxClosed
	}

	tag, err := tx.conn.Exec(ctx, sql, arguments...)
	if err != nil {
		if tx.conn.IsClosed() {
			tx.closed = true
		}
		return tag, err
	}
	return tag, nil
}

func (tx *tx) Query(ctx context.Context, sql string, args ...any) (Rows, error) {
	if tx.closed {
		return nil, ErrTxClosed
	}

	rows, err := tx.conn.Query(ctx, sql, args...)
	if err != nil {
		if tx.conn.IsClosed() {
			tx.closed = true
		}
		return nil, err
	}
	return rows, nil
}

func (tx *tx) QueryRow(ctx context.Context, sql string, args ...any) Row {
	rows, err := tx.Query(ctx, sql, args...)
	if err != nil {
		return &connRow{err: err}
	}
	return &connRow{rows: rows}
}

func (tx *tx) Conn() *Conn {
	return tx.conn
}
