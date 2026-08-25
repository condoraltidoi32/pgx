package pgx

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestTxRollbackAfterContextCancellation(t *testing.T) {
	ctx := context.Background()
	conn, err := Connect(ctx)
	if err != nil {
		t.Fatalf("unexpected connect error: %v", err)
	}

	tx, err := conn.Begin(ctx)
	if err != nil {
		t.Fatalf("unexpected begin error: %v", err)
	}

	// Execute query with short context timeout
	timeoutCtx, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
	time.Sleep(20 * time.Millisecond)
	_, queryErr := tx.Exec(timeoutCtx, "SELECT pg_sleep(5)")
	cancel()

	if queryErr == nil {
		t.Fatal("expected query error due to timeout, got nil")
	}

	// Rollback using background context
	err = tx.Rollback(context.Background())
	if err == nil {
		t.Fatal("expected rollback error, got nil")
	}
	if !errors.Is(err, ErrTxClosed) {
		t.Fatalf("expected ErrTxClosed, got: %v", err)
	}
	if strings.Contains(err.Error(), "conn closed") {
		t.Fatalf("error message should not contain 'conn closed', got: %v", err.Error())
	}

	// Double rollback test
	err2 := tx.Rollback(context.Background())
	if !errors.Is(err2, ErrTxClosed) {
		t.Fatalf("expected ErrTxClosed on double rollback, got: %v", err2)
	}

	// Commit on cancelled/closed transaction
	err3 := tx.Commit(context.Background())
	if !errors.Is(err3, ErrTxClosed) {
		t.Fatalf("expected ErrTxClosed on commit, got: %v", err3)
	}
}

func TestTxHealthyCommitAndRollback(t *testing.T) {
	ctx := context.Background()

	// Test successful commit
	conn, _ := Connect(ctx)
	tx, err := conn.Begin(ctx)
	if err != nil {
		t.Fatalf("unexpected begin error: %v", err)
	}
	_, err = tx.Exec(ctx, "INSERT INTO test VALUES (1)")
	if err != nil {
		t.Fatalf("unexpected exec error: %v", err)
	}
	err = tx.Commit(ctx)
	if err != nil {
		t.Fatalf("unexpected commit error: %v", err)
	}
	err = tx.Rollback(ctx)
	if !errors.Is(err, ErrTxClosed) {
		t.Fatalf("expected ErrTxClosed after commit, got: %v", err)
	}

	// Test successful rollback
	conn2, _ := Connect(ctx)
	tx2, err := conn2.Begin(ctx)
	if err != nil {
		t.Fatalf("unexpected begin error: %v", err)
	}
	err = tx2.Rollback(ctx)
	if err != nil {
		t.Fatalf("unexpected rollback error: %v", err)
	}
	err = tx2.Commit(ctx)
	if !errors.Is(err, ErrTxClosed) {
		t.Fatalf("expected ErrTxClosed after rollback, got: %v", err)
	}
}

func TestTxQueryCancellationTransitionsClosed(t *testing.T) {
	ctx := context.Background()
	conn, _ := Connect(ctx)
	tx, err := conn.Begin(ctx)
	if err != nil {
		t.Fatalf("unexpected begin error: %v", err)
	}

	timeoutCtx, cancel := context.WithCancel(ctx)
	cancel()

	_, err = tx.Query(timeoutCtx, "SELECT 1")
	if err == nil {
		t.Fatal("expected query error, got nil")
	}

	deferErr := tx.Rollback(context.Background())
	if !errors.Is(deferErr, ErrTxClosed) {
		t.Fatalf("expected ErrTxClosed from deferred rollback, got: %v", deferErr)
	}
}
