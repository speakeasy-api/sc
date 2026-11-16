package pgdriver

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/speakeasy-api/sc"
)

// Querier runs queries. It can be a pool, a connection, or a transaction.
type Querier interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// Store is the location where a [Driver] keeps the snapshot of each
// instance. The methods that take a transaction run in the transaction that
// the driver got. Thus, in the same transaction as the state change, a store
// can do these things:
//
//   - Keep the snapshot on the row of the domain.
//   - Copy the state into the columns that queries and indexes read.
//   - Write an audit row.
//
// Read runs outside all transactions, on the pool of the driver.
//
// A store sets the keys and the scope of instances. If a store serves many
// tenants, it must limit each query to one tenant itself. The driver does not
// change the IDs that it passes to the store.
//
// The default store, [TableStore], keeps snapshots in the table of the
// driver.
type Store[S comparable, C any] interface {
	// Lock reads the stored snapshot of an instance and locks it until the
	// transaction ends. If the instance does not exist, Lock returns an
	// error that wraps [ErrNotFound].
	Lock(ctx context.Context, tx pgx.Tx, id string) ([]byte, error)

	// Read reads the stored snapshot of an instance but does not lock it.
	// If the instance does not exist, Read returns an error that wraps
	// [ErrNotFound].
	Read(ctx context.Context, q Querier, id string) ([]byte, error)

	// Insert stores the first snapshot of a new instance, encoded as data.
	// If an instance with the ID exists already, Insert returns an error
	// that wraps [ErrExists].
	Insert(ctx context.Context, tx pgx.Tx, id string, snap sc.Snapshot[S, C], data []byte) error

	// Update replaces the snapshot of an instance after step. data is the
	// encoded new snapshot. The driver calls Update each time the stored
	// snapshot changes:
	//
	//   - For an accepted step.
	//   - For an ignored step, if the restore of the snapshot made effects,
	//     for example after a migration.
	//
	// Examine step.Accepted before you use the call as a state change.
	Update(ctx context.Context, tx pgx.Tx, id string, step sc.Step[S, C], data []byte) error
}

// TableStore keeps snapshots in the statechart_instances table that
// [Schema] makes. It keeps one row for each instance of the machine.
type TableStore[S comparable, C any] struct {
	// Machine is the ID of the machine. The store holds the instances of
	// this machine.
	Machine string
}

// Lock reads and locks the row of the instance.
func (s TableStore[S, C]) Lock(ctx context.Context, tx pgx.Tx, id string) ([]byte, error) {
	return s.read(ctx, tx, id, `SELECT snapshot FROM statechart_instances WHERE machine = $1 AND id = $2 FOR UPDATE`)
}

// Read reads the row of the instance.
func (s TableStore[S, C]) Read(ctx context.Context, q Querier, id string) ([]byte, error) {
	return s.read(ctx, q, id, `SELECT snapshot FROM statechart_instances WHERE machine = $1 AND id = $2`)
}

func (s TableStore[S, C]) read(ctx context.Context, q Querier, id, query string) ([]byte, error) {
	var data []byte

	err := q.QueryRow(ctx, query, s.Machine, id).Scan(&data)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return nil, ErrNotFound
	case err != nil:
		return nil, fmt.Errorf("read instance: %w", err)
	}

	return data, nil
}

// Insert adds the row of the instance.
func (s TableStore[S, C]) Insert(ctx context.Context, tx pgx.Tx, id string, snap sc.Snapshot[S, C], data []byte) error {
	tag, err := tx.Exec(ctx, `
		INSERT INTO statechart_instances (machine, id, seq, snapshot)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (machine, id) DO NOTHING`,
		s.Machine, id, snap.Seq(), data,
	)
	if err != nil {
		return fmt.Errorf("insert instance: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrExists
	}

	return nil
}

// Update writes the row of the instance again.
func (s TableStore[S, C]) Update(ctx context.Context, tx pgx.Tx, id string, step sc.Step[S, C], data []byte) error {
	_, err := tx.Exec(ctx, `
		UPDATE statechart_instances
		SET seq = $3, snapshot = $4, updated_at = clock_timestamp()
		WHERE machine = $1 AND id = $2`,
		s.Machine, id, step.To.Seq(), data,
	)
	if err != nil {
		return fmt.Errorf("update instance: %w", err)
	}

	return nil
}
