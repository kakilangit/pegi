package pegi

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/Masterminds/squirrel"
	"github.com/jmoiron/sqlx"
)

var (
	// ErrNoTransactionCtx is error when no transaction in context.
	ErrNoTransactionCtx = errors.New("no transaction found in context")

	// ErrTransactionAlreadyStarted is error when you try to start already started transaction, to start sub transaction, see RunInSubTransaction.
	ErrTransactionAlreadyStarted = errors.New("transaction is already started")
)

// DB will hold DB object.
type DB struct {
	*sqlx.DB
	txOptions *sql.TxOptions
}

// Transactioner is transaction runner.
//
// A transaction abstraction without exposing database or library implementation details.
//
// This abstraction helps CPS build deep modules.
//
// See how:
//
// - the repository hold data layer and service/core/business logic not knowing the concrete database implementation, pointer to db, or even specific library.
//
// - the repository can be used inside or outside the transaction.
//
// - changing repository function implementation will not affect repository signature.
//
// Example:
// 		type Repository interface {
// 			GetSegment(ctx context.Context, id int) (string, error)
// 			SaveRecipe(ctx context.Context, recipe string, segment string) error
// 		}
//
// 		type Publisher interface {
// 			Publish(ctx context.Context, eventType string, recipe ...string) error
// 		}
//
// 		type BusinessLogic struct {
// 			Transactioner
// 			repo      Repository
// 			publisher Publisher
// 		}
//
// 		s := BusinessLogic{}
// 		id := 2
// 		recipe := "recipe"
// 		err := s.RunInTransaction(context.Background(), func(ctx context.Context) error {
// 			segment, err := s.repo.GetSegment(ctx, id)
// 			if err != nil {
// 				return err
// 			}
//
// 			if err := s.repo.SaveRecipe(ctx, segment, recipe);err != nil {
// 				return err
// 			}
//
// 			if err := s.publisher.Publish(ctx, "created", recipe); err != nil {
// 				return err
// 			}
//
// 			return nil
// 		})
//
// In the example:
//
// If in the first iteration, we have repo.GetSegment making db calls, changing implementation using a hashmap (in-memory cache), or even using a different persistent layer technology, we don't need to change signatures or create other services.
//
// Similar to publisher.Publish, the core logic doesn't care about the implementation of how publisher Publish events, with outbox workers or directly to Kafka the signature will be the same.
//
// "The module should be deep." ~ John Ousterhout's A Philosophy of Software Design
//
type Transactioner interface {
	RunInTransaction(ctx context.Context, f TxFunc) error
}

// LiteralTransactioner is abstraction of transaction without closure.
// with this abstraction the caller need to write transaction boilerplate in order to avoid closure.
type LiteralTransactioner interface {
	Begin(ctx context.Context) (context.Context, error)
	Rollback(ctx context.Context) error
	Commit(ctx context.Context) error
}

// NewDB construct new DB.
func NewDB(db *sqlx.DB, opts *sql.TxOptions) *DB {
	return &DB{
		DB:        db,
		txOptions: opts,
	}
}

// TxFunc is transaction signature function.
type TxFunc func(ctx context.Context) error

type ctxKey string

// ctxTxKey is key for context transaction.
const ctxTxKey ctxKey = "pegi-tx"

// GetTransaction will get transaction from context.
func GetTransaction(ctx context.Context) (*sqlx.Tx, error) {
	tx, ok := ctx.Value(ctxTxKey).(*sqlx.Tx)
	if !ok {
		return nil, ErrNoTransactionCtx
	}

	return tx, nil
}

// BeginTransaction from the context.
func BeginTransaction(ctx context.Context, db *sqlx.DB, opts *sql.TxOptions) (context.Context, error) {
	_, err := GetTransaction(ctx)
	if err == nil {
		return nil, ErrTransactionAlreadyStarted
	}

	tx, err := db.BeginTxx(ctx, opts)
	if err != nil {
		return ctx, err
	}

	return context.WithValue(ctx, ctxTxKey, tx), nil
}

// RollbackTransaction from the context.
func RollbackTransaction(ctx context.Context) error {
	tx, err := GetTransaction(ctx)
	if err != nil {
		return err
	}

	return tx.Rollback()
}

// CommitTransaction from the context.
func CommitTransaction(ctx context.Context) error {
	tx, err := GetTransaction(ctx)
	if err != nil {
		return err
	}

	return tx.Commit()
}

// RunInTransaction run a function within a database transaction.
func RunInTransaction(ctx context.Context, db *sqlx.DB, opts *sql.TxOptions, f TxFunc) error {
	ctx, err := BeginTransaction(ctx, db, opts)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	if err := f(ctx); err != nil {
		if errRollback := RollbackTransaction(ctx); errRollback != nil {
			return fmt.Errorf("failed to rollback, rollback error %s: %w", errRollback.Error(), err)
		}

		return err
	}

	if err := CommitTransaction(ctx); err != nil {
		if errRollback := RollbackTransaction(ctx); errRollback != nil {
			return fmt.Errorf("failed to rollback after commit, rollback error %s: %w", errRollback.Error(), err)
		}

		return fmt.Errorf("failed to commit: %w", err)
	}

	return nil
}

// RunInTransaction is wrapper around the transaction.
func (db DB) RunInTransaction(ctx context.Context, f TxFunc) error {
	return RunInTransaction(ctx, db.DB, db.txOptions, f)
}

// RunInSubTransaction will spawn sub transaction.
func (db DB) RunInSubTransaction(ctx context.Context, f TxFunc) error {
	return db.RunInTransaction(context.WithValue(ctx, ctxTxKey, nil), f)
}

// Begin is abstraction of tx.Begin.
func (db DB) Begin(ctx context.Context) (context.Context, error) {
	return BeginTransaction(ctx, db.DB, db.txOptions)
}

// Rollback is abstraction of tx.Rollback.
func (db DB) Rollback(ctx context.Context) error {
	return RollbackTransaction(ctx)
}

// Commit is abstraction of tx.Commit.
func (db DB) Commit(ctx context.Context) error {
	return CommitTransaction(ctx)
}

// Accessor interface is common interface of sqlx.DB and sqlx.Tx types to access the sqlx full capabilities.
type Accessor interface {
	sqlx.Queryer
	sqlx.Execer
	sqlx.ExtContext
	sqlx.Preparer
	sqlx.PreparerContext
	Select(dest interface{}, query string, args ...interface{}) error
	SelectContext(ctx context.Context, dest interface{}, query string, args ...interface{}) error
	Get(dest interface{}, query string, args ...interface{}) error
	GetContext(ctx context.Context, dest interface{}, query string, args ...interface{}) error
	NamedQuery(query string, arg interface{}) (*sqlx.Rows, error)
	NamedExec(query string, arg interface{}) (sql.Result, error)
}

// GetAccessor will get Accessor interface from context.
func (db DB) GetAccessor(ctx context.Context) Accessor {
	if tx, err := GetTransaction(ctx); err == nil {
		return tx
	}

	return db.DB
}

// A is shorter method for GetAccessor
func (db DB) A(ctx context.Context) Accessor {
	return db.GetAccessor(ctx)
}

// QueryBuilder is PostgreSQL wrapper for squirrel.
// Chaining will not have side effect, so you have to use the returning value as a new builder.
//
// Example:
//		repo := NewDB(db)
//
//		id := "id"
//		qb := repo.QueryBuilder(context.Background()).Select("*").From("table")
// 		// side effect free
//		qb = qb.Where(`r.id = ?`, id)
//		query, args, err := qb.ToSql()
//		if err != nil {
//			panic(err)
//		}
//
//		var result string
//		if err := repo.QueryRowx(query, args...).Scan(&result); err != nil {
//			panic(err)
//		}
//
//		fmt.Println(result)
//
func (db DB) QueryBuilder(ctx context.Context) squirrel.StatementBuilderType {
	return squirrel.StatementBuilder.PlaceholderFormat(squirrel.Dollar).RunWith(db.GetAccessor(ctx))
}

// Q is shorter method for QueryBuilder
func (db DB) Q(ctx context.Context) squirrel.StatementBuilderType {
	return db.QueryBuilder(ctx)
}

// ErrIgnoreNoRows ignores sql.ErrNoRows.
func ErrIgnoreNoRows(err error) error {
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}

	return nil
}

// LowerColumn returns PostgreSQL column name wrapped with LOWER function. Unsafe if using it manually in query.
func LowerColumn(column string) string {
	return fmt.Sprintf("LOWER(%s)", column)
}

// MatchesAnyPrefixSuffix formats value wrapped with % % that will match any prefix or suffix. Unsafe if using it manually in query.
func MatchesAnyPrefixSuffix(value string) string {
	return fmt.Sprint("%", strings.ToLower(value), "%")
}
