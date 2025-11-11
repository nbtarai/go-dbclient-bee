package db

import (
	"context"
	"database/sql"

	_ "github.com/go-sql-driver/mysql"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/mysqldialect"
)

type BunClient struct {
	Connect      func() error
	Close        func() error
	Exec         func(ctx context.Context, f func(ctx context.Context, db *bun.DB) (any, error)) (any, error)
	ExecTx       func(ctx context.Context, f func(ctx context.Context, tx bun.Tx) (any, error)) (any, error)
	ExecTxClient func(ctx context.Context, f func(ctx context.Context, txClient *TxBunClient) (any, error)) (any, error)
}

type ImplBunClient struct {
	db        *bun.DB
	connectFn func() (*bun.DB, error)
}

func (c *ImplBunClient) Connect() error {
	if c.db != nil {
		return nil
	}
	db, err := c.connectFn()
	if err != nil {
		return err
	}
	c.db = db
	return nil
}

func (c *ImplBunClient) Close() error {
	if c.db == nil {
		return nil
	}
	c.db.Close()
	c.db = nil
	return nil
}

func (c *ImplBunClient) Exec(ctx context.Context, f func(ctx context.Context, db *bun.DB) (any, error)) (any, error) {
	r, err := f(ctx, c.db)
	if err != nil {
		return nil, err
	}
	return r, nil
}

func (c *ImplBunClient) ExecTx(ctx context.Context, f func(ctx context.Context, tx bun.Tx) (any, error)) (any, error) {
	var result any
	err := c.db.RunInTx(ctx, &sql.TxOptions{}, func(ctx context.Context, tx bun.Tx) error {
		r, err := f(ctx, tx)
		if err != nil {
			return err
		}
		result = r
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (c *ImplBunClient) ExecTxClient(ctx context.Context, f func(ctx context.Context, txClient *TxBunClient) (any, error)) (any, error) {
	var result any
	err := c.db.RunInTx(ctx, &sql.TxOptions{}, func(ctx context.Context, tx bun.Tx) error {
		impl := &ImplTxBunClient{
			tx: tx,
		}
		txClient := &TxBunClient{
			ExecTx: impl.ExecTx,
		}

		r, err := f(ctx, txClient)
		if err != nil {
			return err
		}
		result = r
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

type TxBunClient BunClient

type ImplTxBunClient struct {
	tx bun.Tx
}

func (c *ImplTxBunClient) Connect() error {
	panic("not supported")
}

func (c *ImplTxBunClient) Close() error {
	panic("not supported")
}

func (c *ImplTxBunClient) Exec(ctx context.Context, f func(ctx context.Context, db *bun.DB) (any, error)) (any, error) {
	panic("not supported")
}

func (c *ImplTxBunClient) ExecTxClient(ctx context.Context, f func(ctx context.Context, txClient *TxBunClient) (any, error)) (any, error) {
	panic("not supported")
}

func (c *ImplTxBunClient) ExecTx(ctx context.Context, f func(ctx context.Context, tx bun.Tx) (any, error)) (any, error) {
	r, err := f(ctx, c.tx)
	if err != nil {
		return nil, err
	}
	return r, nil
}

func ConnectMySQLForBun(username string, password string, host string, dbname string) (*bun.DB, error) {
	dsn := MySQLDsn(username, password, host, dbname)
	sqldb, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}

	db := bun.NewDB(sqldb, mysqldialect.New())
	return db, nil
}

func NewBunClientWithMySQL(username string, password string, host string, dbname string) *BunClient {
	impl := &ImplBunClient{
		connectFn: func() (*bun.DB, error) {
			return ConnectMySQLForBun(username, password, host, dbname)
		},
	}
	return &BunClient{
		Connect:      impl.Connect,
		Close:        impl.Close,
		Exec:         impl.Exec,
		ExecTx:       impl.ExecTx,
		ExecTxClient: impl.ExecTxClient,
	}
}

func BunExec[T any](ctx context.Context, client *BunClient, fn func(ctx context.Context, db *bun.DB) (*T, error)) (*T, error) {
	r, err := client.Exec(ctx, func(ctx context.Context, db *bun.DB) (any, error) {
		r, err := fn(ctx, db)
		if err != nil {
			return nil, err
		}
		return r, nil
	})
	if err != nil {
		return nil, err
	}

	result, ok := r.(*T)
	if !ok {
		panic("mismatched result type.")
	}

	return result, nil
}

func BunExecTx[T any](ctx context.Context, client *BunClient, fn func(ctx context.Context, tx bun.Tx) (*T, error)) (*T, error) {
	r, err := client.ExecTx(ctx, func(ctx context.Context, tx bun.Tx) (any, error) {
		r, err := fn(ctx, tx)
		if err != nil {
			return nil, err
		}
		return r, nil
	})
	if err != nil {
		return nil, err
	}

	result, ok := r.(*T)
	if !ok {
		panic("mismatched result type.")
	}

	return result, nil
}

func BunExecTxClient[T any](ctx context.Context, client *BunClient, fn func(ctx context.Context, txClient *TxBunClient) (*T, error)) (*T, error) {
	r, err := client.ExecTxClient(ctx, func(ctx context.Context, txClient *TxBunClient) (any, error) {
		r, err := fn(ctx, txClient)
		if err != nil {
			return nil, err
		}
		return r, nil
	})
	if err != nil {
		return nil, err
	}

	result, ok := r.(*T)
	if !ok {
		panic("mismatched result type.")
	}

	return result, nil
}
