package db

import (
	"context"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

type GormClient struct {
	Connect      func() error
	Close        func() error
	Exec         func(ctx context.Context, f func(ctx context.Context, db *gorm.DB) (any, error)) (any, error)
	ExecTx       func(ctx context.Context, f func(ctx context.Context, tx *gorm.DB) (any, error)) (any, error)
	ExecTxClient func(ctx context.Context, f func(ctx context.Context, txClient *TxGormClient) (any, error)) (any, error)
}

type ImplGormClient struct {
	db        *gorm.DB
	connectFn func() (*gorm.DB, error)
}

func (c *ImplGormClient) Connect() error {
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

func (c *ImplGormClient) Close() error {
	if c.db == nil {
		return nil
	}
	sqlDB, err := c.db.DB()
	if err != nil {
		return err
	}
	sqlDB.Close()
	c.db = nil
	return nil
}

func (c *ImplGormClient) Exec(ctx context.Context, f func(ctx context.Context, db *gorm.DB) (any, error)) (any, error) {
	r, err := f(ctx, c.db)
	if err != nil {
		return nil, err
	}
	return r, nil
}

func (c *ImplGormClient) ExecTx(ctx context.Context, f func(ctx context.Context, tx *gorm.DB) (any, error)) (any, error) {
	var result any
	err := c.db.Transaction(func(tx *gorm.DB) error {
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

func (c *ImplGormClient) ExecTxClient(ctx context.Context, f func(ctx context.Context, txClient *TxGormClient) (any, error)) (any, error) {
	var result any
	if err := c.db.Transaction(func(tx *gorm.DB) error {
		impl := &ImplTxGormClient{
			tx: tx,
		}
		txClient := &TxGormClient{
			Connect:      impl.Connect,
			Close:        impl.Close,
			Exec:         impl.Exec,
			ExecTx:       impl.ExecTx,
			ExecTxClient: impl.ExecTxClient,
		}
		r, err := f(ctx, txClient)
		if err != nil {
			return err
		}
		result = r
		return nil
	}); err != nil {
		return nil, err
	}
	return result, nil
}

type TxGormClient GormClient

func (tx *TxGormClient) AsClient() *GormClient {
	return (*GormClient)(tx)
}

type ImplTxGormClient struct {
	tx *gorm.DB
}

func (c *ImplTxGormClient) Connect() error {
	panic("not supported")
}

func (c *ImplTxGormClient) Close() error {
	panic("not supported")
}

func (c *ImplTxGormClient) Exec(ctx context.Context, f func(ctx context.Context, db *gorm.DB) (any, error)) (any, error) {
	panic("not supported")
}

func (c *ImplTxGormClient) ExecTxClient(ctx context.Context, f func(ctx context.Context, txClient *TxGormClient) (any, error)) (any, error) {
	panic("not supported")
}

func (c *ImplTxGormClient) ExecTx(ctx context.Context, f func(ctx context.Context, tx *gorm.DB) (any, error)) (any, error) {
	r, err := f(ctx, c.tx)
	if err != nil {
		return nil, err
	}
	return r, nil
}

func NewGormClientWithMySQL(username string, password string, host string, dbname string) *GormClient {
	impl := &ImplGormClient{
		connectFn: func() (*gorm.DB, error) {
			return ConnectMySQLForGorm(username, password, host, dbname)
		},
	}
	return &GormClient{
		Connect:      impl.Connect,
		Close:        impl.Close,
		Exec:         impl.Exec,
		ExecTx:       impl.ExecTx,
		ExecTxClient: impl.ExecTxClient,
	}
}

func ConnectMySQLForGorm(username string, password string, host string, dbname string) (*gorm.DB, error) {
	dsn := MySQLDsn(username, password, host, dbname)
	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{})
	if err != nil {
		return nil, err
	}

	return db, nil
}

func GormExec[T any](ctx context.Context, client *GormClient, fn func(ctx context.Context, db *gorm.DB) (*T, error)) (*T, error) {
	r, err := client.Exec(ctx, func(ctx context.Context, db *gorm.DB) (any, error) {
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

func GormExecTx[T any](ctx context.Context, client *GormClient, fn func(ctx context.Context, tx *gorm.DB) (*T, error)) (*T, error) {
	r, err := client.ExecTx(ctx, func(ctx context.Context, tx *gorm.DB) (any, error) {
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

func GormExecTxClient[T any](ctx context.Context, client *GormClient, fn func(ctx context.Context, txClient *TxGormClient) (*T, error)) (*T, error) {
	r, err := client.ExecTxClient(ctx, func(ctx context.Context, txClient *TxGormClient) (any, error) {
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
