package mysql

import (
	"context"
	"database/sql"

	"github.com/usthooz/sqlx"
)

// Callback
func (d *DB) Callback(fn func(sqlx.DbAndTx) error, tx ...*sqlx.Tx) error {
	if fn == nil {
		return nil
	}
	if len(tx) > 0 && tx[0] != nil {
		return fn(tx[0])
	}
	return fn(d)
}

// TransactCallback
func (d *DB) TransactCallback(fn func(*sqlx.Tx) error, tx ...*sqlx.Tx) (err error) {
	if fn == nil {
		return
	}
	var (
		_tx *sqlx.Tx
	)
	if len(tx) > 0 {
		_tx = tx[0]
	}
	if _tx == nil {
		_tx, err = d.Beginx()
		if err != nil {
			return err
		}
		defer func() {
			if err != nil {
				_tx.Rollback()
			} else {
				_tx.Commit()
			}
		}()
	}
	err = fn(_tx)
	return err
}

// CallbackInSession
func (d *DB) CallbackInSession(fn func(context.Context, *sqlx.Conn) error, ctx ...context.Context) error {
	if fn == nil {
		return nil
	}
	var _ctx = context.Background()
	if len(ctx) > 0 {
		_ctx = ctx[0]
	}
	conn, err := d.Conn(_ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	return fn(_ctx, conn)
}

// TransactCallbackInSession
func (d *DB) TransactCallbackInSession(fn func(context.Context, *sqlx.Tx) error, ctx ...context.Context) (err error) {
	if fn == nil {
		return
	}
	var _ctx = context.Background()
	if len(ctx) > 0 {
		_ctx = ctx[0]
	}
	conn, err := d.Conn(_ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	_tx, err := conn.Beginx()
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_tx.Rollback()
		} else {
			_tx.Commit()
		}
	}()
	err = fn(_ctx, _tx)
	return err
}

// ErrNoRows no rows
var ErrNoRows = sql.ErrNoRows

// IsNoRows
func IsNoRows(err error) bool {
	return ErrNoRows == err
}
