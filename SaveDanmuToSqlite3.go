package plugin

import (
	"context"
	"database/sql"
	"errors"
	"time"

	_ "github.com/mattn/go-sqlite3"
	c "github.com/qydysky/bili_danmu/CV"
	p "github.com/qydysky/bili_danmu/plugin"
	psql "github.com/qydysky/part/sqlite"
)

// 保存弹幕至sqlite
//
// 将SaveDanmuToSqlite3.go放置于github.com/qydysky/bili_danmu/plugin以启用插件
//
// 此插件需要启用CGO_ENABLED=1，并安装了gcc
//
// 构建指引 https://github.com/mattn/go-sqlite3
type SaveDanmuToSqlite3 struct {
	db *sql.DB
}

func init() {
	var saveDanmuToSqlite3 SaveDanmuToSqlite3

	p.Plugin.Pull_tag_only(`Event`, func(a any) (disable bool) {
		if v, ok := a.(int); ok && v == p.LoadKv {
			saveDanmuToSqlite3.init(c.C)
		}
		return true
	})

	p.Plugin.Pull_tag_only(`Danmu`, func(a any) (disable bool) {
		if v, ok := a.(p.Danmu); ok {
			saveDanmuToSqlite3.danmu(v)
		}
		return false
	})
}

func (t *SaveDanmuToSqlite3) init(c *c.Common) {
	if v, ok := c.K_v.LoadV(`保存弹幕至sqlite`).(string); ok && v != "" {
		if db, e := sql.Open("sqlite3", v); e != nil {
			panic(e)
		} else {
			t.db = db
		}

		ctx := context.Background()
		tx := psql.BeginTx[any](t.db, ctx, &sql.TxOptions{})
		tx = tx.Do(psql.SqlFunc[any]{
			Ty:         psql.Execf,
			Ctx:        ctx,
			Query:      "create table danmu (created text, createdunix text, msg text, color text, auth text, uid text, roomid text)",
			SkipSqlErr: true,
		})
		if e := tx.Fin(); e != nil {
			panic(e)
		}
	}
}

func (t *SaveDanmuToSqlite3) danmu(item p.Danmu) {
	if t.db != nil {
		ctx := context.Background()
		tx := psql.BeginTx[any](t.db, ctx, &sql.TxOptions{})
		tx = tx.Do(psql.SqlFunc[any]{
			Ty:    psql.Execf,
			Ctx:   ctx,
			Query: "insert into danmu values (?, ?, ?, ?, ?, ?, ?)",
			Args:  []any{time.Now().Format(time.DateTime), time.Now().Unix(), item.Msg, item.Color, item.Auth, item.Uid, item.Roomid},
			AfterEF: func(_ *any, result sql.Result, txE error) (_ *any, stopErr error) {
				if v, e := result.RowsAffected(); e != nil {
					return nil, e
				} else if v != 1 {
					return nil, errors.New("插入数量错误")
				}
				return nil, nil
			},
		})
		if e := tx.Fin(); e != nil {
			c.C.Log.Base_add("保存弹幕至sqlite").L(`E: `, e)
		}
	}
}
