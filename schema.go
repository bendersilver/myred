package myred

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"strings"
	"sync"

	"github.com/bendersilver/jlog"
	_ "github.com/go-sql-driver/mysql"
	"github.com/redis/go-redis/v9"
)

var ctx = context.Background()

type Column struct {
	Name    string
	Primary bool
	Num     int
	dummy   bool
}

type table struct {
	sync.Mutex
	Schema  string
	Name    string
	Include map[string]bool
	Columns []Column
	oldVals []string
	newVals []string
	method  string
	t       MyRdbIfce
}

func (t *table) check() bool {
	switch t.method {
	case "UPDATE":
		return len(t.oldVals) == len(t.Columns) && len(t.newVals) == len(t.Columns)
	case "DELETE":
		return len(t.oldVals) == len(t.Columns)
	case "INSERT":
		return len(t.newVals) == len(t.Columns)
	}
	return false
}

func (t *table) getKey(vals []string) string {
	var args = []string{t.Name}
	for ix, v := range t.Columns {
		if v.Primary {
			args = append(args, vals[ix])
		}
	}
	return strings.Join(args, ":")
}

func (t *table) manageIX(rdb redis.Pipeliner, key string, m map[string]string, del bool) (err error) {
	for _, keys := range t.t.IndexFields() {
		var tmpArr = []string{t.Name, "ix"}
		tmpArr = append(tmpArr, keys...)
		ixName := strings.Join(tmpArr, ":")
		tmpArr = nil
		for _, col := range keys {
			tmpArr = append(tmpArr, m[col])
		}

		if del {
			err = rdb.HDel(ctx, ixName, strings.Join(tmpArr, ":")).Err()

		} else {
			err = rdb.HSet(ctx, ixName, strings.Join(tmpArr, ":"), key).Err()

		}
		if err != nil {
			return err
		}

	}
	return nil
}

func (t *table) update(rdb redis.Pipeliner) (err error) {
	err = t.del(rdb)
	if err != nil {
		return err
	}
	return t.hset(rdb)
}

func (t *table) hset(rdb redis.Pipeliner) error {
	var args = make(map[string]string)
	for i := 0; i < len(t.newVals); i++ {
		if t.Columns[i].dummy || t.newVals[i] == "" {
			continue
		}
		args[t.Columns[i].Name] = t.newVals[i]
	}
	var key = t.getKey(t.newVals)
	err := rdb.HSet(ctx, key, args).Err()
	if err != nil {
		return err
	}
	return t.manageIX(rdb, key, args, false)
}

func (t *table) del(rdb redis.Pipeliner) error {
	var key = t.getKey(t.oldVals)
	err := rdb.Del(ctx, t.getKey(t.oldVals)).Err()
	if err != nil {
		return err
	}
	var args = make(map[string]string)
	for i := 0; i < len(t.oldVals); i++ {
		if t.Columns[i].dummy || t.oldVals[i] == "" {
			continue
		}
		args[t.Columns[i].Name] = t.oldVals[i]
	}
	return t.manageIX(rdb, key, args, true)
}

type MyRdbIfce interface {
	IndexFields() [][]string
}

type Config struct {
	Rdb      *redis.Client
	Database string
	Socket   string
	Password string
	User     string
}

type Stream struct {
	conf   *Config
	conn   *sql.DB
	rdb    *redis.Client
	sub    *redis.PubSub
	tables map[string]*table
	pos    struct {
		Name string `redis:"name"`
		Pos  uint64 `redis:"pos"`
	}
}

func (s *Stream) dumpPos() error {
	err := s.rdb.HSet(ctx, "mysql:position", s.pos).Err()
	if err != nil {
		return fmt.Errorf("hset mysql:position err: %v", err)
	}
	return nil
}

func (s *Stream) AddTable(name string, t MyRdbIfce) error {
	if s.tables == nil {
		s.tables = make(map[string]*table)
	}
	s.tables[name] = &table{
		Schema:  s.conf.Database,
		Name:    name,
		Include: make(map[string]bool),
		t:       t,
	}
	v := reflect.ValueOf(t).Elem()
	tp := reflect.TypeOf(t).Elem()
	for i := 0; i < v.NumField(); i++ {

		fName, ok := tp.Field(i).Tag.Lookup("redis")
		if !ok || fName == "-" {
			continue
		}
		s.tables[name].Include[fName] = true
	}
	rows, err := s.conn.Query(`
			SELECT  column_name, ordinal_position, column_key = 'PRI'
			FROM information_schema.columns
				WHERE table_schema = ?
				AND table_name = ?
			ORDER BY ordinal_position ASC
	`, s.conf.Database, name)
	if err != nil {
		return fmt.Errorf("query information_schema.columns err: %v", err)
	}

	defer rows.Close()
	for rows.Next() {
		var line Column
		err = rows.Scan(&line.Name, &line.Num, &line.Primary)
		if err != nil {
			return err
		}
		line.dummy = !s.tables[name].Include[line.Name]
		s.tables[name].Columns = append(s.tables[name].Columns, line)
	}
	jlog.Debug(s.tables[name])
	return rows.Err()
}

func NewStream(c *Config) (*Stream, error) {
	// check  mysqlbinlog app
	var s Stream
	s.conf = c
	if c.Rdb == nil {
		return nil, fmt.Errorf("redis client not initial")
	}

	err := c.Rdb.HGetAll(ctx, "mysql:position").Scan(&s.pos)
	if err != nil {
		return nil, fmt.Errorf("redis hgetall `mysql:position` err: %v", err)
	}
	s.rdb = c.Rdb
	s.conn, err = sql.Open("mysql", fmt.Sprintf("%s:%s@unix(%s)/%s?parseTime=true", c.User, c.Password, c.Socket, c.Database))
	if err != nil {
		return nil, fmt.Errorf("mysql open err: %v", err)
	}

	return &s, nil
}
