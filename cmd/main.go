package main

import (
	"os"

	"github.com/bendersilver/jlog"
	"github.com/bendersilver/myred"
	"github.com/redis/go-redis/v9"
)

type Conf struct {
	Name string `redis:"name"`
	Val  string `redis:"val"`
}

func (c *Conf) IndexFields() [][]string {
	return nil
}

type Tmp struct {
	Name string `redis:"name"`
	Val  string `redis:"val"`
}

func (t *Tmp) IndexFields() [][]string {
	return [][]string{
		{"val"},
	}
}

func main() {

	s, err := myred.NewStream(&myred.Config{
		Rdb: redis.NewClient(&redis.Options{
			Network: "unix",
			Addr:    "/tmp/rdb.sock",
			DB:      0,
		}),
		Database: os.Getenv("DATABASE"),
		Socket:   "/tmp/my.sock",
		Password: os.Getenv("PASSWORD"),
		User:     os.Getenv("USER"),
	})
	if err != nil {
		jlog.Fatal(err)
	}
	s.AddTable("conf", new(Conf))
	s.AddTable("_conf", new(Conf))
	// binlog.Dump()
	err = s.Run()
	if err != nil {
		jlog.Crit(err)
	}
}
