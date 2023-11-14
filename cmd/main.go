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

func main() {

	s, err := myred.NewStream(&myred.Config{
		Rdb: redis.NewClient(&redis.Options{
			Network: "unix",
			Addr:    "/tmp/rdb.sock",
			DB:      0,
		}),
		Database: os.Getenv("database"),
		Socket:   "/tmp/my.sock",
		Password: os.Getenv("password"),
		User:     os.Getenv("user"),
	})
	if err != nil {
		jlog.Fatal(err)
	}
	s.AddTable("conf", new(Conf))
	// binlog.Dump()
	err = s.Run()
	if err != nil {
		jlog.Crit(err)
	}
}
