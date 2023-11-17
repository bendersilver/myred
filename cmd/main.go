package main

import (
	"context"
	"encoding/json"
	"os"
	"os/signal"
	"syscall"

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

func (*Conf) AfterSet(redis.Pipeliner, string, map[string]string) error {
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
func (*Tmp) AfterSet(redis.Pipeliner, string, map[string]string) error {
	return nil
}

type Queue struct {
	ID   string          `redis:"id"`
	Val  string          `redis:"chan_name"`
	Body json.RawMessage `redis:"body"`
}

func (q *Queue) IndexFields() [][]string {
	return nil
}

func (*Queue) AfterSet(rp redis.Pipeliner, key string, data map[string]string) error {
	return rp.Publish(context.Background(), data["chan_name"], 1).Err()
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
	signalChan := make(chan os.Signal, 1)
	signal.Notify(
		signalChan,
		syscall.SIGHUP,  // kill -SIGHUP XXXX
		syscall.SIGINT,  // kill -SIGINT XXXX or Ctrl+c
		syscall.SIGQUIT, // kill -SIGQUIT XXXX
		syscall.SIGTERM,
	)
	go func() {
		<-signalChan
		err = s.Close()
		if err != nil {
			jlog.Crit(err)
		}
	}()
	s.AddTable("conf", new(Conf))
	s.AddTable("_tmp", new(Tmp))
	s.AddTable("queue", new(Queue))
	err = s.Run()
	if err != nil {
		jlog.Crit(err)
	}
}
