package myred

import (
	"bufio"
	"fmt"
	"os/exec"
	"regexp"
	"strings"

	"github.com/redis/go-redis/v9"
	"golang.org/x/sync/errgroup"
)

var reVals = regexp.MustCompile("^INSERT INTO `(.+?)` VALUES \\((.+)\\);$")

func (s *Stream) Dump(table []string) error {
	if len(table) == 0 {
		return fmt.Errorf("table empty")
	}
	args := []string{
		"--protocol=socket",
		"--socket=" + s.conf.Socket,
		"--user=" + s.conf.User,
		"--password=" + s.conf.Password,
		"--single-transaction",
		"--skip-lock-tables",
		"--compact",
		"--skip-opt",
		"--quick",
		"--no-create-info",
		"--skip-extended-insert",
		"--skip-tz-utc",
		s.conf.Database,
	}
	cmd := exec.Command("/usr/bin/mysqldump", append(args, table...)...)

	stderr, err := cmd.StderrPipe()
	if err != nil {
		return fmt.Errorf("StderrPipe err: %v", err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return fmt.Errorf("StdoutPipe err: %v", err)
	}

	cmd.Start()

	g := new(errgroup.Group)
	g.Go(func() error {
		scanner := bufio.NewScanner(stderr)
		for scanner.Scan() {
			m := scanner.Text()
			if strings.Contains(m, "[ERROR]") {
				return fmt.Errorf(m)
			}

		}
		return nil
	})
	g.Go(func() error {
		scanner := bufio.NewScanner(stdout)
		var tx redis.Pipeliner
		for scanner.Scan() {
			m := scanner.Text()
			if m := reVals.FindAllStringSubmatch(m, -1); len(m) == 1 {
				table := m[0][1]
				vals, err := parseValues(m[0][2])
				if err != nil {
					return err
				}
				tx = s.rdb.Pipeline()
				err = s.tables[table].hset(tx, vals)
				if err != nil {
					return err
				}
				_, err = tx.Exec(ctx)
				if err != nil {
					return err
				}
			}
		}
		return nil
	})
	cmd.Wait()
	return g.Wait()
}

func parseValues(str string) ([]string, error) {
	values := make([]string, 0, 8)
	i := 0
	for i < len(str) {
		if str[i] != '\'' {
			j := i + 1
			for ; j < len(str) && str[j] != ','; j++ {
			}
			if str[i:j] == "NULL" {
				values = append(values, "")
			} else {
				values = append(values, str[i:j])
			}
			i = j + 1
		} else {
			j := i + 1
			for j < len(str) {
				if str[j] == '\\' {
					j += 2
					continue
				} else if str[j] == '\'' {
					break
				} else {
					j++
				}
			}

			if j >= len(str) {
				return nil, fmt.Errorf("parse quote values error")
			}

			values = append(values, str[i+1:j])

			i = j + 2
		}
	}

	return values, nil
}