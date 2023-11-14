package myred

import (
	"bufio"
	"database/sql"
	"fmt"
	"os/exec"
	"regexp"
	"strings"

	"github.com/bendersilver/jlog"
	"golang.org/x/sync/errgroup"
)

func (s *Stream) Run() (err error) {
	s.sub = s.rdb.Subscribe(ctx, "rdbStream")
	go func() {
		for msg := range s.sub.Channel() {
			jlog.Notice(msg)
		}
	}()
	if s.pos.Pos == 0 {
		var rows *sql.Rows
		rows, err = s.conn.Query("SHOW BINARY LOGS;")
		if err != nil {
			return fmt.Errorf("show binary log err: %v", err)
		}
		var dummy string
		for rows.Next() {
			err = rows.Scan(&s.pos.Name, &s.pos.Pos, &dummy)
			if err != nil {
				return fmt.Errorf("scan binary log err: %v", err)
			}
		}
		rows.Close()
		var tabs []string
		for k := range s.tables {
			tabs = append(tabs, k)
		}
		err = s.Dump(tabs)
		if err != nil {
			return fmt.Errorf("dump table err: %v", err)
		}
		err = s.dumpPos()
		if err != nil {
			return err
		}
	}
	return s.run()
}

func (s *Stream) Close() error {
	s.sub.Close()
	s.conn.Close()
	return nil
}

func (s *Stream) run() (err error) {
	jlog.Debug(s.pos)
	args := []string{
		"--stop-never",
		"--read-from-remote-server",
		"--protocol=socket",
		"--socket=" + s.conf.Socket,
		"--user=" + s.conf.User,
		"--password=" + s.conf.Password,
		"--database=" + s.conf.Database,
		"--base64-output=DECODE-ROWS",
		"--verbose",
		"--skip-gtids",
		// "--to-last-log",
		"--start-position=4392730",
		// "--start-position=" + fmt.Sprint(s.pos.Pos),
		"--force-read",
		s.pos.Name,
	}
	cmd := exec.Command("/usr/bin/mysqlbinlog", args...)

	stderr, err := cmd.StderrPipe()
	if err != nil {
		jlog.Fatal(err)
	}
	defer stderr.Close()
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		jlog.Fatal(err)
	}
	defer stdout.Close()
	cmd.Start()
	g := new(errgroup.Group)
	g.Go(func() error {
		scanner := bufio.NewScanner(stderr)
		for scanner.Scan() {
			m := scanner.Text()
			if strings.HasPrefix(m, "ERROR: ") {
				return fmt.Errorf(m)
			}

		}
		return nil
	})
	g.Go(func() error {
		reAt := regexp.MustCompile(`^# at (\d+)$`)
		reTable := regexp.MustCompile("^### ([A-Z]+)[A-Z ]+`.+`.`(.+)`$")
		reVal := regexp.MustCompile(`^###   @\d+=(.*)`)
		scanner := bufio.NewScanner(stdout)

		var item dbItem

		for scanner.Scan() {
			m := scanner.Text()
			jlog.Debug(m)

			args = reAt.FindStringSubmatch(m)
			if len(args) == 2 { // execute
				item.position = args[1]
				item.set()
				continue
			}

			// only UPDATE, DELETE, INSERT
			if !strings.HasPrefix(m, "### ") {
				continue
			}

			args = reTable.FindStringSubmatch(m)
			if len(args) == 3 {
				item.set()
				item.table = args[2]
			}
			switch m {
			case "### SET", "### WHERE":
				item.set()
				item.method = strings.Trim(m, "# ")
			}

			args = reVal.FindStringSubmatch(m)
			if len(args) == 2 {
				v, err := parseValues(args[1])
				if err != nil {
					return err
				}
				item.vals = append(item.vals, v...)

			}

		}
		return nil
	})
	cmd.Wait()
	return g.Wait()
}

type dbItem struct {
	table    string
	method   string
	vals     []string
	position string
}

func (di *dbItem) set() error {
	if di.vals != nil {
		jlog.Debug(di.method, di.table, di.vals)
	}
	di.vals = nil
	return nil
}

func (s *Stream) write(tb *table) (err error) {
	tb.Lock()
	defer tb.Unlock()
	jlog.Notice(tb)
	tx := s.rdb.TxPipeline()
	switch tb.method {
	case "UPDATE":
		err = tb.update(tx)

	case "DELETE":
		err = tb.del(tx)

	case "INSERT":
		err = tb.hset(tx)

	}
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx)
	return err

}
