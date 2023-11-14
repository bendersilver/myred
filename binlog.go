package myred

import (
	"bufio"
	"database/sql"
	"fmt"
	"os/exec"
	"regexp"
	"strconv"
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
		// "--to-last-log",
		// "--start-position=4392730",
		"--start-position=" + fmt.Sprint(s.pos.Pos),
		"--force-read",
		s.pos.Name,
	}
	cmd := exec.Command("/usr/bin/mysqlbinlog", args...)

	stderr, err := cmd.StderrPipe()
	if err != nil {
		jlog.Fatal(err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		jlog.Fatal(err)
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
		re := regexp.MustCompile(`#\d+ .* end_log_pos (\d+) CRC32`)
		reTable := regexp.MustCompile("^### ([A-Z]+)[A-Z ]+`[a-z]+`.`([a-z]+)`")
		reVal := regexp.MustCompile(`^###   @\d+=(.*)`)
		scanner := bufio.NewScanner(stdout)
		var start bool
		var active, method, table string
		var new, old []string
		for scanner.Scan() {
			m := scanner.Text()
			// #700101  5:00:00 server id 1  end_log_pos 0  Rotate to binlog.000032  pos: 4397395
			if strings.HasPrefix(m, "ALTER TABLE") ||
				strings.HasPrefix(m, "CREATE TABLE") ||
				strings.HasPrefix(m, "DROP TABLE") ||
				strings.HasPrefix(m, "TRUNCATE") { // start action
				start = true
			}
			if start && strings.HasPrefix(m, "/*!*/;") { // end action
				start = false
			}

			if m == "COMMIT/*!*/;" {
				method, table = "", ""
			}

			args := reTable.FindStringSubmatch(m)
			if len(args) == 3 {
				if method != "" && s.tables[table] != nil {
					s.tables[table].Lock()
					tx := s.rdb.TxPipeline()
					switch method {
					case "UPDATE":
						jlog.Debug(table, new, old)
						err = s.tables[table].update(tx, new, old)

					case "DELETE":
						err = s.tables[table].del(tx, old)

					case "INSERT":
						err = s.tables[table].hset(tx, new)

					}
					if err != nil {
						return err
					}
					_, err = tx.Exec(ctx)
					if err != nil {
						return err
					}
					s.tables[table].Unlock()
				}
				method, table = args[1], args[2]
				continue
			}
			if m == "### WHERE" || m == "### SET" {
				active = strings.Trim(m, "# ")
				old, new = nil, nil
				continue
			}
			args = reVal.FindStringSubmatch(m)
			if len(args) == 2 {
				vals, err := parseValues(args[1])
				if err != nil {
					return err
				}
				switch active {
				case "WHERE":
					old = append(old, vals...)
				case "SET":
					new = append(new, vals...)
				}
				continue
			}

			// if start || begin {
			// 	jlog.Debug(m)
			// 	continue
			// }
			args = re.FindStringSubmatch(m)
			if len(args) == 2 {
				s.pos.Pos, err = strconv.ParseUint(args[1], 10, 64)
				if err != nil {
					return fmt.Errorf("parse pos err: %v", err)
				}
				if s.pos.Pos == 0 {
					continue
				}
				err = s.dumpPos()
				if err != nil {
					return err
				}
			}
			// if strings.HasPrefix(m, "### ") {
			jlog.Infof("%s", m)
			// }

		}
		return nil
	})
	cmd.Wait()
	return g.Wait()
}
