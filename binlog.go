package myred

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/bendersilver/jlog"
)

func (s *Stream) Run() (err error) {
	// s.sub = s.rdb.Subscribe(ctx, "rdbStream")
	// go func() {
	// 	for msg := range s.sub.Channel() {
	// 		jlog.Notice(msg)
	// 	}
	// }()
	if s.pos.Pos == 0 {
		err = s.initial()
		if err != nil {
			return err
		}
	}
	return s.run()
}

func (s *Stream) Close() error {
	if s.cmd != nil {
		s.cmd.Process.Kill()
	}
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
		"--skip-gtids",
		// "--to-last-log",
		// "--start-position=4392730",
		"--start-position=" + fmt.Sprint(s.pos.Pos),
		"--force-read",
		s.pos.Name,
	}
	reAt := regexp.MustCompile(`^# at (\d+)$`)
	reTable := regexp.MustCompile("^### ([A-Z]+)[A-Z ]+`.+`.`(.+)`$")
	reVal := regexp.MustCompile(`^###   @\d+=(.*)`)
	var item dbItem
	return s.cmdRun("mysqlbinlog", args,
		binErrWrapper,
		func(line string) (err error) {
			jlog.Debug(line)
			args = reAt.FindStringSubmatch(line)
			if len(args) == 2 { // execute
				err = item.set(s)
				if err != nil {
					return err
				}
				s.pos.Pos, err = strconv.ParseUint(args[1], 10, 64)
				if err != nil {
					return err
				}
				return s.dumpPos()
			}

			// only UPDATE, DELETE, INSERT
			if !strings.HasPrefix(line, "### ") {
				return nil
			}

			args = reTable.FindStringSubmatch(line)
			if len(args) == 3 {
				err = item.set(s)
				if err != nil {
					return err
				}
				item.table = args[2]
			}
			switch line {
			case "### SET", "### WHERE":
				err = item.set(s)
				if err != nil {
					return err
				}
				item.method = strings.Trim(line, "# ")
			}

			args = reVal.FindStringSubmatch(line)
			if len(args) == 2 {
				v, err := parseValues(args[1])
				if err != nil {
					return err
				}
				item.vals = append(item.vals, v...)
			}
			return nil
		})
}

func binErrWrapper(line string) (err error) {
	if strings.HasPrefix(line, "ERROR: ") {
		return fmt.Errorf(line)
	}
	return nil
}
