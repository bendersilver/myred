package myred

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/bendersilver/jlog"
)

func (s *Stream) Dump(tables []string) (err error) {
	if len(tables) == 0 {
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
	var reVals = regexp.MustCompile("^INSERT INTO `(.+?)` VALUES \\((.+)\\);$")

	var item dbItem
	item.method = "SET"
	return s.cmdRun("mysqldump",
		append(args, tables...),
		dumpErrWrapper,
		func(line string) (err error) {
			if m := reVals.FindStringSubmatch(line); len(m) == 3 {
				item.table = m[1]
				item.vals, err = parseValues(m[2])
				if err != nil {
					return err
				}
				return item.set(s)
			}
			return nil
		})
}

func dumpErrWrapper(line string) (err error) {
	if strings.Contains(line, "[ERROR]") {
		return fmt.Errorf(line)
	} else {
		jlog.Warning(line)
	}
	return nil
}
