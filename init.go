package myred

import (
	"database/sql"
	"fmt"
)

func (s *Stream) initial() (err error) {
	var rows *sql.Rows
	rows, err = s.conn.Query("SHOW BINARY LOGS;")
	if err != nil {
		return fmt.Errorf("show binary log err: %v", err)
	}
	defer rows.Close()

	var dummy string
	for rows.Next() {
		err = rows.Scan(&s.pos.Name, &s.pos.Pos, &dummy)
		if err != nil {
			return fmt.Errorf("scan binary log err: %v", err)
		}
	}
	var tabs []string
	for k := range s.tables {
		tabs = append(tabs, k)
	}
	err = s.Dump(tabs)
	if err != nil {
		return fmt.Errorf("dump table err: %v", err)
	}
	return s.dumpPos()
}
