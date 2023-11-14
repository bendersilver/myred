package myred

type dbItem struct {
	table  string
	method string
	vals   []string
}

func (di *dbItem) set(s *Stream) (err error) {
	tx := s.rdb.TxPipeline()
	if di.vals != nil {
		table := s.tables[di.table]
		if table != nil {
			switch di.method {
			case "SET":
				err = table.set(tx, di.vals...)
			case "WHERE":
				err = table.del(tx, di.vals...)
			}
			if err != nil {
				tx.Discard()
				return
			}

		}
	}
	_, err = tx.Exec(ctx)
	di.vals = nil
	return err
}
