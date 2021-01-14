package sqlgen

import (
	"sort"
)

func (s *State) PopOneTodoSQL() (string, bool) {
	if len(s.todoSQLs) == 0 {
		return "", false
	}
	sql := s.todoSQLs[0]
	s.todoSQLs = s.todoSQLs[1:]
	return sql, true
}

func (s *State) InjectTodoSQL(sqls ...string) {
	s.todoSQLs = append(s.todoSQLs, sqls...)
}

func (s *State) UpdateCtrlOption(fn func(option *ControlOption)) {
	fn(s.ctrl)
}

func (s *State) AppendTable(tbl *Table) {
	s.tables = append(s.tables, tbl)
}

func (t *Table) AppendColumn(c *Column) {
	t.columns = append(t.columns, c)
	for i := range t.values {
		t.values[i] = append(t.values[i], c.ZeroValue())
	}
}

func (t *Table) RemoveColumn(c *Column) {
	var pos int
	for i := range t.columns {
		if t.columns[i].name == c.name {
			pos = i
			break
		}
	}
	t.columns = append(t.columns[:pos], t.columns[pos+1:]...)
	for i := range t.values {
		t.values[i] = append(t.values[i][:pos], t.values[i][pos+1:]...)
	}
}

// Only use it when there is no table data.
func (t *Table) ReorderColumns() {
	sort.Slice(t.columns, func(i, j int) bool {
		return t.columns[i].id < t.columns[j].id
	})
}

func (t *Table) SetPrimaryKeyAndHandle(state *State) {
	t.containsPK = true
	for _, idx := range t.indices {
		if idx.tp == IndexTypePrimary {
			if len(idx.columns) == 1 && idx.columns[0].tp.IsIntegerType() {
				t.handleCols = idx.columns
				return
			}
			if state.enabledClustered {
				t.handleCols = idx.columns
				return
			}
			break
		}
	}
	t.handleCols = []*Column{{
		id:         0,
		name:       "_tidb_rowid",
		tp:         ColumnTypeBigInt,
		isNotNull:  true,
		isUnsigned: true,
	}}
}

func (t *Table) AppendIndex(idx *Index) {
	for _, idxCol := range idx.columns {
		idxCol.relatedIndices[idx.id] = struct{}{}
	}
	t.indices = append(t.indices, idx)
}

func (t *Table) RemoveIndex(idx *Index) {
	var pos int
	for i := range t.indices {
		if t.indices[i].id == idx.id {
			pos = i
			break
		}
	}
	t.indices = append(t.indices[:pos], t.indices[pos+1:]...)
	for _, idxCol := range idx.columns {
		delete(idxCol.relatedIndices, idx.id)
	}
}

func (t *Table) AppendRow(row []string) {
	t.values = append(t.values, row)
}
