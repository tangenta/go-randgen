package sqlgen

import (
	"math/rand"
)

func (s *State) IsInitializing() bool {
	if s.finishInit {
		return false
	}
	if len(s.tables) < s.ctrl.InitTableCount {
		return true
	}
	for _, t := range s.tables {
		if len(t.values) < s.ctrl.InitRowCount {
			return true
		}
	}
	s.finishInit = true
	return false
}

func (s *State) GetRandTable() *Table {
	return s.tables[rand.Intn(len(s.tables))]
}

func (s *State) GetFirstNonFullTable() *Table {
	for _, t := range s.tables {
		if len(t.values) < s.ctrl.InitRowCount {
			return t
		}
	}
	return nil
}

func (t *Table) GetRandColumn() *Column {
	return t.columns[rand.Intn(len(t.columns))]
}

func (t *Table) GetRandColumnWithIndexUncovered() *Column {
	restCols := make([]*Column, 0, len(t.columns))
	for _, c := range t.columns {
		hasBeenCovered := false
		for _, idx := range t.indices {
			if idx.name == c.name {
				hasBeenCovered = true
				break
			}
		}
		if !hasBeenCovered {
			restCols = append(restCols, c)
		}
	}
	return restCols[rand.Intn(len(restCols))]
}

func (t *Table) GetRandColumnsIncludedDefaultValue() []*Column {
	if RandomBool() {
		// insert into t values (...)
		return nil
	}
	// insert into t (cols..) values (...)
	totalCols := t.FilterColumns(func(c *Column) bool { return c.defaultVal != "" })
	selectedCols := t.FilterColumns(func(c *Column) bool { return c.defaultVal == "" })
	for len(totalCols) > 0 && RandomBool() {
		chosenIdx := rand.Intn(len(totalCols))
		chosenCol := totalCols[chosenIdx]
		totalCols[0], totalCols[chosenIdx] = totalCols[chosenIdx], totalCols[0]
		totalCols = totalCols[1:]

		selectedCols = append(selectedCols, chosenCol)
	}
	return selectedCols
}

func (t *Table) HasColumnUncoveredByIndex() bool {
	indexedCols := make(map[string]struct{})
	for _, idx := range t.indices {
		for _, c := range idx.columns {
			indexedCols[c.name] = struct{}{}
		}
	}
	return len(t.columns) != len(indexedCols)
}

func (t *Table) FilterColumns(pred func(column *Column) bool) []*Column {
	restCols := make([]*Column, 0, len(t.columns))
	for _, c := range t.columns {
		if pred(c) {
			restCols = append(restCols, c)
		}
	}
	return restCols
}

func (t *Table) GetRandomIndex() *Index {
	return t.indices[rand.Intn(len(t.indices))]
}

func (t *Table) GetRandIntColumn() *Column {
	for _, c := range t.columns {
		if c.tp.IsIntegerType() {
			return c
		}
	}
	return nil
}

func (t *Table) GetRandRow(cols []*Column) []string {
	if len(t.values) == 0 {
		return nil
	}
	if len(cols) == 0 {
		return t.values[rand.Intn(len(t.values))]
	}
	vals := make([]string, 0, len(cols))
	randRow := t.values[rand.Intn(len(t.values))]
	for _, targetCol := range cols {
		for i, tableCol := range t.columns {
			if tableCol.id == targetCol.id {
				vals = append(vals, randRow[i])
				break
			}
		}
	}
	return vals
}

func (t *Table) GetRandRowVal(col *Column) string {
	if len(t.values) == 0 {
		return ""
	}
	randRow := t.values[rand.Intn(len(t.values))]
	for i, c := range t.columns {
		if c.id == col.id {
			return randRow[i]
		}
	}
	return "GetRandRowVal: column not found"
}

func (t *Table) cloneColumns() []*Column {
	cols := make([]*Column, len(t.columns))
	for i, c := range t.columns {
		cols[i] = c
	}
	return cols
}

func (t *Table) GetRandColumns() []*Column {
	if RandomBool() {
		// insert into t values (...)
		return nil
	}
	// insert into t (cols..) values (...)
	totalCols := t.cloneColumns()
	var selectedCols []*Column
	for {
		chosenIdx := rand.Intn(len(totalCols))
		chosenCol := totalCols[chosenIdx]
		totalCols[0], totalCols[chosenIdx] = totalCols[chosenIdx], totalCols[0]
		totalCols = totalCols[1:]

		selectedCols = append(selectedCols, chosenCol)
		if len(totalCols) == 0 || RandomBool() {
			break
		}
	}
	return selectedCols
}
