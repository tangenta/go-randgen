package sqlgen

import (
	"github.com/cznic/mathutil"
	"math/rand"
)

type State struct {
	ctrl *ControlOption

	tables []*Table
	scope  []map[ScopeKeyType]ScopeObj
}

type Table struct {
	name    string
	columns []*Column
	indices []*Index

	containsPK bool // to ensure at most 1 pk in each table
	values     [][]string
}

type Column struct {
	name string
	tp   ColumnType

	isUnsigned bool
	arg1       int      // optional
	arg2       int      // optional
	args       []string // for ColumnTypeSet and ColumnTypeEnum

	defaultVal string
}

type Index struct {
	name         string
	tp           IndexType
	columns      []*Column
	columnPrefix []int
}

func NewState() *State {
	s := &State{
		ctrl: DefaultControlOption(),
	}
	s.CreateScope()
	return s
}

func (s *State) UpdateCtrlOption(fn func(option *ControlOption)) {
	fn(s.ctrl)
}

func (s *State) AppendTable(tbl *Table) {
	s.tables = append(s.tables, tbl)
}

func (t *Table) AppendColumn(c *Column) {
	t.columns = append(t.columns, c)
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
}

func (t *Table) AppendIndex(idx *Index) {
	t.indices = append(t.indices, idx)
}

func (t *Table) RemoveIndex(idx *Index) {
	var pos int
	for i := range t.indices {
		if t.indices[i].name == idx.name {
			pos = i
			break
		}
	}
	t.indices = append(t.indices[:pos], t.indices[pos+1:]...)
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

func (t *Table) AppendRow(row []string) {
	t.values = append(t.values, row)
}

func GenNewTable(name string) *Table {
	return &Table{name: name}
}

func GenNewColumn(name string) *Column {
	col := &Column{name: name}
	col.tp = ColumnType(rand.Intn(int(ColumnTypeMax)))
	switch col.tp {
	// https://docs.pingcap.com/tidb/stable/data-type-numeric
	case ColumnTypeFloat | ColumnTypeDouble:
		col.arg1 = rand.Intn(256)
		upper := mathutil.Min(col.arg1, 30)
		col.arg2 = rand.Intn(upper + 1)
	case ColumnTypeDecimal:
		col.arg1 = rand.Intn(66)
		upper := mathutil.Min(col.arg1, 30)
		col.arg2 = rand.Intn(upper + 1)
	case ColumnTypeBit:
		col.arg1 = 1 + rand.Intn(64)
	case ColumnTypeChar, ColumnTypeBinary:
		col.arg1 = 1 + rand.Intn(255)
	case ColumnTypeVarchar:
		col.arg1 = 1 + rand.Intn(1024)
	case ColumnTypeText, ColumnTypeBlob:
		col.arg1 = 1 + rand.Intn(4294967295)
	case ColumnTypeEnum, ColumnTypeSet:
		col.args = []string{"Alice", "Bob", "Charlie", "David"}
	}
	if col.tp.IsIntegerType() {
		col.isUnsigned = RandomBool()
	}
	if !col.tp.DisallowDefaultValue() && RandomBool() {
		col.defaultVal = col.RandomValue()
	}
	return col
}

func GenNewIndex(name string, tbl *Table) *Index {
	idx := &Index{name: name}
	maxVal := int(IndexTypeMax)
	if tbl.containsPK {
		maxVal = int(IndexTypePrimary)
	}
	idx.tp = IndexType(rand.Intn(maxVal))
	if idx.tp == IndexTypePrimary {
		tbl.containsPK = true
	}
	totalCols := tbl.columns
	for {
		chosenIdx := rand.Intn(len(totalCols))
		chosenCol := totalCols[chosenIdx]
		totalCols[0], totalCols[chosenIdx] = totalCols[chosenIdx], totalCols[0]
		totalCols = totalCols[1:]

		idx.columns = append(idx.columns, chosenCol)
		prefixLen := 0
		if chosenCol.tp.NeedKeyLength() ||
			(chosenCol.tp.IsStringType() && rand.Intn(4) == 0) {
			prefixLen = 1 + rand.Intn(5)
		}
		idx.columnPrefix = append(idx.columnPrefix, prefixLen)
		if len(totalCols) == 0 || RandomBool() {
			break
		}
	}
	return idx
}
