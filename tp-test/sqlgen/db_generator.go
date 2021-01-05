package sqlgen

import (
	"fmt"
	"math/rand"
	"strconv"
	"time"

	"github.com/cznic/mathutil"
)

func GenNewTable(name string) *Table {
	return &Table{name: name}
}

func GenNewColumn(id int) *Column {
	col := &Column{id: id, name: fmt.Sprintf("col_%d", id)}
	col.tp = ColumnType(rand.Intn(int(ColumnTypeMax)))
	switch col.tp {
	// https://docs.pingcap.com/tidb/stable/data-type-numeric
	case ColumnTypeFloat | ColumnTypeDouble:
		col.arg1 = 1 + rand.Intn(255)
		upper := mathutil.Min(col.arg1, 30)
		col.arg2 = 1 + rand.Intn(upper)
	case ColumnTypeDecimal:
		col.arg1 = 1 + rand.Intn(65)
		upper := mathutil.Min(col.arg1, 30)
		col.arg2 = 1 + rand.Intn(upper)
	case ColumnTypeBit:
		col.arg1 = 1 + rand.Intn(62)
	case ColumnTypeChar, ColumnTypeBinary:
		col.arg1 = 1 + rand.Intn(255)
	case ColumnTypeVarchar:
		col.arg1 = 1 + rand.Intn(1024)
	case ColumnTypeText, ColumnTypeBlob:
		col.arg1 = 1 + rand.Intn(4294967295)
	case ColumnTypeEnum, ColumnTypeSet:
		col.args = []string{"Alice", "Bob", "Charlie", "David"}
	}
	if col.tp != ColumnTypeVarchar && rand.Intn(5) == 0 {
		col.arg1, col.arg2 = 0, 0
	}
	if col.tp.IsIntegerType() {
		col.isUnsigned = RandomBool()
	}
	if !col.tp.DisallowDefaultValue() && RandomBool() {
		col.defaultVal = col.RandomValue()
	}
	col.isNotNull = RandomBool()
	col.relatedIndices = map[int]struct{}{}
	return col
}

func GenNewIndex(id int, tbl *Table) *Index {
	idx := &Index{id: id, name: fmt.Sprintf("idx_%d", id)}
	maxVal := int(IndexTypeMax)
	if tbl.containsPK {
		maxVal = int(IndexTypePrimary)
	}
	idx.tp = IndexType(rand.Intn(maxVal))
	if idx.tp == IndexTypePrimary {
		tbl.containsPK = true
	}
	totalCols := tbl.cloneColumns()
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

func (t *Table) GenRandValues(cols []*Column) []string {
	if len(cols) == 0 {
		cols = t.columns
	}
	row := make([]string, len(cols))
	for i, c := range cols {
		row[i] = c.RandomValue()
	}
	return row
}

func (c *Column) ZeroValue() string {
	switch c.tp {
	case ColumnTypeTinyInt, ColumnTypeSmallInt, ColumnTypeMediumInt, ColumnTypeInt, ColumnTypeBigInt, ColumnTypeBoolean:
		return "0"
	case ColumnTypeFloat, ColumnTypeDouble, ColumnTypeDecimal, ColumnTypeBit:
		return "0"
	case ColumnTypeChar, ColumnTypeVarchar, ColumnTypeText, ColumnTypeBlob, ColumnTypeBinary:
		return "''"
	case ColumnTypeEnum, ColumnTypeSet:
		return fmt.Sprintf("'%s'", c.args[0])
	case ColumnTypeDate, ColumnTypeDatetime, ColumnTypeTimestamp:
		return fmt.Sprintf("'2000-01-01'")
	case ColumnTypeTime:
		return fmt.Sprintf("'00:00:00'")
	default:
		return "invalid data type"
	}
}

func (c *Column) RandomValue() string {
	if rand.Intn(20) == 0 {
		return "null"
	}
	if c.isUnsigned {
		switch c.tp {
		case ColumnTypeTinyInt:
			return RandomNum(0, 255)
		case ColumnTypeSmallInt:
			return RandomNum(0, 65535)
		case ColumnTypeMediumInt:
			return RandomNum(0, 16777215)
		case ColumnTypeInt:
			return RandomNum(0, 4294967295)
		case ColumnTypeBigInt:
			return RandomNum(0, 9223372036854775806)
		}
	}
	switch c.tp {
	case ColumnTypeTinyInt:
		return RandomNum(-128, 127)
	case ColumnTypeSmallInt:
		return RandomNum(-32768, 32767)
	case ColumnTypeMediumInt:
		return RandomNum(-8388608, 8388607)
	case ColumnTypeInt:
		return RandomNum(-2147483648, 2147483647)
	case ColumnTypeBigInt:
		num := rand.Int63()
		if RandomBool() {
			num = -num
		}
		return strconv.FormatInt(num, 10)
	case ColumnTypeBoolean:
		return RandomNum(0, 1)
	case ColumnTypeFloat, ColumnTypeDouble:
		if c.arg1 == 0 && c.arg2 == 0 {
			return RandomFloat(0, 10000)
		}
		left := rand.Intn(1 + mathutil.Min(c.arg1-c.arg2, 6))
		right := rand.Intn(1 + mathutil.Min(c.arg2, 4))
		return fmt.Sprintf("%s.%s", RandNumRunes(left), RandNumRunes(right))
	case ColumnTypeDecimal:
		if c.arg1 == 0 && c.arg2 == 0 {
			c.arg1 = 10
		}
		left := rand.Intn(1 + mathutil.Min(c.arg1-c.arg2, 6))
		right := rand.Intn(1 + mathutil.Min(c.arg2, 4))
		return fmt.Sprintf("%s.%s", RandNumRunes(left), RandNumRunes(right))
	case ColumnTypeBit:
		return RandomNum(0, (1<<c.arg1)-1)
	case ColumnTypeChar, ColumnTypeVarchar, ColumnTypeBinary:
		length := c.arg1
		if length == 0 {
			length = 1
		} else if length > 20 {
			length = 20
		}
		return fmt.Sprintf("'%s'", RandStringRunes(rand.Intn(length)))
	case ColumnTypeText, ColumnTypeBlob:
		length := c.arg1
		if length == 0 {
			length = 5
		} else if length > 20 {
			length = 20
		}
		return fmt.Sprintf("'%s'", RandStringRunes(rand.Intn(length)))
	case ColumnTypeEnum, ColumnTypeSet:
		return fmt.Sprintf("'%s'", c.args[rand.Intn(len(c.args))])
	case ColumnTypeDate, ColumnTypeDatetime, ColumnTypeTimestamp:
		return fmt.Sprintf("'%s'", RandDate())
	case ColumnTypeTime:
		return fmt.Sprintf("'%s'", RandTime())
	default:
		return "invalid data type"
	}
}

var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func RandStringRunes(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}

var numRunes = []rune("0123456789")

func RandNumRunes(n int) string {
	if n == 0 {
		return "0"
	}
	b := make([]rune, n)
	for i := range b {
		b[i] = numRunes[rand.Intn(len(numRunes))]
	}
	return string(b)
}

func RandDate() string {
	min := time.Date(1970, 1, 0, 0, 0, 0, 0, time.UTC).Unix()
	max := time.Date(2037, 1, 0, 0, 0, 0, 0, time.UTC).Unix()
	delta := max - min

	sec := rand.Int63n(delta) + min
	return time.Unix(sec, 0).Format("2006-01-02")
}

func RandTime() string {
	min := time.Date(1970, 1, 0, 0, 0, 0, 0, time.UTC).Unix()
	max := time.Date(2037, 1, 0, 0, 0, 0, 0, time.UTC).Unix()
	delta := max - min

	sec := rand.Int63n(delta) + min
	return time.Unix(sec, 0).Format("15:04:05.00")
}
