package sqlgen

import (
	"fmt"
	"math/rand"
	"strconv"
	"time"
)

func (s *State) GetRandTable() *Table {
	return s.tables[rand.Intn(len(s.tables))]
}

func (t *Table) GetRandColumn() *Column {
	return t.columns[rand.Intn(len(t.columns))]
}

func (t *Table) GetRandIntColumn() *Column {
	for _, c := range t.columns {
		if c.tp.IsIntegerType() {
			return c
		}
	}
	return nil
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

func (t *Table) GetRandColumns() []*Column {
	if RandomBool() {
		// insert into t values (...)
		return nil
	}
	// insert into t (cols..) values (...)
	totalCols := t.columns
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

func (c *Column) RandomValue() string {
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
	case ColumnTypeFloat, ColumnTypeDouble, ColumnTypeDecimal:
		return RandomFloat(0, 3.402823466e+38)
	case ColumnTypeBit:
		return RandomNum(0, (1<<c.arg1)-1)
	case ColumnTypeChar, ColumnTypeVarchar, ColumnTypeText, ColumnTypeBlob, ColumnTypeBinary:
		length := c.arg1
		if length == 0 {
			length = 5
		} else if length > 20 {
			length = 20
		}
		return fmt.Sprintf("'%s'", RandStringRunes(rand.Intn(length)))
	case ColumnTypeEnum, ColumnTypeSet:
		return fmt.Sprintf("'%s'", c.args[rand.Intn(len(c.args))])
	case ColumnTypeDate, ColumnTypeTime, ColumnTypeDatetime, ColumnTypeTimestamp:
		return fmt.Sprintf("'%s'", RandDateTime())
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

func RandDateTime() string {
	min := time.Date(1970, 1, 0, 0, 0, 0, 0, time.UTC).Unix()
	max := time.Date(2037, 1, 0, 0, 0, 0, 0, time.UTC).Unix()
	delta := max - min

	sec := rand.Int63n(delta) + min
	return time.Unix(sec, 0).Format("01-02-2006")
}
